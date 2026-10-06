#include <Storages/MergeTree/Compaction/MergeSelectors/DisjointPartsRangesSet.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/FluidLSMMergeSelector.h>
#include <Storages/MergeTree/Compaction/PartitionStatistics.h>

#include <algorithm>
#include <limits>
#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

namespace
{

/// Tries to make [begin, end) a merge range: honors max_parts_to_merge_at_once
/// and the range filter.
std::optional<PartsRange> makeRange(
    PartsIterator begin,
    PartsIterator end,
    const IMergeSelector::RangeFilter & range_filter,
    size_t max_parts_to_merge_at_once)
{
    if (max_parts_to_merge_at_once && end - begin > static_cast<ssize_t>(max_parts_to_merge_at_once))
        end = begin + max_parts_to_merge_at_once;

    if (end - begin < 2)
        return std::nullopt;

    if (range_filter && !range_filter({begin, end}))
        return std::nullopt;

    return PartsRange(begin, end);
}

/// Shrink the range from the right until it fits the constraint.
/// Returns false if it does not fit even with two parts.
bool shrinkToFit(PartsRange & range, const MergeConstraint & constraint)
{
    size_t sum_size = 0;
    size_t sum_rows = 0;
    for (const auto & part : range)
    {
        sum_size += part.size;
        sum_rows += part.rows;
    }

    while (range.size() > 2 && (sum_size > constraint.max_size_bytes || sum_rows > constraint.max_size_rows))
    {
        sum_size -= range.back().size;
        sum_rows -= range.back().rows;
        range.pop_back();
    }

    return sum_size <= constraint.max_size_bytes && sum_rows <= constraint.max_size_rows;
}

/// Merge every maximal contiguous run of parts of an overflowing lower level.
/// `swept` marks the positions claimed by largest-level sweep windows: a run
/// is always fully inside or fully outside a window (windows start and end on
/// largest-level parts), so skipping swept positions keeps the runs disjoint
/// from the sweeps.
void emitLowerLevelRuns(
    const PartsRange & parts,
    const std::unordered_map<UInt32, size_t> & level_counts,
    UInt32 max_level,
    size_t lower_level_bound,
    const std::vector<bool> * swept,
    const IMergeSelector::RangeFilter & range_filter,
    size_t max_parts_to_merge_at_once,
    std::vector<PartsRange> & out)
{
    for (const auto & [level, count] : level_counts)
    {
        if (level == max_level || count <= lower_level_bound)
            continue;

        for (auto begin = parts.begin(); begin != parts.end();)
        {
            if (begin->info.level != level || (swept && (*swept)[static_cast<size_t>(begin - parts.begin())]))
            {
                ++begin;
                continue;
            }

            auto end = begin + 1;
            while (end != parts.end() && end->info.level == level && !(swept && (*swept)[static_cast<size_t>(end - parts.begin())]))
                ++end;

            if (auto range = makeRange(begin, end, range_filter, max_parts_to_merge_at_once))
                out.push_back(std::move(*range));
            begin = end;
        }
    }
}

}

PartsRanges FluidLSMMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeConstraints & merge_constraints,
    const RangeFilter & range_filter) const
{
    /// Merge ranges emitted by this selector directly. Each parts range holds
    /// parts of a single partition (guaranteed by the merge predicate), so
    /// these can never overlap the ranges picked by the delegated Simple scan
    /// in other partitions.
    std::vector<PartsRange> explicit_ranges;

    /// Partitions where a lower level overflowed: the merge range is chosen
    /// by SimpleMergeSelector. The partitions themselves are needed for the
    /// same-level fallback if the scan finds nothing there.
    PartsRanges delegated;
    std::unordered_map<String, const PartsRange *> overflowed_partitions;

    for (const PartsRange & parts : parts_ranges)
    {
        if (parts.size() < 2)
            continue;

        const auto partition_id = parts.front().info.getPartitionId();

        /// Merges that collapse parts unconditionally, without consulting the
        /// level bounds: aggressive selection (OPTIMIZE) and force-merge by age.
        bool force_merge = settings.aggressive;

        if (!force_merge && settings.min_partition_age_to_force_merge && settings.partitions_stats)
        {
            auto it = settings.partitions_stats->find(partition_id);
            if (it != settings.partitions_stats->end()
                && it->second.min_age != std::numeric_limits<time_t>::max()
                && it->second.min_age >= static_cast<time_t>(settings.min_partition_age_to_force_merge))
                force_merge = true;
        }

        if (force_merge)
        {
            if (auto range = makeRange(parts.begin(), parts.end(), range_filter, settings.max_parts_to_merge_at_once))
                explicit_ranges.push_back(std::move(*range));
            continue;
        }

        /// Every maximal contiguous run of parts that are all old enough is
        /// merged; these preempt the level-bounded merges of the same parts.
        if (settings.min_age_to_force_merge)
        {
            const auto min_age = static_cast<time_t>(settings.min_age_to_force_merge);
            bool emitted = false;
            for (auto begin = parts.begin(); begin != parts.end();)
            {
                if (begin->age < min_age)
                {
                    ++begin;
                    continue;
                }

                auto end = begin + 1;
                while (end != parts.end() && end->age >= min_age)
                    ++end;

                if (auto range = makeRange(begin, end, range_filter, settings.max_parts_to_merge_at_once))
                {
                    explicit_ranges.push_back(std::move(*range));
                    emitted = true;
                }
                begin = end;
            }

            if (emitted)
                continue;
        }

        /// Count parts per level and find the largest occupied level.
        std::unordered_map<UInt32, size_t> level_counts;
        UInt32 max_level = 0;
        for (const auto & part : parts)
        {
            ++level_counts[part.info.level];
            max_level = std::max(max_level, part.info.level);
        }

        /// Leveled merge at the largest level: merge contiguous windows that
        /// cover bound + 1 parts of the largest level each. Parts of lower
        /// levels that lie between them are swept up into the merge; this is
        /// what keeps the bound enforceable when the parts of the largest
        /// level are not adjacent in block-number order. The delegated Simple
        /// scan cannot be used in this partition because its windows could
        /// overlap the swept parts, so the overflowing lower levels are merged
        /// as same-level runs disjoint from the sweep windows instead - this
        /// also keeps the lower levels draining while a largest-level merge
        /// does not fit the merge constraints.
        if (level_counts[max_level] > settings.max_parts_at_largest_level)
        {
            std::vector<bool> swept(parts.size(), false);
            PartsIterator window_begin = parts.end();
            size_t top_parts = 0;
            for (auto it = parts.begin(); it != parts.end(); ++it)
            {
                if (it->info.level != max_level)
                    continue;

                if (window_begin == parts.end())
                    window_begin = it;

                if (++top_parts > settings.max_parts_at_largest_level)
                {
                    if (auto range = makeRange(window_begin, it + 1, range_filter, settings.max_parts_to_merge_at_once))
                        explicit_ranges.push_back(std::move(*range));
                    /// The swept parts are claimed by the window even if the
                    /// range itself was filtered out.
                    for (auto jt = window_begin; jt != it + 1; ++jt)
                        swept[static_cast<size_t>(jt - parts.begin())] = true;
                    window_begin = parts.end();
                    top_parts = 0;
                }
            }

            emitLowerLevelRuns(parts, level_counts, max_level, settings.max_parts_at_lower_levels,
                &swept, range_filter, settings.max_parts_to_merge_at_once, explicit_ranges);
            continue;
        }

        /// FluidLSM decides when to merge: a lower level must overflow its
        /// bound. Simple decides what to merge, so the partition is delegated.
        if (std::any_of(level_counts.begin(), level_counts.end(),
                [&](const auto & kv) { return kv.first != max_level && kv.second > settings.max_parts_at_lower_levels; }))
        {
            delegated.push_back(parts);
            overflowed_partitions.emplace(partition_id, &parts);
        }
    }

    /// Bound enforcement takes the largest merge constraints first.
    PartsRanges result;
    size_t constraints_used = 0;
    for (auto & range : explicit_ranges)
    {
        if (constraints_used >= merge_constraints.size())
            break;
        if (shrinkToFit(range, merge_constraints[constraints_used]))
        {
            result.push_back(std::move(range));
            ++constraints_used;
        }
    }

    /// Simple picks the merge ranges in the overflowing partitions. Its own
    /// estimator scores the windows, keeps the results disjoint and fits them
    /// into the remaining constraints.
    if (!delegated.empty() && constraints_used < merge_constraints.size())
    {
        for (auto & range : SimpleMergeSelector(settings.simple).select(
                 delegated, merge_constraints.subspan(constraints_used), range_filter))
        {
            overflowed_partitions.erase(range.front().info.getPartitionId());
            result.push_back(std::move(range));
            ++constraints_used;
        }
    }

    /// If Simple found nothing in an overflowing partition - for example the
    /// overflowing runs are too small to pass its size-ratio check - merge the
    /// same-level runs so a level can never be stuck over its bound.
    for (const auto & [_, parts] : overflowed_partitions)
    {
        if (constraints_used >= merge_constraints.size())
            break;

        std::unordered_map<UInt32, size_t> level_counts;
        UInt32 max_level = 0;
        for (const auto & part : *parts)
        {
            ++level_counts[part.info.level];
            max_level = std::max(max_level, part.info.level);
        }

        std::vector<PartsRange> runs;
        emitLowerLevelRuns(*parts, level_counts, max_level, settings.max_parts_at_lower_levels,
            nullptr, range_filter, settings.max_parts_to_merge_at_once, runs);

        for (auto & run : runs)
        {
            if (constraints_used >= merge_constraints.size())
                break;
            if (shrinkToFit(run, merge_constraints[constraints_used]))
            {
                result.push_back(std::move(run));
                ++constraints_used;
            }
        }
    }

    return result;
}

}
