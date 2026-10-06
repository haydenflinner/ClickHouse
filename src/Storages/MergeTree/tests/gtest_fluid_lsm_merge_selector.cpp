#include <Storages/MergeTree/Compaction/MergeSelectors/FluidLSMMergeSelector.h>

#include <base/unit.h>

#include <fmt/format.h>
#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

using namespace DB;

namespace
{

/// The delegated Simple scan reads PartitionsStatistics unconditionally when
/// the width heuristic is enabled; tests leave the stats empty and disable it.
void disableWidthHeuristic(FluidLSMMergeSelector::Settings & settings)
{
    settings.simple.enable_heuristic_to_lower_max_parts_to_merge_at_once = false;
}

/// A part is described by its block range and level; names are built accordingly
/// so that a test can assert *which* parts were selected.
struct PartDesc
{
    size_t min_block;
    size_t max_block;
    UInt32 level;
    size_t size = 10 * MiB;
    time_t age = 0;
    std::string partition = "all";
};

std::string partName(const PartDesc & part)
{
    return fmt::format("{}_{}_{}_{}", part.partition, part.min_block, part.max_block, part.level);
}

PartsRange makePartsRange(const std::vector<PartDesc> & parts)
{
    PartsRange parts_range;
    for (const auto & part : parts)
    {
        std::string name = partName(part);
        parts_range.push_back(PartProperties
        {
            .name = name,
            .info = MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING),
            .size = part.size,
            .age = part.age,
            .rows = 100,
        });
    }

    return parts_range;
}

/// One parts range per partition, as guaranteed by the merge predicate.
PartsRanges makePartsRanges(const std::vector<PartDesc> & parts)
{
    std::map<std::string, std::vector<PartDesc>> by_partition;
    for (const auto & part : parts)
        by_partition[part.partition].push_back(part);

    PartsRanges ranges;
    for (const auto & [_, partition_parts] : by_partition)
        ranges.push_back(makePartsRange(partition_parts));
    return ranges;
}

/// Parts at level 0 covering consecutive block ranges.
std::vector<PartDesc> level0Parts(size_t begin_block, size_t count, size_t size = 10 * MiB, time_t age = 0)
{
    std::vector<PartDesc> parts;
    for (size_t i = 0; i < count; ++i)
        parts.push_back(PartDesc{.min_block = begin_block + i, .max_block = begin_block + i, .level = 0, .size = size, .age = age});
    return parts;
}

std::vector<std::string> partNames(const PartsRange & range)
{
    std::vector<std::string> names;
    names.reserve(range.size());
    for (const auto & part : range)
        names.push_back(part.name);
    return names;
}

std::vector<MergeConstraint> makeConstraints(size_t max_bytes = 100 * GiB, size_t max_rows = 100000, size_t num_constraints = 1)
{
    return std::vector<MergeConstraint>(num_constraints, {max_bytes, max_rows});
}

}

/// A level below the largest one merges only when it accumulates more than K parts.
TEST(FluidLSMMergeSelector, LowerLevelsAreMergedOnlyWhenLevelOverflows)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 3;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    /// A single high-level part makes level 0 a lower level governed by K.
    auto descs = level0Parts(1, 3);
    descs.push_back(PartDesc{.min_block = 4, .max_block = 10, .level = 1});

    /// Within the bound - nothing to merge.
    auto parts_range = makePartsRange(descs);
    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 0);

    /// One part over the bound - a merge is scheduled. All parts have the same
    /// size, so the Simple scan sweeps the whole window including the level-1
    /// part into a single cross-level merge.
    descs = level0Parts(1, 4);
    descs.push_back(PartDesc{.min_block = 5, .max_block = 10, .level = 1});
    parts_range = makePartsRange(descs);
    selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 5);
}

/// The level bound is the trigger: even when the Simple selector's size-ratio
/// check would allow a merge, nothing is merged while every level is within
/// its bound.
TEST(FluidLSMMergeSelector, WithinBoundsSelectsNothingEvenIfRatioQualifies)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 10;
    settings.max_parts_at_largest_level = 10;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    /// Six equal parts pass Simple's ratio of 5, but no level overflows.
    auto parts_range = makePartsRange(level0Parts(1, 6));
    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 0);
}

/// When a lower level overflows, the merge range is picked by the Simple
/// selector and is not restricted to the overflowing level's parts.
TEST(FluidLSMMergeSelector, SimpleScanPicksTheOverflowingRun)
{
    FluidLSMMergeSelector::Settings settings;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    /// Ten level-0 parts overflow K = 9; an equal-size window passes the ratio
    /// check and is merged. The high-level part is not swept in.
    auto descs = level0Parts(1, 10);
    descs.push_back(PartDesc{.min_block = 11, .max_block = 20, .level = 5, .size = GiB});
    auto parts_range = makePartsRange(descs);

    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 10);
    for (const auto & part : selected[0])
        EXPECT_EQ(part.info.level, 0);
}

/// Under a backlog the delegated Simple scan picks wide, possibly cross-level
/// windows - this is what gives Simple its low write amplification under
/// sustained heavy inserts.
TEST(FluidLSMMergeSelector, SimpleScanPicksWideCrossLevelWindowUnderBacklog)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_largest_level = 100;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    /// 30 equal parts across two levels: level 0 overflows, and the lowered
    /// base of a full partition makes the whole range qualify, so everything
    /// is merged at once regardless of level.
    auto descs = level0Parts(1, 20);
    for (size_t i = 0; i < 10; ++i)
        descs.push_back(PartDesc{.min_block = 21 + i, .max_block = 21 + i, .level = 1});
    auto parts_range = makePartsRange(descs);

    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 30);
}

/// If the Simple scan finds nothing - for example the overflowing run is too
/// small to pass its ratio check - the same-level runs are merged so a level
/// can never be stuck over its bound.
TEST(FluidLSMMergeSelector, FallsBackToSameLevelRunsWhenSimpleFindsNothing)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 3;
    settings.simple.base = 1000;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    auto descs = level0Parts(1, 4);
    descs.push_back(PartDesc{.min_block = 5, .max_block = 10, .level = 5});
    auto parts_range = makePartsRange(descs);

    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 4);
    for (const auto & part : selected[0])
        EXPECT_EQ(part.info.level, 0);
}

/// With Z = 1 the largest level is "leveled": a second part at the largest level merges with it.
TEST(FluidLSMMergeSelector, LargestLevelIsLeveled)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 10;
    settings.max_parts_at_largest_level = 1;

    FluidLSMMergeSelector selector(settings);

    /// A single part at the largest level - nothing to merge.
    auto parts_range = makePartsRange({PartDesc{.min_block = 1, .max_block = 10, .level = 2}});
    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 0);

    /// Two parts at the largest level - they are merged.
    parts_range = makePartsRange({
        PartDesc{.min_block = 1, .max_block = 10, .level = 2},
        PartDesc{.min_block = 11, .max_block = 20, .level = 2},
    });
    selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 2);
}

/// Parts of the largest level that are not adjacent in block-number order are merged
/// through a window that sweeps the lower-level parts between them.
TEST(FluidLSMMergeSelector, LargestLevelWindowSweepsLowerLevelParts)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 10;
    settings.max_parts_at_largest_level = 1;

    FluidLSMMergeSelector selector(settings);

    auto parts_range = makePartsRange({
        PartDesc{.min_block = 1, .max_block = 10, .level = 2},
        PartDesc{.min_block = 11, .max_block = 11, .level = 0},
        PartDesc{.min_block = 12, .max_block = 12, .level = 0},
        PartDesc{.min_block = 13, .max_block = 20, .level = 2},
    });

    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 4);
    EXPECT_EQ(selected[0].front().info.level, 2);
    EXPECT_EQ(selected[0].back().info.level, 2);
}

/// Parts of a lower level that are not contiguous are not merged;
/// only a contiguous run of the same level can form a merge range.
TEST(FluidLSMMergeSelector, LowerLevelMergeRequiresContiguity)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 1;
    settings.max_parts_at_largest_level = 10;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    /// Three level-0 parts separated by a level-1 part: the level overflows its bound,
    /// but no contiguous same-level run has more than one part... the runs [0] and [0,0]
    /// below give a merge of the contiguous pair.
    auto parts_range = makePartsRange({
        PartDesc{.min_block = 1, .max_block = 1, .level = 0},
        PartDesc{.min_block = 2, .max_block = 2, .level = 1},
        PartDesc{.min_block = 3, .max_block = 3, .level = 0},
        PartDesc{.min_block = 4, .max_block = 4, .level = 0},
    });

    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(partNames(selected[0]), (std::vector<std::string>{"all_3_3_0", "all_4_4_0"}));
}

/// A fully tiered configuration (large K and large Z) tolerates many parts at every level.
TEST(FluidLSMMergeSelector, TieringToleratesManyParts)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 100;
    settings.max_parts_at_largest_level = 100;

    FluidLSMMergeSelector selector(settings);

    auto parts_range = makePartsRange(level0Parts(1, 10));
    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 0);
}

/// When both a lower level and the largest level overflow in the same
/// partition, the largest-level sweep is emitted and the lower level is
/// merged as same-level runs disjoint from the sweep - the delegated Simple
/// scan is not used because its windows could overlap the swept parts.
TEST(FluidLSMMergeSelector, LargestLevelSweepRunsAlongsideLowerLevelRuns)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 2;
    settings.max_parts_at_largest_level = 1;
    settings.size_fixed_cost_to_add = 0;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    auto descs = level0Parts(1, 3, /*size=*/MiB);
    descs.push_back(PartDesc{.min_block = 4, .max_block = 10, .level = 3, .size = GiB});
    descs.push_back(PartDesc{.min_block = 11, .max_block = 20, .level = 3, .size = GiB});
    auto parts_range = makePartsRange(descs);

    /// Level 0 overflows (3 > K = 2) and level 3 overflows (2 > Z = 1): the
    /// sweep merges the two level-3 parts, and the level-0 run is merged too.
    auto selected = selector.select({parts_range}, makeConstraints(100 * GiB, 100000, 2), nullptr);
    ASSERT_EQ(selected.size(), 2);
    EXPECT_EQ(selected[0].size(), 2);
    EXPECT_EQ(selected[0].front().info.level, 3);
    EXPECT_EQ(selected[1].size(), 3);
    EXPECT_EQ(selected[1].front().info.level, 0);
}

/// Partitions are independent: a largest-level sweep in one does not prevent
/// a delegated merge in another in the same selection round.
TEST(FluidLSMMergeSelector, PartitionsAreSelectedIndependently)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 2;
    settings.max_parts_at_largest_level = 1;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    auto parts_ranges = makePartsRanges({
        PartDesc{.min_block = 1, .max_block = 10, .level = 3, .size = GiB, .partition = "a"},
        PartDesc{.min_block = 11, .max_block = 20, .level = 3, .size = GiB, .partition = "a"},
        PartDesc{.min_block = 1, .max_block = 1, .level = 0, .partition = "b"},
        PartDesc{.min_block = 2, .max_block = 2, .level = 0, .partition = "b"},
        PartDesc{.min_block = 3, .max_block = 3, .level = 0, .partition = "b"},
        PartDesc{.min_block = 4, .max_block = 10, .level = 5, .partition = "b"},
    });

    auto selected = selector.select(parts_ranges, makeConstraints(100 * GiB, 100000, 2), nullptr);
    ASSERT_EQ(selected.size(), 2);
    EXPECT_EQ(selected[0].front().info.getPartitionId(), "a");
    EXPECT_EQ(selected[1].front().info.getPartitionId(), "b");
    EXPECT_EQ(selected[1].size(), 3);
}

/// Merge candidates are trimmed to the merge constraints.
TEST(FluidLSMMergeSelector, CandidatesAreTrimmedToConstraints)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 2;
    settings.max_parts_at_largest_level = 1;

    FluidLSMMergeSelector selector(settings);

    auto parts_range = makePartsRange(level0Parts(1, 4, /*size=*/60 * MiB));

    auto selected = selector.select({parts_range}, makeConstraints(/*max_bytes=*/130 * MiB), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 2);
}

/// Aggressive mode (OPTIMIZE) merges whole ranges regardless of the level bounds.
TEST(FluidLSMMergeSelector, AggressiveMergesWholeRange)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 100;
    settings.max_parts_at_largest_level = 100;
    settings.aggressive = true;

    FluidLSMMergeSelector selector(settings);

    auto parts_range = makePartsRange(level0Parts(1, 3));
    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 3);
}

/// Parts that are all older than min_age_to_force_merge are merged unconditionally.
TEST(FluidLSMMergeSelector, ForceMergeByPartAge)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 100;
    settings.max_parts_at_largest_level = 100;
    settings.min_age_to_force_merge = 100;

    FluidLSMMergeSelector selector(settings);

    /// Only the old tail of the partition is force-merged.
    std::vector<PartDesc> descs = level0Parts(1, 2, /*size=*/MiB, /*age=*/200);
    descs.push_back(PartDesc{.min_block = 3, .max_block = 3, .level = 0, .size = MiB, .age = 0});
    auto parts_range = makePartsRange(descs);

    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);
    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(partNames(selected[0]), (std::vector<std::string>{"all_1_1_0", "all_2_2_0"}));
}

/// A partition whose youngest part is old enough is merged entirely.
TEST(FluidLSMMergeSelector, ForceMergeByPartitionAge)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 100;
    settings.max_parts_at_largest_level = 100;
    settings.min_partition_age_to_force_merge = 100;

    auto parts_range = makePartsRange(level0Parts(1, 3));

    PartitionsStatistics statistics;
    statistics["all"] = PartitionStatistics{
        .min_age = 200,
        .part_count = parts_range.size(),
        .total_size = 30 * MiB,
    };
    settings.partitions_stats = &statistics;

    FluidLSMMergeSelector selector(settings);
    auto selected = selector.select({parts_range}, makeConstraints(), nullptr);

    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected[0].size(), 3);
}

/// The merge predicate can split one partition into several parts ranges.
/// The same-level fallback applies to each range that the delegated Simple
/// scan did not cover, not just once per partition.
TEST(FluidLSMMergeSelector, FallbackIsPerRangeNotPerPartition)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 2;
    settings.max_parts_at_largest_level = 100;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    /// Two ranges of the same partition with disjoint parts (as produced by
    /// splitByMergePredicate). In the first one the overflowing level-0 run
    /// is too small to pass Simple's ratio check, while the second one is
    /// merged by Simple itself.
    PartsRanges ranges;
    {
        auto descs = level0Parts(1, 3);
        descs.push_back(PartDesc{.min_block = 4, .max_block = 10, .level = 9});
        ranges.push_back(makePartsRange(descs));
    }
    {
        auto descs = level0Parts(100, 20);
        descs.push_back(PartDesc{.min_block = 120, .max_block = 130, .level = 9});
        ranges.push_back(makePartsRange(descs));
    }

    auto selected = selector.select(ranges, makeConstraints(100 * GiB, 100000, 2), nullptr);
    ASSERT_EQ(selected.size(), 2);
    /// One range is the wide delegated merge, the other is the fallback run
    /// of the three level-0 parts.
    size_t fallback_count = 0;
    for (const auto & range : selected)
    {
        EXPECT_GE(range.size(), 3);
        if (range.size() == 3)
        {
            ++fallback_count;
            for (const auto & part : range)
                EXPECT_EQ(part.info.level, 0);
        }
    }
    EXPECT_EQ(fallback_count, 1);
}

/// Merge candidates never cross a partition boundary.
TEST(FluidLSMMergeSelector, PartitionBoundariesAreRespected)
{
    FluidLSMMergeSelector::Settings settings;
    settings.max_parts_at_lower_levels = 1;
    settings.max_parts_at_largest_level = 1;
    disableWidthHeuristic(settings);

    FluidLSMMergeSelector selector(settings);

    auto parts_ranges = makePartsRanges({
        PartDesc{.min_block = 1, .max_block = 1, .level = 0, .partition = "all"},
        PartDesc{.min_block = 2, .max_block = 2, .level = 0, .partition = "all"},
        PartDesc{.min_block = 1, .max_block = 1, .level = 0, .partition = "other"},
        PartDesc{.min_block = 2, .max_block = 2, .level = 0, .partition = "other"},
    });

    auto selected = selector.select(parts_ranges, makeConstraints(100 * GiB, 100000, 2), nullptr);
    ASSERT_EQ(selected.size(), 2);
    for (const auto & range : selected)
    {
        ASSERT_EQ(range.size(), 2);
        EXPECT_EQ(range.front().info.getPartitionId(), range.back().info.getPartitionId());
    }
}
