#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/PartitionStatistics.h>

/**
Merge selector implementing the Fluid LSM-tree merge policy
from the "Dostoevsky" paper:

Niv Dayan, Stratos Idreos. "Dostoevsky: Better Space-Time Trade-Offs for
LSM-Tree Based Key-Value Stores via Adaptive Removal of Superfluous Merging"
(SIGMOD'18). https://doi.org/10.1145/3183713.3196927

Idea: parts are runs, part level (MergeTreePartInfo::level) is the LSM-tree
level - the number of merges the part has survived. Merging parts at level `l`
produces a part at level `l + 1`, so levels below the top play the role of the
smaller LSM-tree levels and the largest level is the level of the largest parts.

The paper's insight is that merging at all levels but the largest is
"superfluous": point lookups, long range lookups and space-amplification are
dominated by the largest level, while update cost is paid equally at all
levels. For ClickHouse virtually every SELECT is a (long) range lookup over all
parts, so it is safe to merge lazily at the lower levels, and the largest level
only needs a bound (`Z`), not eager leveling.

The policy keeps at most `K` parts at each level below the largest occupied
level and at most `Z` parts at the largest level, merging a level when it
overflows its bound:

- K = 1 and Z = 1 gives leveling - every incoming part is merged into the
  existing parts of its level as soon as possible;
- large K and large Z gives tiering - runs accumulate and merge only when a
  level is full;
- K > 1 and Z = 1 gives "Lazy Leveling" - tiering at all levels but the
  largest, leveling at the largest level.

The default Z is above 1: eager leveling of the top is the most expensive
merging and it can starve the background pool under a merge backlog, so a
bounded number of parts at the top is traded for much less merge work.

Because ClickHouse merges produce level + 1 parts rather than filling a
fixed-capacity level, the merge fanout (K + 1 parts merged into the next level)
plays the role of the size ratio T of the paper, and levels grow organically
towards the top instead of being pre-allocated.

Tuning notes:
- Raising K lowers write amplification (parts are merged once per level) but
  increases the number of parts per partition: at most ~K * (L - 1) + Z parts,
  where L is the number of occupied levels.
- Raising Z lowers the frequency of the biggest (most expensive) merges but
  increases the number of parts at the largest level.
*/
namespace DB
{

class FluidLSMMergeSelector final : public IMergeSelector
{
public:
    struct Settings
    {
        /// Zero means unlimited.
        size_t max_parts_to_merge_at_once = 100;

        /// Bound on the number of parts at each level below the largest level
        /// (K in the paper). A merge is scheduled when a level accumulates
        /// more parts than this bound.
        size_t max_parts_at_lower_levels = 9;

        /// Bound on the number of parts at the largest level (Z in the paper).
        size_t max_parts_at_largest_level = 20;

        /// Merge ranges for overflowing lower levels are chosen by the Simple
        /// merge selector: FluidLSM decides when to merge (a level overflows
        /// its bound) and Simple decides what to merge. Under a merge backlog
        /// it picks wide, possibly cross-level windows like Simple does, while
        /// under light load the overflowing run is usually the only window that
        /// qualifies, preserving lazy leveling.
        SimpleMergeSelector::Settings simple;

        /// If it's not 0, a parts range whose youngest part is at least this
        /// old is merged unconditionally.
        size_t min_age_to_force_merge = 0;

        /// If it's not 0, a parts range inside a partition whose youngest part
        /// is at least this old is merged unconditionally.
        size_t min_partition_age_to_force_merge = 0;

        /// Merge whole ranges (used by OPTIMIZE).
        bool aggressive = false;

        /// Add this to the part size before estimating the merge cost. It means:
        /// merging even very small parts has its fixed cost.
        size_t size_fixed_cost_to_add = 5 * 1024 * 1024;

        const PartitionsStatistics * partitions_stats = nullptr;
    };

    explicit FluidLSMMergeSelector(const Settings & settings_) : settings(settings_) {}

    PartsRanges select(
        const PartsRanges & parts_ranges,
        const MergeConstraints & merge_constraints,
        const RangeFilter & range_filter) const override;

private:
    const Settings settings;
};

}
