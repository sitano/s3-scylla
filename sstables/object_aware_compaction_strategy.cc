#include "object_aware_compaction_strategy.hh"
#include "database.hh"
#include "sstables.hh"
#include "service/priority_manager.hh"
#include <algorithm>

namespace sstables {

void object_aware_compaction_strategy::maybe_init_pk_component_idx(const schema_ptr& schema) {
    if (_object_id_pk_component_idx) {
        return;
    }
    for (auto& comp : schema->partition_key_columns()) {
        if (comp.name_as_text() == *_object_id_pk_component) {
            _object_id_pk_component_idx = comp.component_index();
            return;
        }
    }
    throw std::runtime_error("no pk component with this name");
}

object_aware_compaction_strategy::objects_map
object_aware_compaction_strategy::group_sstables_by_pk_component(const std::vector<sstables::shared_sstable>& ssts) const {
    objects_map map;
    for (auto& sst : ssts) {
        const auto& first = sst->get_first_partition_key();
        const auto& last = sst->get_last_partition_key();

        bytes_view first_pk_comp = first.get_component(*sst->get_schema(), *_object_id_pk_component_idx);
        bytes_view last_pk_comp = last.get_component(*sst->get_schema(), *_object_id_pk_component_idx);
        // sstable should only store data for a single object
        assert(first_pk_comp == last_pk_comp);

        map[first_pk_comp].push_back(sst);
    }
    return map;
}

compaction_descriptor
object_aware_compaction_strategy::get_sstables_for_compaction(column_family& cf,
                                                              std::vector<sstables::shared_sstable> candidates) {
    // TODO:
    // - Find if any sstable break the invariant (store data for more than one object (PK COMPONENT)), and compact it for the invariant to be restored.
    // NOTE: Only proceed to step 2, if the invariant is not broken.

    if (!_object_id_pk_component) {
        return compaction_descriptor();
    }
    maybe_init_pk_component_idx(cf.schema());

    objects_map map = group_sstables_by_pk_component(candidates);

    // Compact a set of sstables for a given object if there's a sstable in that set with a tombstone inside,
    // to garbage collect the deleted data from a specific object.
    for (auto& entry : map) {
        bool has_old_enough_tombstone = std::any_of(entry.second.begin(), entry.second.end(), [this] (const shared_sstable& sst) {
            bool has_tombstone = sst->get_stats_metadata().estimated_tombstone_drop_time.bin.size() > 0;
            // age of tombstone is taken into account, to prevent tombstone compaction from being
            // triggered infinitely
            if (has_tombstone) {
                return (db_clock::now()-_tombstone_compaction_interval) < sst->data_file_write_time();
            }
            return false;
        });
        if (has_old_enough_tombstone) {
            return compaction_descriptor(std::move(entry.second),
                                         cf.get_sstable_set(),
                                         service::get_local_compaction_priority());
        }
        // TODO: perform STCS if the set of sstables, for a given object, is growing too large???
    }

    return compaction_descriptor();
}

}
