#include "object_aware_compaction_strategy.hh"
#include "database.hh"
#include "sstables.hh"

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

    // Compact a set of sstables for a given object if there's a sstable in that set with a tombstone inside
    for (auto& entry : map) {
        // TODO: return set for object if it has tombstone inside
        if (true) {
            return compaction_descriptor();
        }
    }

    return compaction_descriptor();
}

}
