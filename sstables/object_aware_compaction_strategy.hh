/*
 * Copyright (C) 2020 ScyllaDB
 */

/*
 * This file is part of Scylla.
 *
 * Scylla is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Scylla is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */
 
#pragma once

#include "compaction_strategy.hh"
#include "compaction_strategy_impl.hh"
#include "compaction_descriptor.hh"
#include <map>
#include <unordered_map>
#include <vector>
 
namespace sstables {

class object_aware_compaction_strategy : public compaction_strategy_impl {
    compaction_backlog_tracker _backlog_tracker;
    std::optional<sstring> _object_id_pk_component;
    std::optional<unsigned> _object_id_pk_component_idx;
public:
    using objects_map = std::unordered_map<bytes_view, std::vector<shared_sstable>>;
    static constexpr auto object_id_key = "object-identifier";
private:
    void maybe_init_pk_component_idx(const schema_ptr& schema);

    objects_map group_sstables_by_pk_component(const std::vector<sstables::shared_sstable>& ssts) const;
public:
    object_aware_compaction_strategy(const std::map<sstring, sstring>& options);

    virtual compaction_descriptor
    get_sstables_for_compaction(column_family& cf, std::vector<sstables::shared_sstable> candidates) override;

    virtual int64_t estimated_pending_compactions(column_family& cf) const override {
        return 0;
    }

    virtual compaction_strategy_type type() const {
        return compaction_strategy_type::object_aware;
    }

    virtual compaction_backlog_tracker& get_backlog_tracker() override;
    
    // TODO: implement adjust_partition_estimate
    // virtual uint64_t adjust_partition_estimate(const mutation_source_metadata& ms_meta, uint64_t partition_estimate) override;

    virtual reader_consumer make_interposer_consumer(const mutation_source_metadata& ms_meta, reader_consumer end_consumer) override;

    virtual bool use_interposer_consumer() const override {
        return true;
    }
};

}
