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

#include "mutation_writer/pk_based_splitting_writer.hh"

#include <cinttypes>
#include <unordered_map>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/min_element.hpp>
#include <boost/range/adaptor/map.hpp>
#include <seastar/core/shared_mutex.hh>

#include "mutation_reader.hh"

namespace mutation_writer {

class pk_based_splitting_mutation_writer {
    class pk_writer {
        queue_reader_handle _handle;
        future<> _consume_fut;
    private:
        pk_writer(schema_ptr schema, std::pair<flat_mutation_reader, queue_reader_handle> queue_reader, reader_consumer& consumer)
            : _handle(std::move(queue_reader.second))
            , _consume_fut(consumer(std::move(queue_reader.first))) {
        }

    public:
        pk_writer(schema_ptr schema, reader_permit permit, reader_consumer& consumer)
            : pk_writer(schema, make_queue_reader(schema, std::move(permit)), consumer) {
        }
        future<> consume(mutation_fragment mf) {
            return _handle.push(std::move(mf));
        }
        future<> consume_end_of_stream() {
            // consume_end_of_stream is always called from a finally block,
            // and that's because we wait for _consume_fut to return. We
            // don't want to generate another exception here if the read was
            // aborted.
            if (!_handle.is_terminated()) {
                _handle.push_end_of_stream();
            }
            return std::move(_consume_fut);
        }
    };

private:    
    using pk_component_t = bytes;

    schema_ptr _schema;
    reader_permit _permit;
    reader_consumer _consumer;
    pk_component_t _current_pk;
    std::unordered_map<pk_component_t, std::optional<pk_writer>> _pks;
    const unsigned _pk_component_idx;

    future<> write_to_pk(mutation_fragment&& mf) {
        auto& writer = *_pks[_current_pk];
        return writer.consume(std::move(mf));
    }
    
    unsigned init_pk_component_idx(sstring pk_component) const {
        for (auto& comp : _schema->partition_key_columns()) {
            if (comp.name_as_text() == pk_component) {
                return comp.component_index();
            }
        }
        throw std::runtime_error("no pk component with this name");
    }

    pk_component_t get_pk_component_from_composed_key(const dht::decorated_key& dk) const {
        return bytes(dk.key().get_component(*_schema, _pk_component_idx));
    }
public:
    pk_based_splitting_mutation_writer(schema_ptr schema, reader_permit permit, reader_consumer consumer, sstring pk_component)
        : _schema(std::move(schema))
        , _permit(std::move(permit))
        , _consumer(std::move(consumer))
        , _pk_component_idx(init_pk_component_idx(std::move(pk_component)))
    {}

    future<> consume(partition_start&& ps) {
        _current_pk = get_pk_component_from_composed_key(ps.key());
        if (!_pks[_current_pk]) {
            _pks[_current_pk] = pk_writer(_schema, _permit, _consumer);
        }
        return write_to_pk(mutation_fragment(*_schema, _permit, std::move(ps)));
    }

    future<> consume(static_row&& sr) {
        return write_to_pk(mutation_fragment(*_schema, _permit, std::move(sr)));
    }

    future<> consume(clustering_row&& cr) {
        return write_to_pk(mutation_fragment(*_schema, _permit, std::move(cr)));
    }

    future<> consume(range_tombstone&& rt) {
        return write_to_pk(mutation_fragment(*_schema, _permit, std::move(rt)));
    }

    future<> consume(partition_end&& pe) {
        return write_to_pk(mutation_fragment(*_schema, _permit, std::move(pe)));
    }

    future<> consume_end_of_stream() {
        return parallel_for_each(_pks | boost::adaptors::map_values, [] (std::optional<pk_writer>& pk) {
            if (!pk) {
                return make_ready_future<>();
            }
            return pk->consume_end_of_stream();
        });
    }
};

future<> segregate_by_pk_component(flat_mutation_reader producer, reader_consumer consumer, sstring pk_component) {
    auto schema = producer.schema();
    auto permit = producer.permit();
    return feed_writer(
        std::move(producer),
        pk_based_splitting_mutation_writer(std::move(schema), std::move(permit), std::move(consumer), std::move(pk_component)));
}
} // namespace mutation_writer
