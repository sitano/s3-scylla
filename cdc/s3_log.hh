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

#include <queue>

#include "bytes.hh"
#include "exceptions/exceptions.hh"
#include "timestamp.hh"
#include "tracing/trace_state.hh"
#include "utils/UUID.hh"
#include "service/storage_proxy.hh"
#include "database.hh"

class mutation;

namespace cdc {

class s3_log_accumulator {
    bytes _current_chunk;
    int _current_chunk_number = 0;
    int _total_length = 0;
    bool _initialized = false;

    // Because we don't want to end the object in the middle of some
    // line, we store _current_length which is the last position of
    // line (last byte) inside last fully-completed chunk. _line_endings
    // queue allows us to update _current_length.
    int _current_length = 0;
    std::queue<int> _line_endings;

    utils::UUID _object_id;
    utils::UUID _blob_id;
    sstring _object_name;

public:
    void append_cdc_log_mutations(const std::vector<mutation>& cdc_log_mutations);

    std::vector<mutation> generate_mutations();
};

} // namespace cdc