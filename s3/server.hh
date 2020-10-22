/*
 * Copyright 2020 ScyllaDB
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
 * You should have received a copy of the GNU Affero General Public License
 * along with Scylla.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/http/httpd.hh>
#include <seastar/net/tls.hh>
#include <optional>
#include "alternator/auth.hh"
#include "utils/small_vector.hh"
#include <seastar/core/units.hh>
#include <string>
#include "service/storage_proxy.hh"
#include "service/migration_manager.hh"
#include "service/client_state.hh"

namespace db {
    class system_distributed_keyspace;
}


namespace s3 {

// TODO: we need to return a stream, not a fully ready string!!
using request_return_type = std::string;

class server {
    service::storage_proxy& _proxy;
    service::migration_manager& _mm;
    db::system_distributed_keyspace& _sdks;
    service::storage_service& _ss;

    //static constexpr size_t content_length_limit = 16*MB;
    //using alternator_callback = std::function<future<executor::request_return_type>(executor&, executor::client_state&,
    //        tracing::trace_state_ptr, service_permit, rjson::value, std::unique_ptr<request>)>;
    //using alternator_callbacks_map = std::unordered_map<std::string_view, alternator_callback>;

    http_server _http_server;
    http_server _https_server;

#if 0
    key_cache _key_cache;
#endif
    bool _enforce_authorization;
    utils::small_vector<std::reference_wrapper<seastar::httpd::http_server>, 2> _enabled_servers;
    gate _pending_requests;
    //alternator_callbacks_map _callbacks;

    semaphore* _memory_limiter;

public:
    server(service::storage_proxy& proxy, service::migration_manager& mm, db::system_distributed_keyspace& sdks, service::storage_service& ss)
        : _proxy(proxy), _mm(mm), _sdks(sdks), _ss(ss)
        , _http_server("http-s3")
        , _https_server("https-s3")
        , _enforce_authorization(false)
        , _enabled_servers{}
        , _pending_requests{}
 {}

    future<> init(net::inet_address addr, std::optional<uint16_t> port, std::optional<uint16_t> https_port, std::optional<tls::credentials_builder> creds,
            bool enforce_authorization, semaphore* memory_limiter);
    future<> stop();
private:
    void set_routes(seastar::httpd::routes& r);
    future<> verify_signature(const seastar::httpd::request& r);
    future<request_return_type> handle_request(std::unique_ptr<request>&& req);
};

}

