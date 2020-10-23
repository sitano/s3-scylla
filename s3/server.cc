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

#include "s3/server.hh"
#include "log.hh"
#include <seastar/http/function_handlers.hh>
#include "seastarx.hh"
#include "error.hh"
#include <cctype>
#include "cql3/query_processor.hh"
#include "service/storage_service.hh"
#include "utils/overloaded_functor.hh"

static logging::logger slogger("s3-server");

using namespace httpd;

namespace s3 {

#if 0 // no
inline std::vector<std::string_view> split(std::string_view text, char separator) {
    std::vector<std::string_view> tokens;
    if (text == "") {
        return tokens;
    }

    while (true) {
        auto pos = text.find_first_of(separator);
        if (pos != std::string_view::npos) {
            tokens.emplace_back(text.data(), pos);
            text.remove_prefix(pos + 1);
        } else {
            tokens.emplace_back(text);
            break;
        }
    }
    return tokens;
}
#endif

// DynamoDB HTTP error responses are structured as follows
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// Our handlers throw an exception to report an error. If the exception
// is of type alternator::api_error, it unwrapped and properly reported to
// the user directly. Other exceptions are unexpected, and reported as
// Internal Server Error.
class api_handler : public handler_base {
public:
    api_handler(const std::function<future<request_return_type>(std::unique_ptr<request> req)>& _handle) : _f_handle(
         [this, _handle](std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
         return seastar::futurize_invoke(_handle, std::move(req)).then_wrapped([this, rep = std::move(rep)](future<request_return_type> resf) mutable {
             if (resf.failed()) {
                 // TODO: how does s3 return errors?
                 try {
                     resf.get();
                 } catch (api_error &ae) {
                     generate_error_reply(*rep, ae);
                 } catch (...) {
                     generate_error_reply(*rep,
                             api_error(format("Internal server error: {}", std::current_exception()), httpd::reply::status_type::internal_server_error));
                }
                 return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
             }
             auto res = resf.get0();
            // TODO return shouldn't have been a srtring, should be a
            // streamm.... and use write_body...
            // rep->write_body("json", std::move(json_return_value._body_writer));
            rep->_content += res;
             return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
         });
    }), _type("json") { }

    api_handler(const api_handler&) = default;
    future<std::unique_ptr<reply>> handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        return _f_handle(std::move(req), std::move(rep)).then(
                [this](std::unique_ptr<reply> rep) {
                    rep->done(_type);
                    return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
                });
    }

protected:
    void generate_error_reply(reply& rep, const api_error& err) {
        rep._content += err._msg;
        rep._status = err._http_code;
        slogger.trace("api_handler error case: {}", rep._content);
    }

    future_handler_function _f_handle;
    sstring _type;
};

class gated_handler : public handler_base {
    seastar::gate& _gate;
public:
    gated_handler(seastar::gate& gate) : _gate(gate) {}
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) = 0;
    virtual future<std::unique_ptr<reply>> handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) final override {
        return with_gate(_gate, [this, &path, req = std::move(req), rep = std::move(rep)] () mutable {
            return do_handle(path, std::move(req), std::move(rep));
        });
    }
};

class health_handler : public gated_handler {
public:
    health_handler(seastar::gate& pending_requests) : gated_handler(pending_requests) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        rep->set_status(reply::status_type::ok);
        rep->write_body("txt", format("healthy: {}", req->get_header("Host")));
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

class local_nodelist_handler : public gated_handler {
public:
    local_nodelist_handler(seastar::gate& pending_requests) : gated_handler(pending_requests) {}
protected:
    virtual future<std::unique_ptr<reply>> do_handle(const sstring& path, std::unique_ptr<request> req, std::unique_ptr<reply> rep) override {
        rjson::value results = rjson::empty_array();
        // It's very easy to get a list of all live nodes on the cluster,
        // using gms::get_local_gossiper().get_live_members(). But getting
        // just the list of live nodes in this DC needs more elaborate code:
        sstring local_dc = locator::i_endpoint_snitch::get_local_snitch_ptr()->get_datacenter(
                utils::fb_utilities::get_broadcast_address());
        std::unordered_set<gms::inet_address> local_dc_nodes =
                service::get_local_storage_service().get_token_metadata().
                get_topology().get_datacenter_endpoints().at(local_dc);
        for (auto& ip : local_dc_nodes) {
            if (gms::get_local_gossiper().is_alive(ip)) {
                rjson::push_back(results, rjson::from_string(ip.to_sstring()));
            }
        }
        rep->set_status(reply::status_type::ok);
        rep->set_content_type("json");
        rep->_content = rjson::print(results);
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

#if 1
future<> server::verify_signature(const request& req) {
        return make_ready_future<>();
}
#else // copied from Alternator, untested in S3
future<> server::verify_signature(const request& req) {
    if (!_enforce_authorization) {
        slogger.debug("Skipping authorization");
        return make_ready_future<>();
    }
    auto host_it = req._headers.find("Host");
    if (host_it == req._headers.end()) {
        throw api_error::invalid_signature("Host header is mandatory for signature verification");
    }
    auto authorization_it = req._headers.find("Authorization");
    if (authorization_it == req._headers.end()) {
        throw api_error::invalid_signature("Authorization header is mandatory for signature verification");
    }
    std::string host = host_it->second;
    std::vector<std::string_view> credentials_raw = split(authorization_it->second, ' ');
    std::string credential;
    std::string user_signature;
    std::string signed_headers_str;
    std::vector<std::string_view> signed_headers;
    for (std::string_view entry : credentials_raw) {
        std::vector<std::string_view> entry_split = split(entry, '=');
        if (entry_split.size() != 2) {
            if (entry != "AWS4-HMAC-SHA256") {
                throw api_error::invalid_signature(format("Only AWS4-HMAC-SHA256 algorithm is supported. Found: {}", entry));
            }
            continue;
        }
        std::string_view auth_value = entry_split[1];
        // Commas appear as an additional (quite redundant) delimiter
        if (auth_value.back() == ',') {
            auth_value.remove_suffix(1);
        }
        if (entry_split[0] == "Credential") {
            credential = std::string(auth_value);
        } else if (entry_split[0] == "Signature") {
            user_signature = std::string(auth_value);
        } else if (entry_split[0] == "SignedHeaders") {
            signed_headers_str = std::string(auth_value);
            signed_headers = split(auth_value, ';');
            std::sort(signed_headers.begin(), signed_headers.end());
        }
    }
    std::vector<std::string_view> credential_split = split(credential, '/');
    if (credential_split.size() != 5) {
        throw api_error::validation(format("Incorrect credential information format: {}", credential));
    }
    std::string user(credential_split[0]);
    std::string datestamp(credential_split[1]);
    std::string region(credential_split[2]);
    std::string service(credential_split[3]);

    std::map<std::string_view, std::string_view> signed_headers_map;
    for (const auto& header : signed_headers) {
        signed_headers_map.emplace(header, std::string_view());
    }
    for (auto& header : req._headers) {
        std::string header_str;
        header_str.resize(header.first.size());
        std::transform(header.first.begin(), header.first.end(), header_str.begin(), ::tolower);
        auto it = signed_headers_map.find(header_str);
        if (it != signed_headers_map.end()) {
            it->second = std::string_view(header.second);
        }
    }

    auto cache_getter = [] (std::string username) {
        return get_key_from_roles(cql3::get_query_processor().local(), std::move(username));
    };
    return _key_cache.get_ptr(user, cache_getter).then([this, &req,
                                                    user = std::move(user),
                                                    host = std::move(host),
                                                    datestamp = std::move(datestamp),
                                                    signed_headers_str = std::move(signed_headers_str),
                                                    signed_headers_map = std::move(signed_headers_map),
                                                    region = std::move(region),
                                                    service = std::move(service),
                                                    user_signature = std::move(user_signature)] (key_cache::value_ptr key_ptr) {
        std::string signature = get_signature(user, *key_ptr, std::string_view(host), req._method,
                datestamp, signed_headers_str, signed_headers_map, req.content, region, service, "");

        if (signature != std::string_view(user_signature)) {
            _key_cache.remove(user);
            throw api_error::unrecognized_client("The security token included in the request is invalid.");
        }
    });
}
#endif

future<request_return_type> server::handle_request(std::unique_ptr<request>&& req) {
    slogger.trace("Got request: method='{}' url='{}' parameters='{}' headers='{}' content='{}'", req->_method, req->_url, req->query_parameters, req->_headers, req->content);

    return verify_signature(*req).then([this, req = std::move(req)] () mutable {
        // CONTINUE HERE!
        return with_gate(_pending_requests, [this, req = std::move(req)] () mutable {
            //FIXME: Client state can provide more context, e.g. client's endpoint address
            // We use unique_ptr because client_state cannot be moved or copied
            return do_with(std::make_unique<service::client_state>(service::client_state::internal_tag()),
                    [this, req = std::move(req)] (std::unique_ptr<service::client_state>& client_state) mutable {
                //tracing::trace_state_ptr trace_state = executor::maybe_trace_query(*client_state, op, req->content);
                //tracing::trace(trace_state, op);
                size_t mem_estimate = req->content.size() * 3 + 8000;
                auto units_fut = get_units(*_memory_limiter, mem_estimate);
                if (_memory_limiter->waiters()) {
                    //++_executor._stats.requests_blocked_memory;
                }
                return units_fut.then([this, &client_state, /*trace_state,*/ req = std::move(req)] (semaphore_units<> units) mutable {
                    throw api_error("Bad Request", httpd::reply::status_type::bad_request);
                    return std::string("");
                });
            });
        });
    });
}

void server::set_routes(routes& r) {
    match_rule *get_rule = new match_rule(new api_handler([this] (std::unique_ptr<request> req) mutable {
        return handle_request(std::move(req));
    }));
    r.add(get_rule, operation_type::GET);
    r.add(get_rule, operation_type::PUT);
}

future<> server::init(net::inet_address addr, std::optional<uint16_t> port, std::optional<uint16_t> https_port, std::optional<tls::credentials_builder> creds,
        bool enforce_authorization, semaphore* memory_limiter) {
    _memory_limiter = memory_limiter;
    _enforce_authorization = enforce_authorization;
    if (!port && !https_port) {
        return make_exception_future<>(std::runtime_error("Either regular port or TLS port"
                " must be specified in order to init an alternator HTTP server instance"));
    }
    return seastar::async([this, addr, port, https_port, creds] {
        try {
            //_executor.start().get();

            if (port) {
                set_routes(_http_server._routes);
                //_http_server.set_content_length_limit(server::content_length_limit);
                _http_server.listen(socket_address{addr, *port}).get();
                _enabled_servers.push_back(std::ref(_http_server));
            }
            if (https_port) {
                set_routes(_https_server._routes);
                //_https_server.set_content_length_limit(server::content_length_limit);
                _https_server.set_tls_credentials(creds->build_reloadable_server_credentials([](const std::unordered_set<sstring>& files, std::exception_ptr ep) {
                    if (ep) {
                        slogger.warn("Exception loading {}: {}", files, ep);
                    } else {
                        slogger.info("Reloaded {}", files);
                    }
                }).get0());
                _https_server.listen(socket_address{addr, *https_port}).get();
                _enabled_servers.push_back(std::ref(_https_server));
            }
        } catch (...) {
            slogger.error("Failed to set up S3 HTTP server on {} port {}, TLS port {}: {}",
                    addr, port ? std::to_string(*port) : "OFF", https_port ? std::to_string(*https_port) : "OFF", std::current_exception());
            std::throw_with_nested(std::runtime_error(
                    format("Failed to set up S3 HTTP server on {} port {}, TLS port {}",
                            addr, port ? std::to_string(*port) : "OFF", https_port ? std::to_string(*https_port) : "OFF")));
        }
    });
}

future<> server::stop() {
    return parallel_for_each(_enabled_servers, [] (http_server& server) {
        return server.stop();
    }).then([this] {
        return _pending_requests.close();
    });
}

}

