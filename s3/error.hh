/*
 * Copyright 2019 ScyllaDB
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

#include <seastar/http/httpd.hh>
#include "seastarx.hh"

namespace s3 {

// api_error contains a DynamoDB error message to be returned to the user.
// It can be returned by value (see executor::request_return_type) or thrown.
// The DynamoDB's error messages are described in detail in
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Programming.Errors.html
// An error message has an HTTP code (almost always 400), a type, e.g.,
// "ResourceNotFoundException", and a human readable message.
// Eventually alternator::api_handler will convert a returned or thrown
// api_error into a JSON object, and that is returned to the user.
class api_error final {
public:
    using status_type = httpd::reply::status_type;
    status_type _http_code;
    std::string _msg;
    api_error(std::string msg, status_type http_code)
        : _http_code(std::move(http_code))
        , _msg(std::move(msg))
    { }

};

}

