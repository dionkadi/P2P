// Http/HttpServer.hpp
#pragma once

#include "Utils.hpp"

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <functional>
#include <map>
#include <string>
#include <memory>

namespace beast = boost::beast;
namespace http = beast::http;
namespace asio = boost::asio;
using tcp = asio::ip::tcp;
using HttpRequest = http::request<http::string_body>;
using HttpResponse = http::response<http::string_body>;
using HttpHandler = std::function<asio::awaitable<HttpResponse>(HttpRequest)>;

static constexpr std::chrono::seconds HTTP_SESSION_TIMEOUT = std::chrono::seconds(30);

class HttpRouter {
public:
    void add_route(std::string_view path, HttpHandler handler) {
        routes_[std::string(path)] = handler;
    }
    asio::awaitable<HttpResponse> handle_request(HttpRequest req) {
        auto target = req.target();
        auto query_pos = target.find('?');
        auto path = (query_pos != std::string_view::npos) ? target.substr(0, query_pos) : target;
        auto it = routes_.find(path);
        if (it != routes_.end()) {
            co_return co_await it->second(std::move(req));
        } else {
            HttpResponse res{http::status::not_found, req.version()};
            res.set(http::field::server, BOOST_BEAST_VERSION_STRING);
            res.set(http::field::content_type, "text/plain");
            res.keep_alive(req.keep_alive());
            res.body() = std::format("The resource '{}' was not found.", req.target());
            res.prepare_payload();
            co_return res;
        }
    }
private:
    std::map<std::string, HttpHandler, std::less<>> routes_;
};

inline asio::awaitable<void> http_session(tcp::socket socket, std::shared_ptr<HttpRouter> router) {
    beast::error_code ec;
    beast::tcp_stream stream(std::move(socket));
    beast::flat_buffer buffer;
    stream.expires_after(HTTP_SESSION_TIMEOUT);
    try {
        while (true) {
            HttpRequest req;
            co_await http::async_read(stream, buffer, req, asio::use_awaitable);
            
            req.set("remote_endpoint", stream.socket().remote_endpoint().address().to_string());
            HttpResponse res = co_await router->handle_request(std::move(req));
            
            bool const keep_alive = res.keep_alive();
            
            co_await http::async_write(stream, std::move(res), asio::use_awaitable);
            
            if (!keep_alive) {
                break;
            }

            stream.expires_after(HTTP_SESSION_TIMEOUT);
        }
    } catch (const boost::system::system_error& e) {
        if (e.code() == http::error::end_of_stream || 
            e.code() == asio::error::eof ||
            e.code() == asio::error::connection_reset) 
        {
            // Normal disconnect
        } else {
            LOGERR("HTTP Session error: {}", e.what());
        }
    } catch (const std::exception& e) {
        LOGERR("HTTP Session unexpected error: {}", e.what());
    }

    ec = stream.socket().shutdown(tcp::socket::shutdown_send, ec);
    if (ec && ec != asio::error::not_connected) { // Ignore 'not_connected' during shutdown
        LOGWARN("HTTP Session socket shutdown error: {}", ec.message()); // String formatting
    }
}

inline asio::awaitable<void> http_listener(asio::io_context& ioc, tcp::endpoint endpoint, std::shared_ptr<HttpRouter> router) {
    auto executor = co_await asio::this_coro::executor;
    tcp::acceptor acceptor(executor, endpoint);
    LOGINFO("HTTP Server listening on port {}", endpoint.port());
    
    try {
        while (true) {
            tcp::socket socket = co_await acceptor.async_accept(asio::use_awaitable);
            LOGINFO("[HTTP] Accepted connection from {}", socket.remote_endpoint().address().to_string());
            
            asio::co_spawn(executor, http_session(std::move(socket), router), asio::detached);
        }
    } catch (const std::exception& e) {
        LOGCRITICAL("HTTP Listener failed: {}", e.what());
    }
}