#include "influxdb2_simple_async_api.h"
#include "influxdb_line.h"
#include "input_sanitizer.h"

#include <cpprest/http_client.h>
#include <cpprest/json.h>
#include <cpprest/rawptrstream.h>
#include <fmt/ostream.h>
#include <rx.hpp>
#include <atomic>
#include <chrono>

using namespace influxdb::utility;
using namespace web::http::client;
using namespace web::http;
using namespace utility;
using namespace web::http::compression::builtin;

namespace influxdb2
{
namespace async_api
{
struct simple_db::impl
{
  http_client client;
  uri uri_with_db;
  std::string org;
  std::string orgid;
  std::string bucket;
  std::string bucketid;
  std::string token;
  std::atomic<bool> started;
  rxcpp::subscription listener;
  rxcpp::subjects::subject<influxdb::api::line> subj;
  const int duration_seconds;
  const int shard_duration_seconds;
  const int window_max_lines;
  const std::chrono::milliseconds window_max_ms;

  impl(std::string const& url, std::string const& org, std::string const& bucket,
       std::string const& token, const int duration_seconds, const int shard_duration_seconds,
       const int window_max_lines, const int window_max_ms)
      : client(url),
        org(org),
        bucket(bucket),
        token(token),
        started(false),
        duration_seconds(duration_seconds),
        shard_duration_seconds(shard_duration_seconds),
        window_max_lines(window_max_lines),
        window_max_ms(window_max_ms)
  {
    throw_on_invalid_identifier(org);
    throw_on_invalid_identifier(bucket);
    uri_builder builder(client.base_uri());
    builder.append(U("/api/v2/write"));
    builder.append_query(U("org"), org);
    builder.append_query(U("bucket"), bucket);
    uri_with_db = builder.to_uri();
    start_once();
  }

  void start_once()
  {
    if (started)
    {
      return;
    }

    started = true;

    auto incoming_requests = subj.get_observable().map([](auto&& line) { return line.get(); });

    listener =
        incoming_requests
            .window_with_time_or_count(window_max_ms, int(window_max_lines),
                                       rxcpp::synchronize_new_thread())
            .subscribe([this](rxcpp::observable<std::string> window) {
              window
                  .scan(std::make_shared<fmt::MemoryWriter>(),
                        [this](std::shared_ptr<fmt::MemoryWriter> const& w, std::string const& v) {
                          *w << v << '\n';
                          return w;
                        })
                  .start_with(std::make_shared<fmt::MemoryWriter>())
                  .last()
                  .observe_on(rxcpp::synchronize_new_thread())
                  .subscribe(
                      [this](std::shared_ptr<fmt::MemoryWriter> const& w) {
                        if (w->size() > 0u)
                        {
                          try
                          {
                            std::vector<concurrency::streams::istream> streams;
                            auto c = make_compressor(algorithm::GZIP);

                            bool done = false;
                            size_t processed = 0;
                            size_t csize = 0;
                            std::vector<uint8_t> pre;
                            pre.resize(w->size());
                            while (!done)
                            {
                              size_t used = 0;
                              if (csize == pre.size())
                              {
                                pre.resize(csize * 2);
                              }
                              auto got = c->compress(
                                  reinterpret_cast<const uint8_t*>(w->data()) + processed,
                                  w->size() - processed, pre.data() + csize, pre.size() - csize,
                                  web::http::compression::operation_hint::is_last, used, done);
                              csize += got;
                              processed += used;
                            }
                            streams.emplace_back(
                                concurrency::streams::rawptr_stream<uint8_t>::open_istream(
                                    pre.data(), csize));

                            for (auto& stream : streams)
                            {
                              http_request request;
                              request.set_request_uri(uri_with_db);
                              request.set_method(methods::POST);
                              request.headers().add("Authorization", "Token " + token);
                              request.headers().add(header_names::content_encoding,
                                                    algorithm::GZIP);

                              request.set_body(stream);
                              auto response = client.request(request);
                              try
                              {
                                response.wait();
                                if (!(response.get().status_code() == status_codes::OK ||
                                      response.get().status_code() == status_codes::NoContent))
                                {
                                  throw std::runtime_error(response.get().extract_string().get());
                                }
                              }
                              catch (const std::exception& e)
                              {
                                throw std::runtime_error(e.what());
                              }
                            }
                          }
                          catch (const std::exception& e)
                          {
                            throw std::runtime_error(std::string("async_api::insert failed: ") +
                                                     e.what() + " -> Dropping " +
                                                     std::to_string(w->size()) + " bytes");
                          }
                        }
                      },
                      [](std::exception_ptr ep) {
                        try
                        {
                          std::rethrow_exception(ep);
                        }
                        catch (const std::runtime_error& ex)
                        {
                          std::cerr << ex.what() << std::endl;
                        }
                      });
            });
  }

  web::json::value get_json(const web::uri& uri)
  {
    http_request req;
    req.headers().add("Authorization", "Token " + token);
    req.set_request_uri(uri);
    auto response = client.request(req);
    try
    {
      response.wait();
      if (response.get().status_code() == status_codes::OK)
      {
        return response.get().extract_json().get();
      }
      else
      {
        throw std::runtime_error(response.get().extract_string().get());
        return web::json::value();
      }
    }
    catch (const std::exception& e)
    {
      throw std::runtime_error(e.what());
    }
  }

  std::string get_orgid(const std::string& org_name)
  {
    uri_builder builder(U("/api/v2/orgs"));
    builder.append_query(U("org"), org_name);
    const auto orgid = get_json(builder.to_uri())["orgs"][0]["id"];
    if (!orgid.is_string())
    {
      return "";
    }
    return orgid.as_string();
  }

  web::json::object get_bucket(const std::string& orgid, const std::string& bucket_name)
  {
    uri_builder builder(U("/api/v2/buckets"));
    builder.append_query(U("orgID"), orgid);
    builder.append_query(U("name"), bucket_name);
    try
    {
      auto buckets = get_json(builder.to_uri())["buckets"];
      if (buckets.size())
      {
        return buckets[0].as_object();
      }
    }
    catch (const std::runtime_error& e)
    {
      // ignore
    }
    return web::json::value::object().as_object();
  }

  void create_bucket(const std::string& orgid, const std::string& bucket_name)
  {
    uri_builder builder(U("/api/v2/buckets"));
    auto json_body = web::json::value::object(
        {std::make_pair("name", web::json::value::string(bucket)),
         std::make_pair("orgID", web::json::value::string(orgid)),
         std::make_pair(
             "retentionRules",
             web::json::value::array({web::json::value::object({
                 std::make_pair("everySeconds", web::json::value::number(duration_seconds)),
                 std::make_pair("shardGroupDurationSeconds",
                                web::json::value::number(shard_duration_seconds)),
                 std::make_pair("type", web::json::value::string("expire")),
             })}))});
    http_request req;
    req.set_request_uri(builder.to_uri());
    req.set_method(methods::POST);
    req.headers().add("Authorization", "Token " + token);
    req.set_body(json_body);
    auto response = client.request(req);
    response.wait();
    if (response.get().status_code() != status_codes::Created)
    {
      throw std::runtime_error(response.get().extract_string().get());
    }
  }

  ~impl()
  {
    started = false;
    listener.unsubscribe();
  }
};

simple_db::simple_db(std::string const& url, std::string const& org, std::string const& bucket)
    : simple_db(url, org, bucket, "", 0, 0, 50000, 100)
{
}

simple_db::simple_db(std::string const& url, std::string const& org, std::string const& bucket,
                     std::string const& token, const int duration_seconds,
                     const int shard_duration_seconds, const int window_max_lines,
                     const int window_max_ms)
    : influxdb::async_api::simple_db(url, bucket, window_max_lines, window_max_ms),
      pimpl(std::make_unique<impl>(url, org, bucket, token, duration_seconds,
                                   shard_duration_seconds, window_max_lines, window_max_ms))
{
}

simple_db::~simple_db() { pimpl->started = false; }

void simple_db::create()
{
  pimpl->orgid = pimpl->get_orgid(pimpl->org);
  auto bucket_obj = pimpl->get_bucket(pimpl->orgid, pimpl->bucket);
  if (bucket_obj.empty())
  {
    // bucket not found
    pimpl->create_bucket(pimpl->orgid, pimpl->bucket);
  }
  else
  {
    pimpl->bucketid = bucket_obj["id"].as_string();
    const int duration = bucket_obj["retentionRules"][0]["everySeconds"].as_number().to_int64();
    int shard_duration = 0;
    if (bucket_obj["retentionRules"][0].has_field("shardGroupDurationSeconds"))
    {
      shard_duration =
          bucket_obj["retentionRules"][0]["shardGroupDurationSeconds"].as_number().to_int64();
    }

    if (duration != pimpl->duration_seconds || shard_duration != pimpl->shard_duration_seconds)
    {
      std::ostringstream oss;
      oss << "retention rule mismatch: (" << duration << ", " << shard_duration << ")";
      throw influxdb2::retention_rule_error(oss.str().c_str());
    }
  }
}

void simple_db::insert(influxdb::api::line const& lines)
{
  auto subscriber = pimpl->subj.get_subscriber();

  if (!subscriber.is_subscribed())
  {
    return;
  }

  subscriber.on_next(lines);
}

std::string simple_db::get_orgid() const { return pimpl->orgid; }
std::string simple_db::get_bucketid() const { return pimpl->bucketid; }

}  // namespace async_api
}  // namespace influxdb2
