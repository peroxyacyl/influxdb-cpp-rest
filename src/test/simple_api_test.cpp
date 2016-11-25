#include <catch.hpp>

#include "../influxdb-cpp-rest/influxdb_simple_api.h"
#include "../influxdb-cpp-rest/influxdb_simple_async_api.h"
#include "../influxdb-cpp-rest/influxdb_line.h"

#include "fixtures.h"

#include <chrono>
#include <thread>
#include <iostream>

using influxdb::api::simple_db;
using influxdb::api::key_value_pairs;
using influxdb::api::line;

namespace {
    struct simple_connected_test : connected_test {
        simple_db db;

        // drop and create test db
        simple_connected_test();

        // drop the db
        ~simple_connected_test();

        bool db_exists();

        struct result_t {
            std::string line;
            result_t(std::string const& line):line(line) {}

            inline bool contains(std::string const& what) const {
                return line.find(what) != std::string::npos;
            }
        };

        inline result_t result(std::string const& measurement) {
            return result_t(raw_db.get(std::string("select * from ") + db_name + ".." + measurement));
        }
    };
}

TEST_CASE_METHOD(simple_connected_test, "creating the db using the simple api", "[connected]") {
    CHECK(db_exists());
}

TEST_CASE("tags and values should be formatted according to the line protocol") {
    auto&& kvp=key_value_pairs("a", "b").add("b", 42).add("c", 33.01);

    CHECK(kvp.get().find("a=\"b\",b=42i,c=33.01") != std::string::npos);
}

TEST_CASE_METHOD(simple_connected_test, "inserting values using the simple api", "[connected]") {
    db.insert(line("test", key_value_pairs("mytag", 424242L), key_value_pairs("value", "hello world!")));

    wait_for([] {return false; },3);
    
    auto res = result("test");
    CHECK(res.contains("424242i"));
    CHECK(res.contains("mytag"));
    CHECK(res.contains("hello world!"));
}

#include <zmq.hpp>
#include <zmq_addon.hpp>

TEST_CASE_METHOD(simple_connected_test, "more than 1000 inserts per second") {
    influxdb::async_api::simple_db asyncdb("http://localhost:8086", db_name);
    using Clock = std::chrono::high_resolution_clock;
    const int count = 1024;
    printf("started sending\n");
    auto t1 = Clock::now();
    for (int i = 0; i < count; i++) {
        asyncdb.insert(line("asynctest", key_value_pairs("mytag", 424242L), key_value_pairs("value", "hello world!")));
    }
    auto t2 = Clock::now();
    printf("stopped sending\n");
    auto diff = t2 - t1;
    auto count_per_second = static_cast<double>(count) / (std::chrono::duration_cast<std::chrono::milliseconds>(t2 - t1).count() / 1000.);
    CHECK(count_per_second > 1000.0);
    WARN(count_per_second);
    auto query = std::string("select count(*) from ") + db_name + "..asynctest";
    // wait for asynchronous fill
    wait_for([this,query] { return raw_db.get(query).find("1024") != std::string::npos; }, 100);
    printf("%s\n", raw_db.get(query).c_str());
    //CHECK(raw_db.get(query).find("1024") != std::string::npos);
}

simple_connected_test::simple_connected_test() :
    db("http://localhost:8086", db_name)
{
}

simple_connected_test::~simple_connected_test()
{
}

bool simple_connected_test::db_exists()
{
    return database_exists(db_name);
}
