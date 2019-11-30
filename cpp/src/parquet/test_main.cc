#include <seastar/testing/test_case.hh>
#include <seastar/core/thread.hh>
#include <gtest/gtest.h>

SEASTAR_TEST_CASE (gtest) {
  return seastar::async([] {
    int ret = RUN_ALL_TESTS();
    BOOST_CHECK(ret == 0);
  });
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return seastar::testing::entry_point(argc, argv);
}
