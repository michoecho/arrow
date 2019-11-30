#include <seastar/testing/test_case.hh>
#include <gtest/gtest.h>

SEASTAR_TEST_CASE (gtest) {
  int ret = RUN_ALL_TESTS();
  BOOST_CHECK(ret == 0);
  return seastar::make_ready_future<>();
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return seastar::testing::entry_point(argc, argv);
}
