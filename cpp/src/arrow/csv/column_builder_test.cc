// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "arrow/csv/column_builder.h"
#include "arrow/csv/options.h"
#include "arrow/csv/test_common.h"
#include "arrow/memory_pool.h"
#include "arrow/table.h"
#include "arrow/testing/util.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/task_group.h"
#include "arrow/util/thread_pool.h"

namespace arrow {
namespace csv {

class BlockParser;

using internal::checked_cast;
using internal::GetCpuThreadPool;
using internal::TaskGroup;

using ChunkData = std::vector<std::vector<std::string>>;

void AssertBuilding(const std::shared_ptr<ColumnBuilder>& builder,
                    const ChunkData& chunks, std::shared_ptr<ChunkedArray>* out) {
  for (const auto& chunk : chunks) {
    std::shared_ptr<BlockParser> parser;
    MakeColumnParser(chunk, &parser);
    builder->Append(parser);
  }
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK(builder->Finish(out));
  ASSERT_OK((*out)->ValidateFull());
}

void CheckInferred(const std::shared_ptr<TaskGroup>& tg, const ChunkData& csv_data,
                   const ConvertOptions& options,
                   std::shared_ptr<ChunkedArray> expected) {
  std::shared_ptr<ColumnBuilder> builder;
  std::shared_ptr<ChunkedArray> actual;
  ASSERT_OK(ColumnBuilder::Make(default_memory_pool(), 0, options, tg, &builder));
  AssertBuilding(builder, csv_data, &actual);
  AssertChunkedEqual(*actual, *expected);
}

void CheckInferred(const std::shared_ptr<TaskGroup>& tg, const ChunkData& csv_data,
                   const ConvertOptions& options,
                   std::vector<std::shared_ptr<Array>> expected_chunks) {
  CheckInferred(tg, csv_data, options, std::make_shared<ChunkedArray>(expected_chunks));
}

void CheckFixedType(const std::shared_ptr<TaskGroup>& tg,
                    const std::shared_ptr<DataType>& type, const ChunkData& csv_data,
                    const ConvertOptions& options,
                    std::shared_ptr<ChunkedArray> expected) {
  std::shared_ptr<ColumnBuilder> builder;
  std::shared_ptr<ChunkedArray> actual;
  ASSERT_OK(ColumnBuilder::Make(default_memory_pool(), type, 0, options, tg, &builder));
  AssertBuilding(builder, csv_data, &actual);
  AssertChunkedEqual(*actual, *expected);
}

void CheckFixedType(const std::shared_ptr<TaskGroup>& tg,
                    const std::shared_ptr<DataType>& type, const ChunkData& csv_data,
                    const ConvertOptions& options,
                    std::vector<std::shared_ptr<Array>> expected_chunks) {
  CheckFixedType(tg, type, csv_data, options,
                 std::make_shared<ChunkedArray>(expected_chunks));
}

static ConvertOptions default_options = ConvertOptions::Defaults();

//////////////////////////////////////////////////////////////////////////
// Tests for null column builder

TEST(NullColumnBuilder, Empty) {
  std::shared_ptr<DataType> type = null();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::MakeNull(default_memory_pool(), type, tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {}, &actual);

  ChunkedArray expected({}, type);
  AssertChunkedEqual(*actual, expected);
}

TEST(NullColumnBuilder, InsertNull) {
  // Bulding a column of nulls with type null()
  std::shared_ptr<DataType> type = null();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::MakeNull(default_memory_pool(), type, tg, &builder));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  // Those values are indifferent, only the number of rows is used
  MakeColumnParser({"456", "789"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({"123"}, &parser);
  builder->Insert(0, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK(builder->Finish(&actual));
  ASSERT_OK(actual->ValidateFull());

  auto chunks =
      ArrayVector{std::make_shared<NullArray>(1), std::make_shared<NullArray>(2)};
  expected = std::make_shared<ChunkedArray>(chunks);
  AssertChunkedEqual(*actual, *expected);
}

TEST(NullColumnBuilder, InsertTyped) {
  // Bulding a column of nulls with another type
  std::shared_ptr<DataType> type = int16();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::MakeNull(default_memory_pool(), type, tg, &builder));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  // Those values are indifferent, only the number of rows is used
  MakeColumnParser({"abc", "def", "ghi"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({"jkl"}, &parser);
  builder->Insert(0, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK(builder->Finish(&actual));
  ASSERT_OK(actual->ValidateFull());

  auto chunks = ArrayVector{ArrayFromJSON(type, "[null]"),
                            ArrayFromJSON(type, "[null, null, null]")};
  expected = std::make_shared<ChunkedArray>(chunks);
  AssertChunkedEqual(*actual, *expected);
}

TEST(NullColumnBuilder, EmptyChunks) {
  std::shared_ptr<DataType> type = int16();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(ColumnBuilder::MakeNull(default_memory_pool(), type, tg, &builder));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  // Those values are indifferent, only the number of rows is used
  MakeColumnParser({}, &parser);
  builder->Insert(0, parser);
  MakeColumnParser({"abc", "def"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({}, &parser);
  builder->Insert(2, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK(builder->Finish(&actual));
  ASSERT_OK(actual->ValidateFull());

  auto chunks =
      ArrayVector{ArrayFromJSON(type, "[]"), ArrayFromJSON(type, "[null, null]"),
                  ArrayFromJSON(type, "[]")};
  expected = std::make_shared<ChunkedArray>(chunks);
  AssertChunkedEqual(*actual, *expected);
}

//////////////////////////////////////////////////////////////////////////
// Tests for fixed-type column builder

TEST(ColumnBuilder, Empty) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(
      ColumnBuilder::Make(default_memory_pool(), int32(), 0, options, tg, &builder));

  std::shared_ptr<ChunkedArray> actual;
  AssertBuilding(builder, {}, &actual);

  ChunkedArray expected({}, int32());
  AssertChunkedEqual(*actual, expected);
}

TEST(ColumnBuilder, Basics) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckFixedType(tg, int32(), {{"123", "-456"}}, options,
                 {ArrayFromJSON(int32(), "[123, -456]")});
}

TEST(ColumnBuilder, Insert) {
  // Test ColumnBuilder::Insert()
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ColumnBuilder> builder;
  ASSERT_OK(
      ColumnBuilder::Make(default_memory_pool(), int32(), 0, options, tg, &builder));

  std::shared_ptr<BlockParser> parser;
  std::shared_ptr<ChunkedArray> actual, expected;
  MakeColumnParser({"456"}, &parser);
  builder->Insert(1, parser);
  MakeColumnParser({"123"}, &parser);
  builder->Insert(0, parser);
  ASSERT_OK(builder->task_group()->Finish());
  ASSERT_OK(builder->Finish(&actual));
  ASSERT_OK(actual->ValidateFull());

  ChunkedArrayFromVector<Int32Type>({{123}, {456}}, &expected);
  AssertChunkedEqual(*actual, *expected);
}

TEST(ColumnBuilder, MultipleChunks) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckFixedType(tg, int16(), {{"1", "2", "3"}, {"4", "5"}}, options,
                 {ArrayFromJSON(int16(), "[1, 2, 3]"), ArrayFromJSON(int16(), "[4, 5]")});
}

TEST(ColumnBuilder, MultipleChunksParallel) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeThreaded(GetCpuThreadPool());

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int32Type>({{1, 2}, {3}, {4, 5}, {6, 7}}, &expected);
  CheckFixedType(tg, int32(), {{"1", "2"}, {"3"}, {"4", "5"}, {"6", "7"}}, options,
                 expected);
}

TEST(ColumnBuilder, EmptyChunks) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckFixedType(tg, int16(), {{}, {"1", "2"}, {}}, options,
                 {ArrayFromJSON(int16(), "[]"), ArrayFromJSON(int16(), "[1, 2]"),
                  ArrayFromJSON(int16(), "[]")});
}

//////////////////////////////////////////////////////////////////////////
// Tests for type-inferring column builder

TEST(InferringColumnBuilder, Empty) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {}, options, std::make_shared<ChunkedArray>(ArrayVector(), null()));
}

TEST(InferringColumnBuilder, SingleChunkNull) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "NA"}}, options, {std::make_shared<NullArray>(2)});
}

TEST(InferringColumnBuilder, MultipleChunkNull) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "NA"}, {""}, {"NaN"}}, options,
                {std::make_shared<NullArray>(2), std::make_shared<NullArray>(1),
                 std::make_shared<NullArray>(1)});
}

TEST(InferringColumnBuilder, SingleChunkInteger) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "123", "456"}}, options,
                {ArrayFromJSON(int64(), "[null, 123, 456]")});
}

TEST(InferringColumnBuilder, MultipleChunkInteger) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(
      tg, {{""}, {"NA", "123", "456"}}, options,
      {ArrayFromJSON(int64(), "[null]"), ArrayFromJSON(int64(), "[null, 123, 456]")});
}

TEST(InferringColumnBuilder, SingleChunkBoolean) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "0", "FALSE", "TRUE"}}, options,
                {ArrayFromJSON(boolean(), "[null, false, false, true]")});
}

TEST(InferringColumnBuilder, MultipleChunkBoolean) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{""}, {"1", "True", "0"}}, options,
                {ArrayFromJSON(boolean(), "[null]"),
                 ArrayFromJSON(boolean(), "[true, true, false]")});
}

TEST(InferringColumnBuilder, SingleChunkReal) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{"", "0.0", "12.5"}}, options,
                {ArrayFromJSON(float64(), "[null, 0.0, 12.5]")});
}

TEST(InferringColumnBuilder, MultipleChunkReal) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  CheckInferred(tg, {{""}, {"008"}, {"NaN", "12.5"}}, options,
                {ArrayFromJSON(float64(), "[null]"), ArrayFromJSON(float64(), "[8.0]"),
                 ArrayFromJSON(float64(), "[null, 12.5]")});
}

TEST(InferringColumnBuilder, SingleChunkTimestamp) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(timestamp(TimeUnit::SECOND),
                                        {{false, true, true}}, {{0, 0, 1542129070}},
                                        &expected);
  CheckInferred(tg, {{"", "1970-01-01", "2018-11-13 17:11:10"}}, options, expected);
}

TEST(InferringColumnBuilder, MultipleChunkTimestamp) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<TimestampType>(timestamp(TimeUnit::SECOND),
                                        {{false}, {true}, {true}},
                                        {{0}, {0}, {1542129070}}, &expected);
  CheckInferred(tg, {{""}, {"1970-01-01"}, {"2018-11-13 17:11:10"}}, options, expected);
}

TEST(InferringColumnBuilder, SingleChunkString) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ChunkedArray> expected;

  // With valid UTF8
  CheckInferred(tg, {{"", "foo", "baré"}}, options,
                {ArrayFromJSON(utf8(), R"(["", "foo", "baré"])")});

  // With invalid UTF8, non-checking
  options.check_utf8 = false;
  tg = TaskGroup::MakeSerial();
  ChunkedArrayFromVector<StringType, std::string>({{true, true, true}},
                                                  {{"", "foo\xff", "baré"}}, &expected);
  CheckInferred(tg, {{"", "foo\xff", "baré"}}, options, expected);
}

TEST(InferringColumnBuilder, SingleChunkBinary) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();
  std::shared_ptr<ChunkedArray> expected;

  // With invalid UTF8, checking
  tg = TaskGroup::MakeSerial();
  ChunkedArrayFromVector<BinaryType, std::string>({{true, true, true}},
                                                  {{"", "foo\xff", "baré"}}, &expected);
  CheckInferred(tg, {{"", "foo\xff", "baré"}}, options, expected);
}

TEST(InferringColumnBuilder, MultipleChunkString) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<StringType, std::string>(
      {{true}, {true}, {true, true}}, {{""}, {"008"}, {"NaN", "baré"}}, &expected);

  CheckInferred(tg, {{""}, {"008"}, {"NaN", "baré"}}, options, expected);
}

TEST(InferringColumnBuilder, MultipleChunkBinary) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeSerial();

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<BinaryType, std::string>(
      {{true}, {true}, {true, true}}, {{""}, {"008"}, {"NaN", "baré\xff"}}, &expected);

  CheckInferred(tg, {{""}, {"008"}, {"NaN", "baré\xff"}}, options, expected);
}

// Parallel parsing is tested more comprehensively on the Python side
// (see python/pyarrow/tests/test_csv.py)

TEST(InferringColumnBuilder, MultipleChunkIntegerParallel) {
  auto options = ConvertOptions::Defaults();
  auto tg = TaskGroup::MakeThreaded(GetCpuThreadPool());

  std::shared_ptr<ChunkedArray> expected;
  ChunkedArrayFromVector<Int64Type>({{1, 2}, {3}, {4, 5}, {6, 7}}, &expected);
  CheckInferred(tg, {{"1", "2"}, {"3"}, {"4", "5"}, {"6", "7"}}, options, expected);
}

void CheckAutoDictEncoded(const std::shared_ptr<TaskGroup>& tg, const ChunkData& csv_data,
                          const ConvertOptions& options,
                          std::vector<std::shared_ptr<Array>> expected_indices,
                          std::vector<std::shared_ptr<Array>> expected_dictionaries) {
  std::shared_ptr<ColumnBuilder> builder;
  std::shared_ptr<ChunkedArray> actual;
  ASSERT_OK(ColumnBuilder::Make(default_memory_pool(), 0, options, tg, &builder));
  AssertBuilding(builder, csv_data, &actual);
  ASSERT_EQ(actual->num_chunks(), static_cast<int>(csv_data.size()));
  for (int i = 0; i < actual->num_chunks(); ++i) {
    ASSERT_EQ(actual->chunk(i)->type_id(), Type::DICTIONARY);
    const auto& dict_array = checked_cast<const DictionaryArray&>(*actual->chunk(i));
    AssertArraysEqual(*dict_array.dictionary(), *expected_dictionaries[i]);
    AssertArraysEqual(*dict_array.indices(), *expected_indices[i]);
  }
}

TEST(InferringColumnBuilder, SingleChunkBinaryAutoDict) {
  auto options = ConvertOptions::Defaults();
  options.auto_dict_encode = true;
  options.auto_dict_max_cardinality = 3;

  // With valid UTF8
  auto expected_indices = ArrayFromJSON(int32(), "[0, 1, 0]");
  auto expected_dictionary = ArrayFromJSON(utf8(), R"(["abé", "cd"])");
  ChunkData csv_data = {{"abé", "cd", "abé"}};

  CheckAutoDictEncoded(TaskGroup::MakeSerial(), csv_data, options, {expected_indices},
                       {expected_dictionary});

  // With invalid UTF8, non-checking
  csv_data = {{"ab", "cd\xff", "ab"}};
  options.check_utf8 = false;
  ArrayFromVector<StringType, std::string>({"ab", "cd\xff"}, &expected_dictionary);

  CheckAutoDictEncoded(TaskGroup::MakeSerial(), csv_data, options, {expected_indices},
                       {expected_dictionary});

  // With invalid UTF8, checking
  options.check_utf8 = true;
  ArrayFromVector<BinaryType, std::string>({"ab", "cd\xff"}, &expected_dictionary);

  CheckAutoDictEncoded(TaskGroup::MakeSerial(), csv_data, options, {expected_indices},
                       {expected_dictionary});
}

}  // namespace csv
}  // namespace arrow
