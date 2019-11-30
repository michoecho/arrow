#pragma once
#include <seastar/core/fstream.hh>
#include "arrow/util/memory.h"

namespace parquet::seastarized {

class FutureOutputStream {
 public:
  virtual ~FutureOutputStream() = default;
  virtual seastar::future<> Write(const std::shared_ptr<Buffer>& data) = 0;
  virtual seastar::future<> Write(const void *data, int64_t nbytes) = 0;
  virtual seastar::future<> Flush() = 0;
  virtual seastar::future<> Close() = 0;
  virtual int64_t Tell() = 0;
};

class FileFutureOutputStream : public FutureOutputStream {
  seastar::output_stream<char> sink_;
  int64_t pos = 0;

 public:
  FileFutureOutputStream(seastar::output_stream<char> &&sink)
    : sink_(std::move(sink)) {}

  seastar::future<> Write(const std::shared_ptr<Buffer>& data) override {
    pos += data->size();
    return sink_.write(reinterpret_cast<const char*>(data->data()), data->size());
  }

  seastar::future<> Write(const void *data, int64_t nbytes) override {
    pos += nbytes;
    return sink_.write(reinterpret_cast<const char*>(data), nbytes);
  };

  seastar::future<> Flush() override {
    return sink_.flush();
  };
  seastar::future<> Close() override {
    return sink_.close();
  }

  int64_t Tell() override {
    return pos;
  }
};

class MemoryFutureOutputStream : public FutureOutputStream {
  std::shared_ptr<::arrow::io::BufferOutputStream> sink_;

 public:
  MemoryFutureOutputStream(std::shared_ptr<::arrow::io::BufferOutputStream> sink)
    : sink_(std::move(sink)) {}

  seastar::future<> Write(const std::shared_ptr<Buffer>& data) override {
    PARQUET_THROW_NOT_OK(sink_->Write(data));
    return seastar::make_ready_future<>();
  }

  seastar::future<> Write(const void *data, int64_t nbytes) override {
    PARQUET_THROW_NOT_OK(sink_->Write(data, nbytes));
    return seastar::make_ready_future<>();
  };

  seastar::future<> Flush() override {
    PARQUET_THROW_NOT_OK(sink_->Flush());
    return seastar::make_ready_future<>();
  };
  seastar::future<> Close() override {
    PARQUET_THROW_NOT_OK(sink_->Close());
    return seastar::make_ready_future<>();
  }

  int64_t Tell() override {
    int64_t pos;
    PARQUET_THROW_NOT_OK(sink_->Tell(&pos));
    return pos;
  }

  ::arrow::Status Finish(std::shared_ptr<Buffer>* result) {
    return sink_->Finish(result);
  }
};

} // namespace parquet::seastarized
