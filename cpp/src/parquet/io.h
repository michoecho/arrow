#pragma once
#include <seastar/core/fstream.hh>
#include <vector>
#include <string_view>
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

class FutureInputStream {
 public:
  virtual ~FutureInputStream() = default;
  virtual seastar::future<int64_t> Read(int64_t nbytes, void *out) = 0;
  virtual seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out) = 0;
  virtual seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out) = 0;
  virtual seastar::future<> Close() = 0;
  virtual int64_t Tell() = 0;
};

//todo: avoid copying
class FileFutureInputStream : public FutureInputStream {
 private:
  seastar::input_stream<char> input_;
  std::vector<char> buffer;
  int64_t pos = 0;
  int64_t buffered_bytes = 0;

 public:
  FileFutureInputStream(seastar::input_stream<char> &&input)
      : input_(std::move(input)) {}

  seastar::future<int64_t> Read(int64_t nbytes, void *out) {
    if (buffer.size() < (unit64_t) nbytes) {
      buffer.resize(nbytes);
    }

    if (buffered_bytes >= nbytes) {
      pos += nbytes;
      memcpy(out, buffer.data(), nbytes);
      memmove(buffer.data(), buffer.data() + buffered_bytes, buffered_bytes - nbytes);
      buffered_bytes -= nbytes;
      return seastar::make_ready_future<int64_t>(nbytes);
    }

    return input_.read_up_to(nbytes - buffered_bytes).then([=](seastar::temporary_buffer<char> buf) {
      memcpy(buffer.data() + buffered_bytes, buf.get(), buf.size());
      int64_t read_bytes = buffered_bytes + buf.size();
      memcpy(out, buffer.data(), read_bytes);
      pos += read_bytes;
      buffered_bytes = 0;
      return seastar::make_ready_future<int64_t>(read_bytes);
    });
  }

  seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out) {
    PARQUET_THROW_NOT_OK(::arrow::AllocateBuffer(nbytes, out));
    return Read(nbytes, out->get()->mutable_data());
  }

  seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out) {
    if (buffer.size() < (uint64_t) nbytes) {
      buffer.resize(nbytes);
    }

    if (buffered_bytes >= nbytes) {
      *out = std::string_view(buffer.data(), nbytes);
      return seastar::make_ready_future<int64_t>(nbytes);
    }

    return input_.read_up_to(nbytes - buffered_bytes).then([=](seastar::temporary_buffer<char> buf) {
      memcpy(buffer.data() + buffered_bytes, buf.get(), buf.size());
      buffered_bytes += buf.size();
      *out = std::string_view(buffer.data(), buffered_bytes);
      return seastar::make_ready_future<int64_t>(buffered_bytes);
    });
  }

  seastar::future<> Close() override {
    return input_.close();
  }

  int64_t Tell() override {
    return pos;
  }
};

} // namespace parquet::seastarized
