#pragma once
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <vector>
#include <string_view>
#include "parquet/platform.h"
#include "parquet/exception.h"

namespace parquet::seastarized {

class FutureOutputStream {
 public:
  virtual ~FutureOutputStream() = default;
  virtual seastar::future<> Write(std::shared_ptr<Buffer> data) = 0;
  virtual seastar::future<> Write(const void *data, int64_t nbytes) = 0;
  virtual seastar::future<> Flush() = 0;
  virtual seastar::future<> Close() = 0;
  virtual int64_t Tell() const = 0;
};

class FileFutureOutputStream : public FutureOutputStream {
 public:
  FileFutureOutputStream(seastar::output_stream<char> &&sink);
  seastar::future<> Write(std::shared_ptr<Buffer> data) override;
  seastar::future<> Write(const void *data, int64_t nbytes) override;
  seastar::future<> Flush() override;
  seastar::future<> Close() override;
  int64_t Tell() const override;

 private:
  seastar::output_stream<char> sink_;
  int64_t pos_ = 0;
};

class MemoryFutureOutputStream : public FutureOutputStream {
 public:
  MemoryFutureOutputStream(const std::shared_ptr<::arrow::io::BufferOutputStream>& sink);
  seastar::future<> Write(std::shared_ptr<Buffer> data) override;
  seastar::future<> Write(const void *data, int64_t nbytes) override;
  seastar::future<> Flush() override;
  seastar::future<> Close() override;
  int64_t Tell() const override;
  ::arrow::Status Finish(std::shared_ptr<Buffer>* result);

 private:
  std::shared_ptr<::arrow::io::BufferOutputStream> sink_;
};

class FutureInputStream {
 public:
  virtual ~FutureInputStream() = default;
  virtual seastar::future<int64_t> Read(int64_t nbytes, void *out) = 0;
  virtual seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out) = 0;
  virtual seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out) = 0;
  virtual seastar::future<> Close() = 0;
  virtual seastar::future<> Advance(int64_t nbytes) = 0;
  virtual int64_t Tell() const = 0;
};

//todo: avoid copying
class FileFutureInputStream : public FutureInputStream {
 public:
  FileFutureInputStream(seastar::input_stream<char> &&input);
  seastar::future<int64_t> Read(int64_t nbytes, void *out);
  seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out);
  seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out);
  seastar::future<> Close();
  seastar::future<> Advance(int64_t nbytes);
  int64_t Tell() const;

 private:
  seastar::input_stream<char> input_;
  std::vector<char> buffer;
  int64_t pos_ = 0;
  int64_t buffered_bytes = 0;
};

class MemoryFutureInputStream : public FutureInputStream {
 public:
  MemoryFutureInputStream(const std::shared_ptr<Buffer>& buffer, int64_t pos, int64_t size);
  MemoryFutureInputStream(const std::shared_ptr<Buffer>& buffer);
  seastar::future<int64_t> Read(int64_t nbytes, void *out) override;
  seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out) override;
  seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out) override;
  seastar::future<> Close() override;
  seastar::future<> Advance(int64_t nbytes) override;
  int64_t Tell() const override;

 private:
  std::shared_ptr<Buffer> buffer_;
  int64_t pos_;
  int64_t end_;
};

class RandomAccessSource {
 public:
  virtual ~RandomAccessSource() = default;
  virtual int64_t GetSize() const = 0;
  virtual seastar::future<> Close() = 0;
  virtual seastar::future<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) = 0;
  virtual std::shared_ptr<FutureInputStream> GetStream(int64_t position, int64_t nbytes) = 0;
};

class RandomAccessBuffer : public RandomAccessSource {
 public:
  RandomAccessBuffer(std::shared_ptr<Buffer> buffer);
  int64_t GetSize() const override;
  seastar::future<> Close() override;
  seastar::future<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
  std::shared_ptr<FutureInputStream> GetStream(int64_t position, int64_t nbytes) override;

 private:
  std::shared_ptr<Buffer> buffer_;
};

class RandomAccessFile : public RandomAccessSource {
 public:
  RandomAccessFile(seastar::file file, int64_t size);
  static seastar::future<std::shared_ptr<RandomAccessFile>> Open(seastar::file file);
  static seastar::future<std::shared_ptr<RandomAccessFile>> Open(std::string path);
  int64_t GetSize() const override;
  seastar::future<> Close() override;
  seastar::future<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) override;
  std::shared_ptr<FutureInputStream> GetStream(int64_t position, int64_t nbytes) override;

 private:
  seastar::file file_;
  int64_t size_;
};

} // namespace parquet::seastarized
