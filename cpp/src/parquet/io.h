#pragma once
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
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
  virtual seastar::future<> Advance(int64_t nbytes) = 0;
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
    if (buffer.size() < (uint64_t) nbytes) {
      buffer.resize(nbytes);
    }

    if (buffered_bytes >= nbytes) {
      pos += nbytes;
      memcpy(out, buffer.data(), nbytes);
      memmove(buffer.data(), buffer.data() + nbytes, buffered_bytes - nbytes);
      buffered_bytes -= nbytes;
      return seastar::make_ready_future<int64_t>(nbytes);
    }

    return input_.read_up_to(nbytes - buffered_bytes).then([=](seastar::temporary_buffer<char> buf) {
      int64_t read_bytes = buffered_bytes + buf.size();
      pos += read_bytes;
      memcpy(out, buffer.data(), buffered_bytes);
      memcpy(static_cast<char*>(out) + buffered_bytes, buf.get(), buf.size());
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

  seastar::future<> Advance(int64_t nbytes) override {
    pos += nbytes;
    if (nbytes >= buffered_bytes){
      nbytes -= buffered_bytes;
      buffered_bytes = 0;
      return input_.skip(nbytes);
    } else{
      memmove(buffer.data(), buffer.data() + nbytes, buffered_bytes - nbytes);
      buffered_bytes -= nbytes;
      return seastar::make_ready_future<>();
    }
  }

  int64_t Tell() override {
    return pos;
  }
};

class MemoryFutureInputStream : public FutureInputStream {
 public:
  MemoryFutureInputStream(const std::shared_ptr<Buffer>& buffer, int64_t pos, int64_t size)
    : buffer_(std::move(buffer)), pos_(pos), end_(pos + size) {
      assert(pos_ >= 0);
      assert(end_ >= pos_);
      assert(buffer_->size() >= end_);
    }

  MemoryFutureInputStream(const std::shared_ptr<Buffer>& buffer)
    : buffer_(std::move(buffer)), pos_(0), end_(buffer_->size()) {} 

  seastar::future<int64_t> Read(int64_t nbytes, void *out) override {
    if (nbytes > end_ - pos_) {
      nbytes = end_ - pos_;
    }
    memcpy(out, buffer_->data() + pos_, nbytes);
    pos_ += nbytes;
    return seastar::make_ready_future<int64_t>(nbytes);
  };

  seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out) {
    if (nbytes > end_ - pos_) {
      nbytes = end_ - pos_;
    }

    std::shared_ptr<ResizableBuffer> new_buffer;
    PARQUET_THROW_NOT_OK(::arrow::AllocateResizableBuffer(nbytes, &new_buffer));
    std::memcpy(new_buffer->mutable_data(), buffer_->data() + pos_, static_cast<size_t>(nbytes));
    *out = new_buffer;

    pos_ += nbytes;
    return seastar::make_ready_future<int64_t>(nbytes);
  }

  seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out) {
    if (nbytes > end_ - pos_) {
      nbytes = end_ - pos_;
    }
    *out = std::string_view(reinterpret_cast<const char *>(buffer_->data() + pos_), nbytes);
    return seastar::make_ready_future<int64_t>(nbytes);
  }

  seastar::future<> Close() {
    return seastar::make_ready_future<>();
  };

  seastar::future<> Advance(int64_t nbytes) {
    pos_ += nbytes;
    if (pos_ > end_) {
      pos_ = end_;
    }
    return seastar::make_ready_future<>();
  }

  int64_t Tell() {
    return pos_;
  };

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
  RandomAccessBuffer(std::shared_ptr<Buffer> buffer) : buffer_(buffer) {}

  int64_t GetSize() const override {
    return buffer_->size();
  };

  seastar::future<> Close() override {
    return seastar::make_ready_future<>();
  };

  virtual seastar::future<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) {
    std::shared_ptr<Buffer> retval;
    PARQUET_THROW_NOT_OK(buffer_->Copy(position, nbytes, &retval));
    return seastar::make_ready_future<std::shared_ptr<Buffer>>(std::move(retval));
  }

  virtual std::shared_ptr<FutureInputStream> GetStream(int64_t position, int64_t nbytes) {
    auto stream = std::make_shared<MemoryFutureInputStream>(buffer_, position, nbytes);
    return stream; 
  };

 private:
  std::shared_ptr<Buffer> buffer_;
};

class RandomAccessFile : public RandomAccessSource {
 public:
  static seastar::future<std::shared_ptr<RandomAccessFile>> Open(seastar::file file) {
    return file.size().then([file] (int64_t size) {
      return std::make_shared<RandomAccessFile>(file, size);
    });
  }

  static seastar::future<std::shared_ptr<RandomAccessFile>> Open(std::string path) {
    return seastar::open_file_dma(path, seastar::open_flags::ro).then([] (seastar::file file) {
      return Open(file);
    });
  }

  int64_t GetSize() const override {
    return size_;
  };

  seastar::future<> Close() override {
    return file_.close();
  };

  virtual seastar::future<std::shared_ptr<Buffer>> ReadAt(int64_t position, int64_t nbytes) {
    return file_.dma_read<char>(position, nbytes).then(
      [] (seastar::temporary_buffer<char> read_buf) {
      std::shared_ptr<ResizableBuffer> retval;
      PARQUET_THROW_NOT_OK(::arrow::AllocateResizableBuffer(read_buf.size(), &retval));
      memcpy(retval->mutable_data(), read_buf.get(), read_buf.size());
      return std::static_pointer_cast<Buffer>(retval);
    });
  }

  virtual std::shared_ptr<FutureInputStream> GetStream(int64_t position, int64_t nbytes) {
    return std::make_shared<FileFutureInputStream>(
        seastar::make_file_input_stream(file_, position, nbytes));
  };

  RandomAccessFile(seastar::file file, int64_t size) : file_(file), size_(size) {}

 private:
  seastar::file file_;
  int64_t size_;
};

} // namespace parquet::seastarized
