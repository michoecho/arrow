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

  seastar::future<> Advance(int64_t nbytes) override {
    if (nbytes >= buffered_bytes){
      nbytes -= buffered_bytes;
      buffered_bytes = 0;
      return input_.skip(nbytes);
    } else{
      memmove(buffer.data(), buffer.data() + buffered_bytes, buffered_bytes - nbytes);
      buffered_bytes -= nbytes;
      return seastar::make_ready_future<>();
    }
  }

  int64_t Tell() override {
    return pos;
  }
};

class RandomAccessFile : public FileFutureInputStream {
/// Modelled after ArrowInputFile
  public:
    /// Necessary because we hold a std::unique_ptr
    ~RandomAccessFile() override {};

    /// \brief Create an isolated InputStream that reads a segment of a
    /// RandomAccessFile. Multiple such stream can be created and used
    /// independently without interference
    /// \param[in] file a file instance
    /// \param[in] file_offset the starting position in the file
    /// \param[in] nbytes the extent of bytes to read. The file should have
    /// sufficient bytes available
    static std::shared_ptr<FutureInputStream> GetStream(std::shared_ptr<RandomAccessFile> file,
                                                  int64_t file_offset, int64_t nbytes) {
      //TODO jacek42
      return nullptr;
    };

    virtual void GetSize(int64_t* size) {
      // TODO jacek42
      // TODO do not make it a future, just throw an exception
    }

    /// \brief Read nbytes at position, provide default implementations using
    /// Read(...), but can be overridden. The default implementation is
    /// thread-safe. It is unspecified whether this method updates the file
    /// position or not.
    ///
    /// \param[in] position Where to read bytes from
    /// \param[in] nbytes The number of bytes to read
    /// \param[out] bytes_read The number of bytes read
    /// \param[out] out The buffer to read bytes into
    /// \return Status
    virtual seastar::future<> ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
      //TODO jacek42
      return seastar::make_ready_future();
    };

    /// \brief Read nbytes at position, provide default implementations using
    /// Read(...), but can be overridden. The default implementation is
    /// thread-safe. It is unspecified whether this method updates the file
    /// position or not.
    ///
    /// \param[in] position Where to read bytes from
    /// \param[in] nbytes The number of bytes to read
    /// \param[out] out The buffer to read bytes into. The number of bytes read can be
    /// retrieved by calling Buffer::size().
    virtual seastar::future<> ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
      //TODO jacek42
      return seastar::make_ready_future();
    };

    /// Because of interfaces.h/Seekable
    virtual seastar::future<> Seek(int64_t position) {
      //TODO jacek42
      return seastar::make_ready_future();
    }

    // Because of Memory/BufferReader
//    explicit RandomAccessFile(const std::shared_ptr<Buffer>& buffer);
//    explicit RandomAccessFile(const Buffer& buffer);
//    RandomAccessFile(const uint8_t* data, int64_t size);

    protected:
      RandomAccessFile();
};

} // namespace parquet::seastarized
