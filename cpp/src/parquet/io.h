#pragma once
#include <seastar/core/fstream.hh>
#include <vector>
#include <string_view>
#include <arrow/io/api.h>
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

class RandomAccessFile : public FutureInputStream {

public:
  virtual ~RandomAccessFile() = default;
  virtual void GetSize(int64_t *size) = 0;
  virtual seastar::future<int64_t> Peek(int64_t nbytes, ::arrow::util::string_view *out) = 0;
  virtual seastar::future<> Seek(int64_t pos) = 0;
  virtual seastar::future<> ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) = 0;
  virtual seastar::future<> ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) = 0;
//protected:
//  RandomAccessFile();
};

class BufferRandomAccessFile : public RandomAccessFile {

private:
  std::shared_ptr<::arrow::io::BufferReader> br_;

/// Modelled after ArrowInputFile
public:
//  Necessary because we hold a std::unique_ptr
  virtual ~BufferRandomAccessFile() override = default;

  virtual void GetSize(int64_t* size) {
    // TODO jacek42
    // TODO do not make it a future, just throw an exception if the file is not open
    br_->GetSize(size);
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
    br_->ReadAt(position, nbytes, bytes_read, out);
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
    br_->ReadAt(position, nbytes, out);
    return seastar::make_ready_future();
  };

//    Because of Memory/BufferReader
  explicit BufferRandomAccessFile(const std::shared_ptr<Buffer>& buffer) {
    br_ = std::make_shared<::arrow::io::BufferReader>(buffer);
  };

  seastar::future<int64_t> Read(int64_t nbytes, void *out) {
    int64_t read = 0;
    br_->Read(nbytes, &read, out);
    return seastar::make_ready_future<int64_t>(read);
  }

  seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out) {
    int64_t read = 0;
    br_->Read(nbytes, &read, out);
    return seastar::make_ready_future<int64_t>(read);
  }

  seastar::future<int64_t> Peek(int64_t nbytes, ::arrow::util::string_view *out) {
    br_->Peek(nbytes, out);
    return seastar::make_ready_future<int64_t>(0);
  }

  seastar::future<> Advance(int64_t nbytes) override {
    br_->Advance(nbytes);
    return seastar::make_ready_future<>();
  }

  int64_t Tell() override {
    int64_t pos = 0;
    br_->Tell(&pos);
    return pos;
  }

  seastar::future<> Seek(int64_t pos) {
    br_->Seek(pos);
    return seastar::make_ready_future();
  }

  seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out) override {
    throw "Not implemented";
    // Peek has this other signature
//    ::arrow::util::string_view out_sv = static_cast<::arrow::util::string_view>(out);
//    return Peek(nbytes, out_sv);
  }

  seastar::future<> Close() override {
    return seastar::make_ready_future();
  }
};

class ReadableRandomAccessFile : public RandomAccessFile {

private:
  std::shared_ptr<::arrow::io::ReadableFile> rf_;

/// Modelled after ArrowInputFile
public:
///  Necessary because we hold a std::unique_ptr
  virtual ~ReadableRandomAccessFile() override = default;

  virtual void GetSize(int64_t* size) {
    // TODO jacek42
    // TODO do not make it a future, just throw an exception if the file is not open
    rf_->GetSize(size);
  }

  virtual seastar::future<> ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    //TODO jacek42
    rf_->ReadAt(position, nbytes, bytes_read, out);
    return seastar::make_ready_future();
  };

  virtual seastar::future<> ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
    //TODO jacek42
    rf_->ReadAt(position, nbytes, out);
    return seastar::make_ready_future();
  };

//    Because of Memory/BufferReader
  explicit ReadableRandomAccessFile(std::shared_ptr<::arrow::io::ReadableFile> rf) : rf_(rf) {};

  seastar::future<int64_t> Read(int64_t nbytes, void *out) {
    int64_t read = 0;
    rf_->Read(nbytes, &read, out);
    return seastar::make_ready_future<int64_t>(read);
  }

  seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out) {
    int64_t read = 0;
    rf_->Read(nbytes, &read, out);
    return seastar::make_ready_future<int64_t>(read);
  }

  seastar::future<int64_t> Peek(int64_t nbytes, ::arrow::util::string_view *out) {
    rf_->Peek(nbytes, out);
    return seastar::make_ready_future<int64_t>(0);
  }

  seastar::future<> Advance(int64_t nbytes) override {
    rf_->Advance(nbytes);
    return seastar::make_ready_future<>();
  }

  int64_t Tell() override {
    int64_t pos = 0;
    rf_->Tell(&pos);
    return pos;
  }

  seastar::future<> Seek(int64_t pos) {
    rf_->Seek(pos);
    return seastar::make_ready_future();
  }

  seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out) override {
    throw "Not implemented";
    // Peek has this other signature
//    ::arrow::util::string_view out_sv = static_cast<::arrow::util::string_view>(out);
//    return Peek(nbytes, out_sv);
  }

  seastar::future<> Close() override {
    return seastar::make_ready_future();
  }
};

class MemoryMappedRandomAccessFile : public RandomAccessFile {

private:
  std::shared_ptr<::arrow::io::MemoryMappedFile> mf_;

/// Modelled after ArrowInputFile
public:
///  Necessary because we hold a std::unique_ptr
  virtual ~MemoryMappedRandomAccessFile() override = default;

  virtual void GetSize(int64_t* size) {
    // TODO jacek42
    // TODO do not make it a future, just throw an exception if the file is not open
    mf_->GetSize(size);
  }

  virtual seastar::future<> ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    //TODO jacek42
    mf_->ReadAt(position, nbytes, bytes_read, out);
    return seastar::make_ready_future();
  };

  virtual seastar::future<> ReadAt(int64_t position, int64_t nbytes, std::shared_ptr<Buffer>* out) {
    //TODO jacek42
    mf_->ReadAt(position, nbytes, out);
    return seastar::make_ready_future();
  };

//    Because of Memory/BufferReader
  explicit MemoryMappedRandomAccessFile(std::shared_ptr<::arrow::io::MemoryMappedFile> mf) : mf_(mf) {};

  seastar::future<int64_t> Read(int64_t nbytes, void *out) {
    int64_t read = 0;
    mf_->Read(nbytes, &read, out);
    return seastar::make_ready_future<int64_t>(read);
  }

  seastar::future<int64_t> Read(int64_t nbytes, std::shared_ptr<Buffer> *out) {
    int64_t read = 0;
    mf_->Read(nbytes, &read, out);
    return seastar::make_ready_future<int64_t>(read);
  }

  seastar::future<int64_t> Peek(int64_t nbytes, ::arrow::util::string_view *out) {
    mf_->Peek(nbytes, out);
    return seastar::make_ready_future<int64_t>(0);
  }

  seastar::future<> Advance(int64_t nbytes) override {
    mf_->Advance(nbytes);
    return seastar::make_ready_future<>();
  }

  int64_t Tell() override {
    int64_t pos = 0;
    mf_->Tell(&pos);
    return pos;
  }

  seastar::future<> Seek(int64_t pos) {
    mf_->Seek(pos);
    return seastar::make_ready_future();
  }

  seastar::future<int64_t> Peek(int64_t nbytes, std::string_view *out) override {
    throw "Not implemented";
    // Peek has this other signature
//    ::arrow::util::string_view out_sv = static_cast<::arrow::util::string_view>(out);
//    return Peek(nbytes, out_sv);
  }

  seastar::future<> Close() override {
    return seastar::make_ready_future();
  }
};



} // namespace parquet::seastarized
