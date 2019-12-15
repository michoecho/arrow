#include "parquet/io.h"

namespace parquet::seastarized {

// FileFutureOutputStream
FileFutureOutputStream::FileFutureOutputStream(seastar::output_stream<char> &&sink)
  : sink_(std::move(sink)) {}

seastar::future<> FileFutureOutputStream::Write(std::shared_ptr<Buffer> data) {
  pos_ += data->size();
  return sink_.write(reinterpret_cast<const char*>(data->data()), data->size());
}

seastar::future<> FileFutureOutputStream::Write(const void *data, int64_t nbytes) {
  pos_ += nbytes;
  return sink_.write(reinterpret_cast<const char*>(data), nbytes);
};

seastar::future<> FileFutureOutputStream::Flush() {
  return sink_.flush();
};

seastar::future<> FileFutureOutputStream::Close() {
  return sink_.close();
}

int64_t FileFutureOutputStream::Tell() const {
  return pos_;
}

// MemoryFutureOutputStream
MemoryFutureOutputStream::MemoryFutureOutputStream(
    const std::shared_ptr<::arrow::io::BufferOutputStream>& sink)
  : sink_(sink) {}

seastar::future<> MemoryFutureOutputStream::Write(std::shared_ptr<Buffer> data) {
  PARQUET_THROW_NOT_OK(sink_->Write(data));
  return seastar::make_ready_future<>();
}

seastar::future<> MemoryFutureOutputStream::Write(const void *data, int64_t nbytes) {
  PARQUET_THROW_NOT_OK(sink_->Write(data, nbytes));
  return seastar::make_ready_future<>();
};

seastar::future<> MemoryFutureOutputStream::Flush() {
  PARQUET_THROW_NOT_OK(sink_->Flush());
  return seastar::make_ready_future<>();
};

seastar::future<> MemoryFutureOutputStream::Close() {
  PARQUET_THROW_NOT_OK(sink_->Close());
  return seastar::make_ready_future<>();
}

int64_t MemoryFutureOutputStream::Tell() const {
  int64_t pos;
  PARQUET_THROW_NOT_OK(sink_->Tell(&pos));
  return pos;
}

::arrow::Status MemoryFutureOutputStream::Finish(std::shared_ptr<Buffer>* result) {
  return sink_->Finish(result);
}

FileFutureInputStream::FileFutureInputStream(seastar::input_stream<char> &&input)
    : input_(std::move(input)) {}

seastar::future<int64_t> FileFutureInputStream::Read(int64_t nbytes, void *out) {
  if (buffer.size() < (uint64_t) nbytes) {
    buffer.resize(nbytes);
  }

  if (buffered_bytes >= nbytes) {
    pos_ += nbytes;
    memcpy(out, buffer.data(), nbytes);
    memmove(buffer.data(), buffer.data() + nbytes, buffered_bytes - nbytes);
    buffered_bytes -= nbytes;
    return seastar::make_ready_future<int64_t>(nbytes);
  }

  return input_.read_up_to(nbytes - buffered_bytes).then(
  [=] (seastar::temporary_buffer<char> buf) {
    int64_t read_bytes = buffered_bytes + buf.size();
    pos_ += read_bytes;
    memcpy(out, buffer.data(), buffered_bytes);
    memcpy(static_cast<char*>(out) + buffered_bytes, buf.get(), buf.size());
    buffered_bytes = 0;
    return seastar::make_ready_future<int64_t>(read_bytes);
  });
}

seastar::future<int64_t>
FileFutureInputStream::Read(int64_t nbytes, std::shared_ptr<Buffer> *out) {
  PARQUET_THROW_NOT_OK(::arrow::AllocateBuffer(nbytes, out));
  return Read(nbytes, out->get()->mutable_data());
}

seastar::future<int64_t>
FileFutureInputStream::Peek(int64_t nbytes, std::string_view *out) {
  if (buffer.size() < (uint64_t) nbytes) {
    buffer.resize(nbytes);
  }

  if (buffered_bytes >= nbytes) {
    *out = std::string_view(buffer.data(), nbytes);
    return seastar::make_ready_future<int64_t>(nbytes);
  }

  return input_.read_up_to(nbytes - buffered_bytes).then(
  [=](seastar::temporary_buffer<char> buf) {
    memcpy(buffer.data() + buffered_bytes, buf.get(), buf.size());
    buffered_bytes += buf.size();
    *out = std::string_view(buffer.data(), buffered_bytes);
    return seastar::make_ready_future<int64_t>(buffered_bytes);
  });
}

seastar::future<> FileFutureInputStream::Close() {
  return input_.close();
}

seastar::future<> FileFutureInputStream::Advance(int64_t nbytes) {
  pos_ += nbytes;
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

int64_t FileFutureInputStream::Tell() const {
  return pos_;
}

MemoryFutureInputStream::MemoryFutureInputStream(
    const std::shared_ptr<Buffer>& buffer, int64_t pos, int64_t size)
  : buffer_(buffer), pos_(pos), end_(pos + size) {
    assert(pos_ >= 0);
    assert(end_ >= pos_);
    assert(buffer_->size() >= end_);
  }

MemoryFutureInputStream::MemoryFutureInputStream(const std::shared_ptr<Buffer>& buffer)
  : buffer_(buffer), pos_(0), end_(buffer_->size()) {} 

seastar::future<int64_t> MemoryFutureInputStream::Read(int64_t nbytes, void *out) {
  if (nbytes > end_ - pos_) {
    nbytes = end_ - pos_;
  }
  memcpy(out, buffer_->data() + pos_, nbytes);
  pos_ += nbytes;
  return seastar::make_ready_future<int64_t>(nbytes);
};

seastar::future<int64_t>
MemoryFutureInputStream::Read(int64_t nbytes, std::shared_ptr<Buffer> *out) {
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

seastar::future<int64_t>
MemoryFutureInputStream::Peek(int64_t nbytes, std::string_view *out) {
  if (nbytes > end_ - pos_) {
    nbytes = end_ - pos_;
  }
  *out = std::string_view(reinterpret_cast<const char *>(buffer_->data() + pos_), nbytes);
  return seastar::make_ready_future<int64_t>(nbytes);
}

seastar::future<> MemoryFutureInputStream::Close() {
  return seastar::make_ready_future<>();
};

seastar::future<> MemoryFutureInputStream::Advance(int64_t nbytes) {
  pos_ += nbytes;
  if (pos_ > end_) {
    pos_ = end_;
  }
  return seastar::make_ready_future<>();
}

int64_t MemoryFutureInputStream::Tell() const {
  return pos_;
};

RandomAccessBuffer::RandomAccessBuffer(std::shared_ptr<Buffer> buffer)
  : buffer_(buffer) {}

int64_t RandomAccessBuffer::GetSize() const {
  return buffer_->size();
};

seastar::future<> RandomAccessBuffer::Close() {
  return seastar::make_ready_future<>();
};

seastar::future<std::shared_ptr<Buffer>>
RandomAccessBuffer::ReadAt(int64_t position, int64_t nbytes) {
  std::shared_ptr<Buffer> retval;
  PARQUET_THROW_NOT_OK(buffer_->Copy(position, nbytes, &retval));
  return seastar::make_ready_future<std::shared_ptr<Buffer>>(std::move(retval));
}

std::shared_ptr<FutureInputStream>
RandomAccessBuffer::GetStream(int64_t position, int64_t nbytes) {
  auto stream = std::make_shared<MemoryFutureInputStream>(buffer_, position, nbytes);
  return stream; 
};

seastar::future<std::shared_ptr<RandomAccessFile>>
RandomAccessFile::Open(seastar::file file) {
  return file.size().then([file] (int64_t size) {
    return std::make_shared<RandomAccessFile>(file, size);
  });
}

seastar::future<std::shared_ptr<RandomAccessFile>>
RandomAccessFile::Open(std::string path) {
  return seastar::open_file_dma(path, seastar::open_flags::ro).then([] (seastar::file file) {
    return Open(file);
  });
}

int64_t RandomAccessFile::GetSize() const {
  return size_;
};

seastar::future<> RandomAccessFile::Close() {
  return file_.close();
};

seastar::future<std::shared_ptr<Buffer>>
RandomAccessFile::ReadAt(int64_t position, int64_t nbytes) {
  return file_.dma_read<char>(position, nbytes).then(
    [] (seastar::temporary_buffer<char> read_buf) {
    std::shared_ptr<ResizableBuffer> retval;
    PARQUET_THROW_NOT_OK(::arrow::AllocateResizableBuffer(read_buf.size(), &retval));
    memcpy(retval->mutable_data(), read_buf.get(), read_buf.size());
    return std::static_pointer_cast<Buffer>(retval);
  });
}

std::shared_ptr<FutureInputStream>
RandomAccessFile::GetStream(int64_t position, int64_t nbytes) {
  return std::make_shared<FileFutureInputStream>(
      seastar::make_file_input_stream(file_, position, nbytes));
};

RandomAccessFile::RandomAccessFile(seastar::file file, int64_t size)
  : file_(file), size_(size) {}

} // namespace parquet::seastarized
