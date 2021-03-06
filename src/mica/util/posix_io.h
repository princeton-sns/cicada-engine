#pragma once
#ifndef MICA_UTIL_POSIX_IO_H_
#define MICA_UTIL_POSIX_IO_H_

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

// Mostly copied from Peloton codebase
namespace mica {
namespace util {
class PosixIO {
 public:
  PosixIO() = delete;

  template <class... Args>
  static int Open(const char* path, int oflag, Args... args) {
    while (true) {
      int ret = open(path, oflag, args...);
      if (ret == -1) {
        if (errno == EINTR) continue;
        throw std::runtime_error("Failed to open file with errno " +
                                 std::to_string(errno) + ": " +
                                 std::strerror(errno));
      }
      return ret;
    }
  }

  static long LSeek(int fd, long offset, int whence) {
    while (true) {
      long ret = lseek(fd, offset, whence);
      if (ret == -1) {
        if (errno == EINTR) continue;
        throw std::runtime_error("Failed to seek file with errno " +
                                 std::to_string(errno) + ": " +
                                 std::strerror(errno));
      }
      return ret;
    }
  }

  static std::size_t Size(const char* path) {
    struct stat st;
    int ret = stat(path, &st);
    if (ret == -1) {
      throw std::runtime_error("Failed to get file size with errno " +
                               std::to_string(errno) + ": " +
                               std::strerror(errno));
    }
    return static_cast<std::size_t>(st.st_size);
  }

  static void FSync(int fd) {
    while (true) {
      int ret = fsync(fd);
      if (ret == -1) {
        if (errno == EINTR) continue;
        throw std::runtime_error("Failed to fsync file with errno " +
                                 std::to_string(errno) + ": " +
                                 std::strerror(errno));
      }
      return;
    }
  }

  static void Close(int fd) {
    while (true) {
      int ret = close(fd);
      if (ret == -1) {
        if (errno == EINTR) continue;
        throw std::runtime_error("Failed to close file with errno " +
                                 std::to_string(errno) + ": " +
                                 std::strerror(errno));
      }
      return;
    }
  }

  static uint32_t Read(int fd, void* buf, size_t nbyte) {
    size_t bytes_read = 0;
    while (bytes_read < nbyte) {
      ssize_t ret = read(fd, reinterpret_cast<char*>(buf) + bytes_read,
                         nbyte - bytes_read);
      if (ret == -1) {
        if (errno == EINTR) continue;
        throw std::runtime_error("Read failed with errno " +
                                 std::to_string(errno) + ": " +
                                 std::strerror(errno));
      }
      if (ret == 0) break;  // no more bytes left in the file
      bytes_read += static_cast<size_t>(ret);
    }
    return static_cast<uint32_t>(bytes_read);
  }

  static void Write(int fd, const void* buf, size_t nbyte) {
    size_t written = 0;
    while (written < nbyte) {
      ssize_t ret = write(fd, reinterpret_cast<const char*>(buf) + written,
                          nbyte - written);
      if (ret == -1) {
        if (errno == EINTR) continue;
        throw std::runtime_error("Write to log file failed with errno " +
                                 std::to_string(errno) + ": " +
                                 std::strerror(errno));
      }
      written += static_cast<size_t>(ret);
    }
  }

  static off_t PRead(int fd, void* buf, size_t nbyte, off_t offset) {
    off_t ret = pread(fd, buf, nbyte, offset);
    if (ret == -1) {
      throw std::runtime_error("PRead failed with errno " +
                               std::to_string(errno) + ": " +
                               std::strerror(errno));
    }

    return ret;
  }

  static void Ftruncate(int fd, off_t length) {
    while (true) {
      int ret = ftruncate(fd, length);
      if (ret == -1) {
        if (errno == EINTR) continue;
        throw std::runtime_error("ftruncate failed with errno " +
                                 std::to_string(errno) + ": " +
                                 std::strerror(errno));
      }

      return;
    }
  }

  static void* Mmap(void* addr, std::size_t len, int prot, int flags, int fd,
                    off_t off) {
    void* ptr = mmap(addr, len, prot, flags, fd, off);
    if (ptr == MAP_FAILED) {
      throw std::runtime_error("mmap failed with errno " +
                               std::to_string(errno) + ": " +
                               std::strerror(errno));
    }

    return ptr;
  }

  static void Munmap(void* addr, std::size_t len) {
    int ret = munmap(addr, len);
    if (ret == -1) {
      throw std::runtime_error("munmap failed with errno " +
                               std::to_string(errno) + ": " +
                               std::strerror(errno));
    }
  }

  static void Msync(void* addr, std::size_t len, int flags) {
    int ret = msync(addr, len, flags);
    if (ret == -1) {
      throw std::runtime_error("msync failed with errno " +
                               std::to_string(errno) + ": " +
                               std::strerror(errno));
    }
  }

  static bool Exists(const char* path) {
    int ret = access(path, F_OK);
    return ret == 0;
  }
};

}  // namespace util
}  // namespace mica

#endif
