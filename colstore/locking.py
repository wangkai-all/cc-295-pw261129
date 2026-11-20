"""进程内读写锁 + 跨进程文件锁。

Web 服务是多线程的：浏览（读）与合并（写）要并发跑。
- RWLock 让多个浏览线程同时进入，合并/更正时独占。
- FileLock 用 fcntl 给 manifest 切换加进程级互斥，防止手工开第二个进程
  时出现双重切换。
"""
from __future__ import annotations

import fcntl
import threading
from contextlib import contextmanager


class RWLock:
    def __init__(self) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._readers = 0
        self._writer = False

    def acquire_read(self) -> None:
        with self._cond:
            while self._writer:
                self._cond.wait()
            self._readers += 1

    def release_read(self) -> None:
        with self._cond:
            self._readers -= 1
            if self._readers == 0:
                self._cond.notify_all()

    def acquire_write(self) -> None:
        self._cond.acquire()
        while self._writer or self._readers:
            self._cond.wait()
        self._writer = True

    def release_write(self) -> None:
        self._writer = False
        self._cond.notify_all()
        self._cond.release()

    @contextmanager
    def read_lock(self):
        self.acquire_read()
        try:
            yield
        finally:
            self.release_read()

    @contextmanager
    def write_lock(self):
        self.acquire_write()
        try:
            yield
        finally:
            self.release_write()


class FileLock:
    """排他文件锁，配合 with 使用。"""

    def __init__(self, path: str):
        self.path = path
        self._fh = None

    def __enter__(self) -> "FileLock":
        self._fh = open(self.path, "a+")
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, *exc) -> None:
        fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        self._fh.close()
        self._fh = None
