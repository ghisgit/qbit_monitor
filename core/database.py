"""
数据库连接管理器
处理SQLite连接池和事务管理
"""

import sqlite3
import time
import threading
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class DatabaseManager:
    """数据库连接管理器 - 线程安全的连接池"""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        """单例模式确保全局只有一个管理器"""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return

        self.logger = logging.getLogger(__name__)
        self.connection_pool = {}
        self.lock = threading.Lock()
        self._initialized = True

    def get_connection(self, db_path: str, timeout: float = 30.0) -> sqlite3.Connection:
        """
        获取数据库连接

        Args:
            db_path: 数据库文件路径
            timeout: 连接超时时间（秒）

        Returns:
            sqlite3.Connection: 数据库连接对象
        """
        thread_id = threading.get_ident()

        with self.lock:
            if thread_id not in self.connection_pool:
                self._create_connection(thread_id, db_path, timeout)

            return self.connection_pool[thread_id]

    def _create_connection(self, thread_id: int, db_path: str, timeout: float):
        """创建新的数据库连接"""
        try:
            # 确保目录存在
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)

            # 创建连接
            conn = sqlite3.connect(db_path, timeout=timeout, check_same_thread=False)

            # 优化设置
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")  # 5秒超时
            conn.execute("PRAGMA foreign_keys=ON")

            self.connection_pool[thread_id] = conn
            self.logger.debug(f"为线程 {thread_id} 创建数据库连接")

        except Exception as e:
            self.logger.error(f"创建数据库连接失败: {e}")
            raise

    @contextmanager
    def transaction(self, db_path: str) -> Iterator[sqlite3.Connection]:
        """
        事务上下文管理器

        Args:
            db_path: 数据库文件路径

        Yields:
            sqlite3.Connection: 数据库连接

        Raises:
            sqlite3.Error: 数据库操作错误
        """
        conn = self.get_connection(db_path)
        try:
            yield conn
            conn.commit()
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                self.logger.warning("数据库锁定，等待重试...")
                time.sleep(0.1)
                conn.rollback()
                # 重试一次
                conn = self.get_connection(db_path)
                yield conn
                conn.commit()
            else:
                conn.rollback()
                raise
        except Exception:
            conn.rollback()
            raise

    def close_all(self):
        """关闭所有数据库连接"""
        with self.lock:
            for thread_id, conn in list(self.connection_pool.items()):
                try:
                    conn.close()
                    self.logger.debug(f"关闭线程 {thread_id} 的数据库连接")
                except Exception as e:
                    self.logger.error(f"关闭连接失败: {e}")

            self.connection_pool.clear()
