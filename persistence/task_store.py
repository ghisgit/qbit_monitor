import sqlite3
import logging
import threading
from pathlib import Path
from typing import List, Tuple, Dict, Any, Optional
import time
from queue import Queue, Empty
import json


class DatabaseManager:
    """数据库管理线程"""

    def __init__(self, db_path: str):
        self.db_path = Path(db_path)
        self.logger = logging.getLogger(__name__)

        # 命令队列
        self.command_queue = Queue()
        self.result_queues: Dict[str, Queue] = {}
        self.result_lock = threading.Lock()

        # 线程控制
        self.running = True
        self.worker_thread = None

        # 初始化数据库
        self._init_database()
        self._start_worker()

    def _init_database(self):
        """初始化数据库结构"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            cursor = conn.cursor()

            # 主任务表
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    torrent_hash TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    hash_file_path TEXT NOT NULL,
                    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(torrent_hash, task_type)
                )
            """
            )

            # 重试任务表
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS retry_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    torrent_hash TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    hash_file_path TEXT NOT NULL,
                    retry_time REAL NOT NULL,
                    retry_count INTEGER DEFAULT 0,
                    created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(torrent_hash, task_type)
                )
            """
            )

            # 创建索引
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_hash_type ON tasks(torrent_hash, task_type)"
            )
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_type ON tasks(task_type)")
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_retry_time ON retry_tasks(retry_time)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_retry_hash_type ON retry_tasks(torrent_hash, task_type)"
            )

            conn.commit()
            self.logger.info("数据库初始化完成")

        finally:
            conn.close()

    def _start_worker(self):
        """启动数据库工作线程"""
        self.worker_thread = threading.Thread(
            target=self._database_worker, name="database_worker", daemon=False
        )
        self.worker_thread.start()
        self.logger.info("数据库管理线程已启动")

    def _database_worker(self):
        """数据库工作线程主循环"""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA cache_size=-64000")

        self.logger.debug("数据库工作线程开始运行")

        while self.running:
            try:
                # 获取命令，超时1秒以便检查运行状态
                command = self.command_queue.get(timeout=1.0)
                command_type, command_id, method, args, kwargs = command

                try:
                    # 执行数据库操作
                    result = self._execute_method(conn, method, *args, **kwargs)

                    # 发送结果
                    with self.result_lock:
                        if command_id in self.result_queues:
                            self.result_queues[command_id].put(("success", result))

                except Exception as e:
                    self.logger.error(f"数据库操作失败 {method}: {e}")

                    # 发送错误
                    with self.result_lock:
                        if command_id in self.result_queues:
                            self.result_queues[command_id].put(("error", str(e)))

                finally:
                    self.command_queue.task_done()

            except Empty:
                # 超时，继续循环检查运行状态
                continue
            except Exception as e:
                self.logger.error(f"数据库工作线程错误: {e}")
                time.sleep(0.1)

        # 清理
        conn.close()
        self.logger.debug("数据库工作线程停止")

    def _execute_method(self, conn, method: str, *args, **kwargs):
        """执行具体的数据库方法"""
        if method == "save_task":
            return self._save_task(conn, *args, **kwargs)
        elif method == "save_retry_task":
            return self._save_retry_task(conn, *args, **kwargs)
        elif method == "get_pending_retry_tasks":
            return self._get_pending_retry_tasks(conn, *args, **kwargs)
        elif method == "delete_retry_task":
            return self._delete_retry_task(conn, *args, **kwargs)
        elif method == "get_pending_tasks":
            return self._get_pending_tasks(conn, *args, **kwargs)
        elif method == "task_exists":
            return self._task_exists(conn, *args, **kwargs)
        elif method == "delete_task":
            return self._delete_task(conn, *args, **kwargs)
        elif method == "get_retry_count":
            return self._get_retry_count(conn, *args, **kwargs)
        elif method == "cleanup_orphaned_tasks":
            return self._cleanup_orphaned_tasks(conn, *args, **kwargs)
        else:
            raise ValueError(f"未知的数据库方法: {method}")

    def _execute_query(self, conn, query: str, params: tuple = ()):
        """执行查询并返回结果"""
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    def _execute_update(self, conn, query: str, params: tuple = ()):
        """执行更新操作"""
        cursor = conn.cursor()
        cursor.execute(query, params)
        conn.commit()
        return cursor.rowcount

    # 具体的数据库操作方法
    def _save_task(self, conn, torrent_hash: str, task_type: str, hash_file_path: str):
        self._execute_update(
            conn,
            "INSERT OR REPLACE INTO tasks (torrent_hash, task_type, hash_file_path) VALUES (?, ?, ?)",
            (torrent_hash, task_type, hash_file_path),
        )

    def _save_retry_task(
        self,
        conn,
        torrent_hash: str,
        task_type: str,
        hash_file_path: str,
        retry_time: float,
        retry_count: int,
    ):
        self._execute_update(
            conn,
            "INSERT OR REPLACE INTO retry_tasks (torrent_hash, task_type, hash_file_path, retry_time, retry_count) VALUES (?, ?, ?, ?, ?)",
            (torrent_hash, task_type, hash_file_path, retry_time, retry_count),
        )

    def _get_pending_retry_tasks(self, conn):
        return self._execute_query(
            conn,
            "SELECT torrent_hash, task_type, hash_file_path, retry_time, retry_count FROM retry_tasks ORDER BY retry_time",
        )

    def _delete_retry_task(self, conn, torrent_hash: str, task_type: str):
        return self._execute_update(
            conn,
            "DELETE FROM retry_tasks WHERE torrent_hash = ? AND task_type = ?",
            (torrent_hash, task_type),
        )

    def _get_pending_tasks(self, conn, task_type: str = None):
        if task_type:
            return self._execute_query(
                conn,
                "SELECT torrent_hash, task_type, hash_file_path FROM tasks WHERE task_type = ? ORDER BY created_time",
                (task_type,),
            )
        else:
            return self._execute_query(
                conn,
                "SELECT torrent_hash, task_type, hash_file_path FROM tasks ORDER BY created_time",
            )

    def _task_exists(self, conn, torrent_hash: str, task_type: str):
        result = self._execute_query(
            conn,
            "SELECT 1 FROM tasks WHERE torrent_hash = ? AND task_type = ?",
            (torrent_hash, task_type),
        )
        return len(result) > 0

    def _delete_task(self, conn, torrent_hash: str, task_type: str):
        return self._execute_update(
            conn,
            "DELETE FROM tasks WHERE torrent_hash = ? AND task_type = ?",
            (torrent_hash, task_type),
        )

    def _get_retry_count(self, conn, torrent_hash: str, task_type: str):
        result = self._execute_query(
            conn,
            "SELECT retry_count FROM retry_tasks WHERE torrent_hash = ? AND task_type = ?",
            (torrent_hash, task_type),
        )
        return result[0][0] if result else 0

    def _cleanup_orphaned_tasks(self, conn):
        return self._execute_update(
            conn,
            "DELETE FROM tasks WHERE hash_file_path IS NULL OR hash_file_path = ''",
        )

    def execute_command(self, method: str, *args, **kwargs) -> Any:
        """执行数据库命令并等待结果"""
        command_id = threading.current_thread().name + str(time.time())
        result_queue = Queue()

        with self.result_lock:
            self.result_queues[command_id] = result_queue

        try:
            # 发送命令
            self.command_queue.put(("command", command_id, method, args, kwargs))

            # 等待结果
            status, result = result_queue.get(timeout=30.0)

            if status == "success":
                return result
            else:
                raise Exception(f"数据库操作失败: {result}")

        finally:
            # 清理结果队列
            with self.result_lock:
                if command_id in self.result_queues:
                    del self.result_queues[command_id]

    def execute_async(self, method: str, *args, **kwargs):
        """异步执行数据库命令（不等待结果）"""
        command_id = "async_" + str(time.time())
        self.command_queue.put(("async", command_id, method, args, kwargs))

    def stop(self):
        """停止数据库管理线程"""
        self.running = False
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=10)
            self.logger.info("数据库管理线程已停止")


class TaskStore:
    """任务存储管理器（线程安全版本）"""

    def __init__(self, db_path: str = "tasks.db"):
        self.db_path = Path(db_path)
        self.logger = logging.getLogger(__name__)
        self.db_manager = DatabaseManager(str(self.db_path))

    def save_task(self, torrent_hash: str, task_type: str, hash_file_path: str):
        """保存任务"""
        try:
            self.db_manager.execute_command(
                "save_task", torrent_hash, task_type, hash_file_path
            )
            self.logger.debug(f"保存任务: {task_type} - {torrent_hash}")
        except Exception as e:
            self.logger.error(f"保存任务失败: {e}")
            raise

    def save_retry_task(
        self,
        torrent_hash: str,
        task_type: str,
        hash_file_path: str,
        retry_time: float,
        retry_count: int,
    ):
        """保存重试任务"""
        try:
            self.db_manager.execute_command(
                "save_retry_task",
                torrent_hash,
                task_type,
                hash_file_path,
                retry_time,
                retry_count,
            )
            self.logger.debug(
                f"保存重试任务: {task_type} - {torrent_hash}, 重试次数: {retry_count}"
            )
        except Exception as e:
            self.logger.error(f"保存重试任务失败: {e}")
            raise

    def get_pending_retry_tasks(self) -> List[Tuple]:
        """获取待处理的重试任务"""
        try:
            return self.db_manager.execute_command("get_pending_retry_tasks")
        except Exception as e:
            self.logger.error(f"获取重试任务失败: {e}")
            return []

    def delete_retry_task(self, torrent_hash: str, task_type: str):
        """删除重试任务"""
        try:
            self.db_manager.execute_command(
                "delete_retry_task", torrent_hash, task_type
            )
            self.logger.debug(f"删除重试任务: {task_type} - {torrent_hash}")
        except Exception as e:
            self.logger.error(f"删除重试任务失败: {e}")
            raise

    def get_pending_tasks(self, task_type: str = None) -> List[Tuple[str, str, str]]:
        """获取待处理任务"""
        try:
            return self.db_manager.execute_command("get_pending_tasks", task_type)
        except Exception as e:
            self.logger.error(f"获取待处理任务失败: {e}")
            return []

    def task_exists(self, torrent_hash: str, task_type: str) -> bool:
        """检查任务是否存在"""
        try:
            return self.db_manager.execute_command(
                "task_exists", torrent_hash, task_type
            )
        except Exception as e:
            self.logger.error(f"检查任务存在性失败: {e}")
            return False

    def delete_task(self, torrent_hash: str, task_type: str):
        """删除任务"""
        try:
            self.db_manager.execute_command("delete_task", torrent_hash, task_type)
            self.logger.debug(f"删除任务: {task_type} - {torrent_hash}")
        except Exception as e:
            self.logger.error(f"删除任务失败: {e}")
            raise

    def get_retry_count(self, torrent_hash: str, task_type: str) -> int:
        """获取重试计数"""
        try:
            return self.db_manager.execute_command(
                "get_retry_count", torrent_hash, task_type
            )
        except Exception as e:
            self.logger.error(f"获取重试计数失败: {e}")
            return 0

    def cleanup_orphaned_tasks(self):
        """清理孤立任务"""
        try:
            deleted_count = self.db_manager.execute_command("cleanup_orphaned_tasks")
            if deleted_count > 0:
                self.logger.info(f"清理了 {deleted_count} 个孤立任务")
        except Exception as e:
            self.logger.error(f"清理孤立任务失败: {e}")

    def save_retry_task_async(
        self,
        torrent_hash: str,
        task_type: str,
        hash_file_path: str,
        retry_time: float,
        retry_count: int,
    ):
        """异步保存重试任务"""
        try:
            self.db_manager.execute_async(
                "save_retry_task",
                torrent_hash,
                task_type,
                hash_file_path,
                retry_time,
                retry_count,
            )
        except Exception as e:
            self.logger.error(f"异步保存重试任务失败: {e}")

    def close(self):
        """关闭数据库管理器"""
        self.db_manager.stop()
