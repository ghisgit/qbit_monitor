import sqlite3
import logging
from pathlib import Path
from typing import List, Tuple


class TaskStore:
    """任务存储管理器"""

    def __init__(self, db_path: str = "tasks.db"):
        self.db_path = Path(db_path)
        self.logger = logging.getLogger(__name__)
        self._init_database()

    def _init_database(self):
        """初始化数据库"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

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

                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_hash_type ON tasks(torrent_hash, task_type)"
                )
                cursor.execute(
                    "CREATE INDEX IF NOT EXISTS idx_type ON tasks(task_type)"
                )

            self.logger.debug("数据库初始化完成")

        except Exception as e:
            self.logger.error(f"数据库初始化失败: {e}")
            raise

    def _get_connection(self):
        """获取数据库连接"""
        return sqlite3.connect(self.db_path)

    def save_task(self, torrent_hash: str, task_type: str, hash_file_path: str):
        """保存任务"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO tasks (torrent_hash, task_type, hash_file_path)
                    VALUES (?, ?, ?)
                """,
                    (torrent_hash, task_type, hash_file_path),
                )

            self.logger.debug(f"保存任务: {task_type} - {torrent_hash}")

        except Exception as e:
            self.logger.error(f"保存任务失败: {e}")
            raise

    def delete_task(self, torrent_hash: str, task_type: str):
        """删除任务"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    DELETE FROM tasks WHERE torrent_hash = ? AND task_type = ?
                """,
                    (torrent_hash, task_type),
                )

            self.logger.debug(f"删除任务: {task_type} - {torrent_hash}")

        except Exception as e:
            self.logger.error(f"删除任务失败: {e}")
            raise

    def get_pending_tasks(self, task_type: str = None) -> List[Tuple[str, str, str]]:
        """获取待处理任务"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                if task_type:
                    cursor.execute(
                        """
                        SELECT torrent_hash, task_type, hash_file_path FROM tasks 
                        WHERE task_type = ? ORDER BY created_time
                    """,
                        (task_type,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT torrent_hash, task_type, hash_file_path FROM tasks 
                        ORDER BY created_time
                    """
                    )

                return cursor.fetchall()

        except Exception as e:
            self.logger.error(f"获取待处理任务失败: {e}")
            return []

    def task_exists(self, torrent_hash: str, task_type: str) -> bool:
        """检查任务是否存在"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT 1 FROM tasks WHERE torrent_hash = ? AND task_type = ?
                """,
                    (torrent_hash, task_type),
                )

                return cursor.fetchone() is not None

        except Exception as e:
            self.logger.error(f"检查任务存在性失败: {e}")
            return False

    def cleanup_orphaned_tasks(self):
        """清理孤立任务"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    DELETE FROM tasks 
                    WHERE hash_file_path IS NULL OR hash_file_path = ''
                """
                )

                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    self.logger.info(f"清理了 {deleted_count} 个孤立任务")

        except Exception as e:
            self.logger.error(f"清理孤立任务失败: {e}")
