"""
数据模型和存储管理
定义任务数据结构和数据库操作
"""

import time
import sqlite3
import logging
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import List, Dict, Any
from utils.database import DatabaseManager


@dataclass
class Task:
    """任务数据模型"""

    torrent_hash: str
    task_type: str  # 'added' 或 'completed'
    status: str = "pending"  # 'pending', 'processing', 'failed'
    retry_count: int = 0
    created_time: float = 0
    updated_time: float = 0

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)


class TaskStore:
    """任务存储管理器"""

    def __init__(self, db_path: str = "data/tasks.db"):
        """
        初始化任务存储

        Args:
            db_path: 数据库文件路径
        """
        self.db_path = str(Path(db_path).resolve())
        self.db_manager = DatabaseManager()
        self.logger = logging.getLogger(__name__)

        # 确保目录存在
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)

        # 初始化数据库
        self._initialize_database()

    def _initialize_database(self):
        """初始化数据库表结构"""
        try:
            with self.db_manager.transaction(self.db_path) as conn:
                self._create_tables(conn)
                self._create_indexes(conn)

            self.logger.info("任务存储数据库初始化完成")

        except Exception as e:
            self.logger.error(f"数据库初始化失败: {e}")
            raise

    def _create_tables(self, conn: sqlite3.Connection):
        """创建数据表"""
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                torrent_hash TEXT NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                retry_count INTEGER DEFAULT 0,
                created_time REAL NOT NULL,
                updated_time REAL NOT NULL,
                PRIMARY KEY (torrent_hash, task_type)
            )
        """
        )

    def _create_indexes(self, conn: sqlite3.Connection):
        """创建索引"""
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_status 
            ON tasks(status, created_time)
        """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_hash 
            ON tasks(torrent_hash)
        """
        )

    def save_task(self, torrent_hash: str, task_type: str) -> bool:
        """
        保存新任务

        Args:
            torrent_hash: 种子哈希
            task_type: 任务类型

        Returns:
            bool: 是否成功保存
        """
        try:
            current_time = time.time()

            with self.db_manager.transaction(self.db_path) as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT 1 FROM tasks 
                    WHERE torrent_hash = ? AND status = 'processing'
                    LIMIT 1
                    """,
                    (torrent_hash,),
                )

                if cursor.fetchone():
                    return False

                cursor.execute(
                    """
                    INSERT OR IGNORE INTO tasks 
                    (torrent_hash, task_type, created_time, updated_time, status)
                    VALUES (?, ?, ?, ?, 'pending')
                    """,
                    (torrent_hash, task_type, current_time, current_time),
                )

                if cursor.rowcount == 0:
                    cursor.execute(
                        """
                        UPDATE tasks 
                        SET status = 'pending', updated_time = ?
                        WHERE torrent_hash = ? AND task_type = ? 
                        AND status != 'processing'
                        """,
                        (current_time, torrent_hash, task_type),
                    )

                return cursor.rowcount > 0

        except Exception as e:
            self.logger.error(f"保存任务失败: {e}")
            return False

    def task_exists(self, torrent_hash: str, task_type: str) -> bool:
        """
        检查任务是否存在

        Args:
            torrent_hash: 种子哈希
            task_type: 任务类型

        Returns:
            bool: 任务是否存在
        """
        try:
            with self.db_manager.transaction(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT 1 FROM tasks 
                    WHERE torrent_hash = ? AND task_type = ?
                """,
                    (torrent_hash, task_type),
                )

                return cursor.fetchone() is not None

        except Exception as e:
            self.logger.error(f"检查任务存在性失败: {e}")
            return True  # 出错时假定存在，防止重复

    def get_pending_tasks(self, limit: int = 10) -> List[Task]:
        """
        获取待处理任务

        Args:
            limit: 最大任务数量

        Returns:
            List[Task]: 任务列表
        """
        tasks = []
        current_time = time.time()

        try:
            with self.db_manager.transaction(self.db_path) as conn:
                cursor = conn.cursor()

                # 查询待处理任务
                cursor.execute(
                    """
                    SELECT torrent_hash, task_type, status, retry_count, created_time
                    FROM tasks 
                    WHERE status = 'pending'
                    ORDER BY created_time ASC
                    LIMIT ?
                """,
                    (limit,),
                )

                rows = cursor.fetchall()

                # 标记为处理中
                for row in rows:
                    torrent_hash = row[0]
                    task_type = row[1]

                    cursor.execute(
                        """
                        UPDATE tasks SET status = 'processing', updated_time = ?
                        WHERE torrent_hash = ? AND task_type = ? 
                        AND status = 'pending'
                    """,
                        (current_time, torrent_hash, task_type),
                    )

                    if cursor.rowcount > 0:
                        tasks.append(
                            Task(
                                torrent_hash=torrent_hash,
                                task_type=task_type,
                                status=row[2],
                                retry_count=row[3],
                                created_time=row[4],
                                updated_time=current_time,
                            )
                        )

            return tasks

        except Exception as e:
            self.logger.error(f"获取待处理任务失败: {e}")
            return []

    def complete_task(self, torrent_hash: str, task_type: str) -> bool:
        """
        完成任务

        Args:
            torrent_hash: 种子哈希
            task_type: 任务类型

        Returns:
            bool: 是否成功完成
        """
        try:
            with self.db_manager.transaction(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    DELETE FROM tasks 
                    WHERE torrent_hash = ? AND task_type = ?
                """,
                    (torrent_hash, task_type),
                )
                success = cursor.rowcount > 0

                time.sleep(0.1)

                return success

        except Exception as e:
            self.logger.error(f"完成任务失败: {e}")
            return False

    def reset_stuck_tasks(self, timeout_hours: float = 0.5):
        """
        重置卡住的任务

        Args:
            timeout_hours: 超时时间（小时）
        """
        try:
            cutoff_time = time.time() - (timeout_hours * 3600)

            with self.db_manager.transaction(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE tasks 
                    SET status = 'pending', updated_time = ?
                    WHERE status = 'processing' 
                    AND updated_time < ?
                """,
                    (time.time(), cutoff_time),
                )

                if cursor.rowcount > 0:
                    self.logger.info(f"重置了 {cursor.rowcount} 个卡住的任务")

        except Exception as e:
            self.logger.error(f"重置卡住任务失败: {e}")

    def get_statistics(self) -> Dict[str, Any]:
        """
        获取任务统计信息

        Returns:
            Dict: 统计信息
        """
        try:
            with self.db_manager.transaction(self.db_path) as conn:
                cursor = conn.cursor()

                # 按状态统计
                cursor.execute(
                    """
                    SELECT status, COUNT(*) as count
                    FROM tasks 
                    GROUP BY status
                """
                )

                stats = {}
                for row in cursor.fetchall():
                    stats[row[0]] = {"count": row[1]}

                # 总任务数
                cursor.execute("SELECT COUNT(*) FROM tasks")
                total = cursor.fetchone()[0]

                return {"total": total, "by_status": stats, "timestamp": time.time()}

        except Exception as e:
            self.logger.error(f"获取任务统计失败: {e}")
            return {"error": str(e)}

    def close(self):
        """关闭数据库连接"""
        try:
            self.db_manager.close_all()
        except Exception as e:
            self.logger.error(f"关闭数据库连接失败: {e}")
