import sqlite3
import logging
import threading
import time
from pathlib import Path
from typing import List
from contextlib import contextmanager
from dataclasses import dataclass
from config.paths import DATA_FILE


@dataclass
class Task:
    """任务数据结构"""

    torrent_hash: str
    task_type: str
    status: str  # 'pending', 'processing', 'failed'
    retry_count: int = 0
    last_attempt: float = 0
    next_retry: float = 0
    failure_reason: str = ""


class TaskStore:
    """任务存储管理器 - 只有任务完成或种子不存在时才删除"""

    def __init__(self, db_path: str = DATA_FILE):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
        self.local = threading.local()
        self._init_database()

    def _get_connection(self) -> sqlite3.Connection:
        """获取线程本地数据库连接"""
        if not hasattr(self.local, "connection"):
            self.local.connection = sqlite3.connect(
                self.db_path, timeout=30.0, check_same_thread=False
            )
            self.local.connection.execute("PRAGMA journal_mode=WAL")
            self.local.connection.execute("PRAGMA synchronous=NORMAL")
        return self.local.connection

    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        conn = self._get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def _init_database(self):
        """初始化数据库表结构"""
        with self.transaction() as conn:
            cursor = conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    torrent_hash TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    last_attempt REAL DEFAULT 0,
                    next_retry REAL DEFAULT 0,
                    failure_reason TEXT,
                    created_time REAL NOT NULL,
                    updated_time REAL NOT NULL,
                    UNIQUE(torrent_hash, task_type)
                )
            """
            )

            # 创建索引
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tasks_status 
                ON tasks(status, next_retry)
            """
            )

            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tasks_hash 
                ON tasks(torrent_hash)
            """
            )

            # 任务清理索引（针对长期失败的任务）
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_tasks_cleanup 
                ON tasks(status, retry_count, updated_time)
            """
            )

        self.logger.info("任务存储数据库初始化完成")

    def task_exists(self, torrent_hash: str, task_type: str) -> bool:
        """检查任务是否存在"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT 1 FROM tasks WHERE torrent_hash = ? AND task_type = ? AND status != 'deleted'",
                    (torrent_hash, task_type),
                )
                return cursor.fetchone() is not None
        except Exception as e:
            self.logger.error(f"检查任务存在性失败: {e}")
            return True  # 如果出错，假定任务存在以防止重复

    def save_task(self, torrent_hash: str, task_type: str) -> bool:
        """保存新任务"""
        try:
            current_time = time.time()
            with self.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO tasks 
                    (torrent_hash, task_type, created_time, updated_time)
                    VALUES (?, ?, ?, ?)
                """,
                    (torrent_hash, task_type, current_time, current_time),
                )

                return cursor.rowcount > 0
        except Exception as e:
            self.logger.error(f"保存任务失败: {e}")
            return False

    def get_pending_tasks(self, limit: int = 10) -> List[Task]:
        """获取待处理任务"""
        try:
            current_time = time.time()
            tasks = []

            with self.transaction() as conn:
                cursor = conn.cursor()

                # 第一步：查询需要处理的任务
                cursor.execute(
                    """
                    SELECT torrent_hash, task_type, status, retry_count, 
                           last_attempt, next_retry, failure_reason
                    FROM tasks 
                    WHERE status IN ('pending', 'failed')
                    AND (next_retry = 0 OR next_retry <= ?)
                    ORDER BY 
                        CASE 
                            WHEN status = 'failed' THEN last_attempt
                            ELSE created_time 
                        END ASC
                    LIMIT ?
                """,
                    (current_time, limit),
                )

                task_rows = cursor.fetchall()

                # 第二步：逐个标记为processing（避免并发问题）
                for row in task_rows:
                    torrent_hash = row[0]
                    task_type = row[1]

                    # 尝试更新状态，确保只有我们获取到这个任务
                    cursor.execute(
                        """
                        UPDATE tasks 
                        SET status = 'processing', last_attempt = ?, updated_time = ?
                        WHERE torrent_hash = ? AND task_type = ? AND status IN ('pending', 'failed')
                        """,
                        (current_time, current_time, torrent_hash, task_type),
                    )

                    # 检查是否成功更新
                    if cursor.rowcount > 0:
                        # 成功获取到任务，添加到返回列表
                        task = Task(
                            torrent_hash=torrent_hash,
                            task_type=task_type,
                            status=row[2],
                            retry_count=row[3],
                            last_attempt=row[4],
                            next_retry=row[5],
                            failure_reason=row[6] or "",
                        )
                        tasks.append(task)
                        self.logger.debug(f"成功获取任务: {torrent_hash} - {task_type}")
                    else:
                        # 任务已被其他线程获取，跳过
                        self.logger.debug(
                            f"任务已被其他线程获取: {torrent_hash} - {task_type}"
                        )

                return tasks

        except Exception as e:
            self.logger.error(f"获取待处理任务失败: {e}")
            return []

    def complete_task(self, torrent_hash: str, task_type: str) -> bool:
        """任务完成 - 从数据库删除"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                # 先检查任务是否存在且状态为processing
                cursor.execute(
                    "SELECT status FROM tasks WHERE torrent_hash = ? AND task_type = ?",
                    (torrent_hash, task_type),
                )

                task_info = cursor.fetchone()
                if not task_info:
                    self.logger.debug(
                        f"任务不存在，无需删除: {torrent_hash} - {task_type}"
                    )
                    return False

                current_status = task_info[0]
                if current_status != "processing":
                    self.logger.warning(
                        f"任务状态不是processing，当前状态: {current_status}"
                    )

                # 删除任务
                cursor.execute(
                    "DELETE FROM tasks WHERE torrent_hash = ? AND task_type = ?",
                    (torrent_hash, task_type),
                )

                deleted = cursor.rowcount > 0
                if deleted:
                    self.logger.debug(f"任务删除成功: {torrent_hash} - {task_type}")
                else:
                    self.logger.debug(
                        f"任务删除失败（可能已被删除）: {torrent_hash} - {task_type}"
                    )

                return deleted
        except Exception as e:
            self.logger.error(f"完成任务失败: {e}")
            return False

    def schedule_retry(
        self,
        torrent_hash: str,
        task_type: str,
        next_retry_time: float,
        failure_reason: str = "",
    ) -> bool:
        """安排任务重试"""
        try:
            current_time = time.time()
            with self.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE tasks 
                    SET status = 'failed', 
                        retry_count = retry_count + 1,
                        next_retry = ?,
                        failure_reason = ?,
                        updated_time = ?
                    WHERE torrent_hash = ? AND task_type = ?
                """,
                    (
                        next_retry_time,
                        failure_reason,
                        current_time,
                        torrent_hash,
                        task_type,
                    ),
                )

                updated = cursor.rowcount > 0
                if not updated:
                    self.logger.debug(
                        f"安排重试失败，任务不存在: {torrent_hash} - {task_type}"
                    )

                return updated
        except Exception as e:
            self.logger.error(f"安排重试失败: {e}")
            return False

    def cleanup_orphaned_tasks(self, qbt_client, days: int = 7):
        """清理孤儿任务（种子已不存在的任务）"""
        try:
            cutoff_time = time.time() - (days * 24 * 3600)
            with self.transaction() as conn:
                cursor = conn.cursor()

                # 获取所有任务
                cursor.execute(
                    """
                    SELECT torrent_hash, task_type FROM tasks 
                    WHERE updated_time < ?
                """,
                    (cutoff_time,),
                )

                tasks_to_check = cursor.fetchall()
                deleted_count = 0

                for torrent_hash, task_type in tasks_to_check:
                    # 检查种子是否还存在
                    try:
                        torrent = qbt_client.get_torrent_by_hash(torrent_hash)
                        if torrent is None:
                            # 种子不存在，删除任务
                            cursor.execute(
                                """
                                DELETE FROM tasks 
                                WHERE torrent_hash = ? AND task_type = ?
                            """,
                                (torrent_hash, task_type),
                            )
                            deleted_count += 1
                            self.logger.info(
                                f"清理孤儿任务: {torrent_hash} - {task_type}（种子不存在）"
                            )
                    except Exception as e:
                        # 检查种子时出错，跳过这个任务
                        self.logger.warning(
                            f"检查种子失败，跳过任务: {torrent_hash}, 错误: {e}"
                        )
                        continue

                if deleted_count > 0:
                    self.logger.info(f"清理了 {deleted_count} 个孤儿任务")

        except Exception as e:
            self.logger.error(f"清理孤儿任务失败: {e}")

    def get_task_stats(self) -> dict:
        """获取任务统计信息"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                # 按状态统计
                cursor.execute(
                    """
                    SELECT status, COUNT(*) as count, 
                           AVG(retry_count) as avg_retries,
                           MIN(created_time) as oldest
                    FROM tasks 
                    GROUP BY status
                """
                )

                stats = {}
                for row in cursor.fetchall():
                    status = row[0]
                    stats[status] = {
                        "count": row[1],
                        "avg_retries": row[2],
                        "oldest_time": row[3],
                        "oldest_hours": (time.time() - row[3]) / 3600 if row[3] else 0,
                    }

                # 总任务数
                cursor.execute("SELECT COUNT(*) FROM tasks")
                total = cursor.fetchone()[0]

                return {"total": total, "by_status": stats, "timestamp": time.time()}

        except Exception as e:
            self.logger.error(f"获取任务统计失败: {e}")
            return {"error": str(e)}

    def close(self):
        """关闭数据库连接"""
        if hasattr(self.local, "connection"):
            try:
                self.local.connection.close()
            except Exception:
                pass
            delattr(self.local, "connection")
