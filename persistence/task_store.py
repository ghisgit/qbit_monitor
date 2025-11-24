import sqlite3
import logging
import threading
import time
import json
import uuid
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from contextlib import contextmanager
from config.paths import DATA_DIR, DATA_FILE


@dataclass
class Task:
    task_uuid: str
    torrent_hash: str
    task_type: str
    status: str
    hash_file_path: str
    retry_count: int = 0
    failure_reason: str = None
    next_retry_time: float = None
    circuit_state: str = "closed"
    created_time: float = None
    updated_time: float = None
    last_attempt_time: float = None
    last_success_time: float = None
    current_phase: str = None


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout: int = 60
    half_open_timeout: int = 30


@dataclass
class RetryStrategy:
    base_delay: int
    max_delay: int = None
    backoff_multiplier: float = 2.0
    jitter_factor: float = 0.1
    max_retries: int = None  # None表示无限重试


class ThreadLocalConnection:
    """线程本地数据库连接管理"""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.local = threading.local()

    def get_connection(self) -> sqlite3.Connection:
        if not hasattr(self.local, "connection"):
            self.local.connection = sqlite3.connect(
                self.db_path, timeout=30.0, check_same_thread=False
            )
            # 优化配置
            self.local.connection.execute("PRAGMA journal_mode=WAL")
            self.local.connection.execute("PRAGMA synchronous=NORMAL")
            self.local.connection.execute("PRAGMA cache_size=-64000")
        return self.local.connection

    def close_all(self):
        if hasattr(self.local, "connection"):
            try:
                self.local.connection.close()
            except:
                pass
            delattr(self.local, "connection")


class TaskStore:
    """增强的任务存储管理器"""

    def __init__(self, db_path: str = DATA_FILE):
        self.db_path = Path(db_path)

        # 确保数据库目录存在
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self.logger = logging.getLogger(__name__)
        self.connection_manager = ThreadLocalConnection(str(self.db_path))
        self._init_database()

    def _init_database(self):
        """初始化数据库表结构"""
        conn = self.connection_manager.get_connection()
        cursor = conn.cursor()

        # 主任务表
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_uuid TEXT UNIQUE NOT NULL,
                torrent_hash TEXT NOT NULL,
                task_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                current_phase TEXT,
                retry_count INTEGER DEFAULT 0,
                failure_reason TEXT,
                next_retry_time REAL,
                circuit_state TEXT DEFAULT 'closed',
                created_time REAL NOT NULL,
                updated_time REAL NOT NULL,
                last_attempt_time REAL,
                last_success_time REAL,
                hash_file_path TEXT NOT NULL,
                
                CHECK (status IN ('pending', 'processing', 'completed', 'archived', 'waiting'))
            )
        """
        )

        # 任务事件表
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS task_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_uuid TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_data TEXT,
                created_time REAL NOT NULL,
                FOREIGN KEY (task_uuid) REFERENCES tasks(task_uuid)
            )
        """
        )

        # 熔断状态表
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS circuit_break_status (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                breaker_type TEXT UNIQUE NOT NULL,
                state TEXT NOT NULL,
                failure_count INTEGER DEFAULT 0,
                success_count INTEGER DEFAULT 0,
                last_state_change REAL NOT NULL,
                last_failure_time REAL,
                last_success_time REAL,
                config TEXT,
                created_time REAL NOT NULL,
                updated_time REAL NOT NULL
            )
        """
        )

        # 检查点表
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS checkpoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                task_uuid TEXT NOT NULL,
                phase_name TEXT NOT NULL,
                checkpoint_data TEXT,
                created_time REAL NOT NULL,
                FOREIGN KEY (task_uuid) REFERENCES tasks(task_uuid),
                UNIQUE(task_uuid, phase_name)
            )
        """
        )

        # 创建索引
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_status_retry 
            ON tasks(status, next_retry_time) 
            WHERE status IN ('pending', 'waiting')
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_circuit 
            ON tasks(circuit_state, status)
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_tasks_hash_type 
            ON tasks(torrent_hash, task_type)
        """
        )
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_events_task 
            ON task_events(task_uuid, created_time)
        """
        )

        conn.commit()
        self.logger.info("数据库初始化完成")

    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        conn = self.connection_manager.get_connection()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    def save_task(self, task: Task) -> bool:
        """保存任务到数据库"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    INSERT OR REPLACE INTO tasks (
                        task_uuid, torrent_hash, task_type, status, hash_file_path,
                        retry_count, failure_reason, next_retry_time, circuit_state,
                        created_time, updated_time, last_attempt_time, last_success_time,
                        current_phase
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        task.task_uuid,
                        task.torrent_hash,
                        task.task_type,
                        task.status,
                        task.hash_file_path,
                        task.retry_count,
                        task.failure_reason,
                        task.next_retry_time,
                        task.circuit_state,
                        task.created_time,
                        task.updated_time,
                        task.last_attempt_time,
                        task.last_success_time,
                        task.current_phase,
                    ),
                )

                # 记录创建事件
                cursor.execute(
                    """
                    INSERT INTO task_events (task_uuid, event_type, event_data, created_time)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        task.task_uuid,
                        "created",
                        json.dumps({"hash_file_path": task.hash_file_path}),
                        time.time(),
                    ),
                )

            return True

        except Exception as e:
            self.logger.error(f"保存任务失败: {e}")
            return False

    def get_eligible_tasks(self, limit: int = 10) -> List[Task]:
        """获取符合条件的待处理任务"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    SELECT * FROM tasks 
                    WHERE status IN ('pending', 'waiting')
                    AND circuit_state = 'closed'
                    AND (next_retry_time IS NULL OR next_retry_time <= ?)
                    ORDER BY 
                        CASE failure_reason 
                            WHEN 'metadata_not_ready' THEN 1
                            WHEN 'qbit_api_error' THEN 2
                            ELSE 3 
                        END,
                        retry_count ASC,
                        created_time ASC
                    LIMIT ?
                """,
                    (time.time(), limit),
                )

                tasks = []
                for row in cursor.fetchall():
                    task = self._row_to_task(
                        dict(zip([col[0] for col in cursor.description], row))
                    )
                    tasks.append(task)

                # 标记任务为处理中
                for task in tasks:
                    cursor.execute(
                        """
                        UPDATE tasks 
                        SET status = 'processing', last_attempt_time = ?, updated_time = ?
                        WHERE task_uuid = ?
                    """,
                        (time.time(), time.time(), task.task_uuid),
                    )

                    cursor.execute(
                        """
                        INSERT INTO task_events (task_uuid, event_type, event_data, created_time)
                        VALUES (?, ?, ?, ?)
                    """,
                        (
                            task.task_uuid,
                            "processing_started",
                            json.dumps({"batch_size": limit}),
                            time.time(),
                        ),
                    )

                return tasks

        except Exception as e:
            self.logger.error(f"获取待处理任务失败: {e}")
            return []

    def update_task_after_processing(
        self,
        task_uuid: str,
        success: bool,
        failure_reason: str = None,
        error_message: str = None,
        next_retry_time: float = None,
    ) -> bool:
        """更新任务处理结果"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                if success:
                    cursor.execute(
                        """
                        UPDATE tasks 
                        SET status = 'completed', 
                            last_success_time = ?,
                            updated_time = ?
                        WHERE task_uuid = ?
                    """,
                        (time.time(), time.time(), task_uuid),
                    )

                    cursor.execute(
                        """
                        INSERT INTO task_events (task_uuid, event_type, event_data, created_time)
                        VALUES (?, ?, ?, ?)
                    """,
                        (task_uuid, "completed", "{}", time.time()),
                    )

                else:
                    cursor.execute(
                        """
                        UPDATE tasks 
                        SET status = 'waiting',
                            retry_count = retry_count + 1,
                            failure_reason = ?,
                            next_retry_time = ?,
                            updated_time = ?
                        WHERE task_uuid = ?
                    """,
                        (failure_reason, next_retry_time, time.time(), task_uuid),
                    )

                    cursor.execute(
                        """
                        INSERT INTO task_events (task_uuid, event_type, event_data, created_time)
                        VALUES (?, ?, ?, ?)
                    """,
                        (
                            task_uuid,
                            "retry_scheduled",
                            json.dumps(
                                {
                                    "reason": failure_reason,
                                    "next_retry": next_retry_time,
                                    "retry_count": self._get_retry_count(
                                        conn, task_uuid
                                    )
                                    + 1,
                                }
                            ),
                            time.time(),
                        ),
                    )

                return True

        except Exception as e:
            self.logger.error(f"更新任务状态失败: {e}")
            return False

    def archive_task(self, task_uuid: str, reason: str) -> bool:
        """归档任务"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    """
                    UPDATE tasks 
                    SET status = 'archived', updated_time = ?
                    WHERE task_uuid = ?
                """,
                    (time.time(), task_uuid),
                )

                cursor.execute(
                    """
                    INSERT INTO task_events (task_uuid, event_type, event_data, created_time)
                    VALUES (?, ?, ?, ?)
                """,
                    (
                        task_uuid,
                        "archived",
                        json.dumps({"reason": reason}),
                        time.time(),
                    ),
                )

                return True

        except Exception as e:
            self.logger.error(f"归档任务失败: {e}")
            return False

    def task_exists(self, torrent_hash: str, task_type: str) -> bool:
        """检查任务是否存在 - 兼容性方法"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT 1 FROM tasks 
                    WHERE torrent_hash = ? AND task_type = ?
                    AND status != 'completed' AND status != 'archived'
                """,
                    (torrent_hash, task_type),
                )

                return cursor.fetchone() is not None

        except Exception as e:
            self.logger.error(f"检查任务存在性失败: {e}")
            return False

    def get_pending_tasks(self, task_type: str = None) -> List[Tuple[str, str, str]]:
        """获取待处理任务 - 兼容性方法"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()

                if task_type:
                    cursor.execute(
                        """
                        SELECT torrent_hash, task_type, hash_file_path 
                        FROM tasks 
                        WHERE task_type = ? 
                        AND status IN ('pending', 'waiting')
                        ORDER BY created_time
                    """,
                        (task_type,),
                    )
                else:
                    cursor.execute(
                        """
                        SELECT torrent_hash, task_type, hash_file_path 
                        FROM tasks 
                        WHERE status IN ('pending', 'waiting')
                        ORDER BY created_time
                    """
                    )

                return cursor.fetchall()

        except Exception as e:
            self.logger.error(f"获取待处理任务失败: {e}")
            return []

    def delete_task(self, torrent_hash: str, task_type: str):
        """删除任务 - 兼容性方法"""
        try:
            with self.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    DELETE FROM tasks 
                    WHERE torrent_hash = ? AND task_type = ?
                """,
                    (torrent_hash, task_type),
                )

        except Exception as e:
            self.logger.error(f"删除任务失败: {e}")
            raise

    def _get_retry_count(self, conn, task_uuid: str) -> int:
        """获取任务当前重试次数"""
        cursor = conn.cursor()
        cursor.execute(
            "SELECT retry_count FROM tasks WHERE task_uuid = ?", (task_uuid,)
        )
        result = cursor.fetchone()
        return result[0] if result else 0

    def _row_to_task(self, row: Dict) -> Task:
        """将数据库行转换为Task对象"""
        return Task(
            task_uuid=row["task_uuid"],
            torrent_hash=row["torrent_hash"],
            task_type=row["task_type"],
            status=row["status"],
            hash_file_path=row["hash_file_path"],
            retry_count=row["retry_count"],
            failure_reason=row["failure_reason"],
            next_retry_time=row["next_retry_time"],
            circuit_state=row["circuit_state"],
            created_time=row["created_time"],
            updated_time=row["updated_time"],
            last_attempt_time=row["last_attempt_time"],
            last_success_time=row["last_success_time"],
            current_phase=row["current_phase"],
        )

    def close(self):
        """关闭数据库连接"""
        self.connection_manager.close_all()
