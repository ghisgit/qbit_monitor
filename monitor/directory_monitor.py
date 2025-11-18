import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging


class TorrentFileHandler(FileSystemEventHandler):
    """种子文件事件处理器"""

    def __init__(self, task_manager, task_store, task_type: str):
        self.task_manager = task_manager
        self.task_store = task_store
        self.task_type = task_type
        self.logger = logging.getLogger(__name__)

    def on_created(self, event):
        """处理文件创建事件"""
        if not event.is_directory and event.src_path.endswith(".hash"):
            self._handle_hash_file(event.src_path)

    def _handle_hash_file(self, file_path: str):
        """处理哈希文件"""
        filename = os.path.basename(file_path)
        torrent_hash = filename[:-5]

        self.logger.info(f"检测到新的哈希文件: {filename}")

        # 检查是否已存在（避免重复处理）
        if not self.task_store.task_exists(torrent_hash, self.task_type):
            self.task_manager.submit_task(self.task_type, torrent_hash, file_path)
        else:
            self.logger.debug(f"任务已存在，删除重复文件: {torrent_hash}")
            self._cleanup_duplicate_file(file_path)

    def _cleanup_duplicate_file(self, file_path: str):
        """清理重复文件"""
        if os.path.exists(file_path):
            os.remove(file_path)


class DirectoryMonitor:
    """目录监控器"""

    def __init__(
        self, event_handler, added_dir, completed_dir, task_manager, task_store
    ):
        self.event_handler = event_handler
        self.added_dir = added_dir
        self.completed_dir = completed_dir
        self.task_manager = task_manager
        self.task_store = task_store
        self.observer = Observer()
        self.logger = logging.getLogger(__name__)

    def start(self):
        """开始监控"""
        # 创建事件处理器
        added_handler = TorrentFileHandler(self.task_manager, self.task_store, "added")
        completed_handler = TorrentFileHandler(
            self.task_manager, self.task_store, "completed"
        )

        # 注册监控
        self.observer.schedule(added_handler, self.added_dir, recursive=False)
        self.observer.schedule(completed_handler, self.completed_dir, recursive=False)

        self.observer.start()
        self.logger.info("目录监控已启动")

    def stop(self):
        """停止监控"""
        self.observer.stop()
        self.observer.join()
        self.logger.info("目录监控已停止")
