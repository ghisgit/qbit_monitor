import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import logging


class TorrentFileHandler(FileSystemEventHandler):
    """种子文件事件处理器 - 适配新架构"""

    def __init__(self, task_manager, task_type: str):
        self.task_manager = task_manager
        self.task_type = task_type
        self.logger = logging.getLogger(__name__)

    def on_created(self, event):
        """处理文件创建事件"""
        if not event.is_directory and event.src_path.endswith(".hash"):
            self._handle_hash_file(event.src_path)

    def _handle_hash_file(self, file_path: str):
        """处理哈希文件"""
        filename = os.path.basename(file_path)
        torrent_hash = filename[:-5]  # 去掉 .hash 后缀

        self.logger.info(f"检测到新的哈希文件: {filename}")

        # 直接提交任务，新的任务管理器会处理重复检测
        success = self.task_manager.submit_task(self.task_type, torrent_hash, file_path)

        if success:
            self.logger.debug(f"成功提交任务: {torrent_hash}")
        else:
            self.logger.warning(f"提交任务失败: {torrent_hash}")
            # 即使提交失败，文件也不会被立即删除，避免数据丢失


class DirectoryMonitor:
    """目录监控器 - 适配新架构"""

    def __init__(
        self, event_handler, added_dir, completed_dir, task_manager, task_store
    ):
        self.event_handler = event_handler
        self.added_dir = added_dir
        self.completed_dir = completed_dir
        self.task_manager = task_manager
        self.observer = Observer()
        self.logger = logging.getLogger(__name__)

    def start(self):
        """开始监控"""
        # 创建事件处理器
        added_handler = TorrentFileHandler(self.task_manager, "added")
        completed_handler = TorrentFileHandler(self.task_manager, "completed")

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
