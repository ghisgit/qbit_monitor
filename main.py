import signal
import sys
import time
import logging
import os
from pathlib import Path

from config.settings import Config
from config.paths import ADDED_TORRENTS_DIR, COMPLETED_TORRENTS_DIR, LOG_FILE
from utils.logging import setup_logging
from core.qbittorrent import QBittorrentClient
from core.file_operations import FileOperations
from core.events import EventHandler
from monitor.directory_monitor import DirectoryMonitor
from monitor.task_manager import TaskWorkerManager
from persistence.task_store import TaskStore


class QBitMonitor:
    """qBittorrent 监控器主类"""

    def __init__(self):
        self.config = Config()
        setup_logging(LOG_FILE, self.config.debug_mode)
        self.logger = logging.getLogger(__name__)
        self.running = False

        # 初始化组件
        self._init_components()

    def _init_components(self):
        """初始化所有组件"""
        self.config.set_logger(self.logger)
        self.config.load_config()

        # 核心组件
        self.task_store = TaskStore("tasks.db")
        self.qbt_client = QBittorrentClient(self.config.host, self.config.port)
        self.file_ops = FileOperations(self.config)
        self.event_handler = EventHandler(self.qbt_client, self.file_ops, self.config)

        # 管理组件
        self.task_manager = TaskWorkerManager(
            self.event_handler,
            self.task_store,
            max_workers=getattr(self.config, "max_workers", 5),
        )

        self.directory_monitor = DirectoryMonitor(
            self.event_handler,
            ADDED_TORRENTS_DIR,
            COMPLETED_TORRENTS_DIR,
            self.task_manager,
            self.task_store,
        )

    def _safe_scan_existing_hash_files(self):
        """安全扫描现有哈希文件"""
        self.logger.info("安全扫描监控目录中的哈希文件...")

        added_count = 0
        completed_count = 0

        try:
            # 扫描添加目录
            for hash_file in ADDED_TORRENTS_DIR.glob("*.hash"):
                if self._process_hash_file(hash_file, "added"):
                    added_count += 1

            # 扫描完成目录
            for hash_file in COMPLETED_TORRENTS_DIR.glob("*.hash"):
                if self._process_hash_file(hash_file, "completed"):
                    completed_count += 1

            self.logger.info(
                f"安全扫描完成: {added_count}个添加文件, {completed_count}个完成文件"
            )

        except Exception as e:
            self.logger.error(f"安全扫描哈希文件失败: {e}")

    def _process_hash_file(self, hash_file: Path, task_type: str) -> bool:
        """处理单个哈希文件"""
        torrent_hash = hash_file.stem
        hash_file_path = str(hash_file)

        if not self.task_store.task_exists(torrent_hash, task_type):
            self.task_store.save_task(torrent_hash, task_type, hash_file_path)
            self._cleanup_hash_file(hash_file_path)

            self.task_manager.submit_task(
                task_type, torrent_hash, hash_file_path, from_startup=True
            )
            return True

        return False

    def _cleanup_hash_file(self, hash_file_path: str):
        """清理哈希文件"""
        if os.path.exists(hash_file_path):
            os.remove(hash_file_path)
            self.logger.debug(f"删除哈希文件: {hash_file_path}")

    def start(self):
        """启动监控器"""
        self.running = True
        self._setup_signal_handlers()

        self.qbt_client.wait_for_qbit()

        self.logger.info("qBittorrent 监控器启动")
        self.logger.info(f"最大工作线程数: {self.task_manager.max_workers}")

        # 安全启动流程
        self._start_safely()

        # 主循环
        self._run_main_loop()

    def _setup_signal_handlers(self):
        """设置信号处理器"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def _start_safely(self):
        """安全启动流程"""
        # 1. 加载数据库中的任务
        self.task_manager.load_pending_tasks()

        # 2. 立即启动事件监控
        self.logger.info("启动事件监控...")
        self.directory_monitor.start()
        self.logger.info("事件监控已启动")

        # 3. 安全扫描目录文件
        self._safe_scan_existing_hash_files()

        # 4. 启动工作线程
        self.task_manager.start_all_workers()

        self.logger.info("所有启动任务加载完成，开始处理...")

    def _run_main_loop(self):
        """运行主循环"""
        try:
            while self.running:
                self.config.load_config()
                time.sleep(self.config.check_interval)

        except KeyboardInterrupt:
            self.logger.info("收到键盘中断信号")
        except Exception as e:
            self.logger.error(f"主循环发生错误: {e}")
        finally:
            self.stop()

    def stop(self):
        """停止监控器"""
        self.running = False
        self.directory_monitor.stop()
        self.task_manager.stop_all_workers()
        self.logger.info("qBittorrent 监控器已停止")

    def signal_handler(self, signum, frame):
        """信号处理函数"""
        signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        self.logger.info(f"收到信号 {signal_name}")
        self.stop()
        sys.exit(0)


def main():
    """主函数"""
    monitor = QBitMonitor()
    monitor.start()


if __name__ == "__main__":
    main()
