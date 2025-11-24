import signal
import sys
import time
import logging
import os
from pathlib import Path

from config.settings import Config
from config.paths import (
    ADDED_TORRENTS_DIR,
    COMPLETED_TORRENTS_DIR,
    LOG_FILE,
    DATA_FILE,
    ensure_directories,
)
from utils.logging import setup_logging
from core.qbittorrent import QBittorrentClient
from core.file_operations import FileOperations
from core.events import EventHandler
from monitor.directory_monitor import DirectoryMonitor
from monitor.task_manager import ResilientTaskManager
from persistence.task_store import TaskStore


class QBitMonitor:
    """增强的qBittorrent监控器 - 永不丢失架构"""

    def __init__(self):
        # 首先确保所有目录都存在
        ensure_directories()

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
        self.task_store = TaskStore(DATA_FILE)
        self.qbt_client = QBittorrentClient(self.config.host, self.config.port)
        self.file_ops = FileOperations(self.config)
        self.event_handler = EventHandler(self.qbt_client, self.file_ops, self.config)

        # 弹性任务管理器
        self.task_manager = ResilientTaskManager(
            event_handler=self.event_handler,
            task_store=self.task_store,
            qbt_client=self.qbt_client,
            file_ops=self.file_ops,
            config=self.config,
        )

        # 目录监控器
        self.directory_monitor = DirectoryMonitor(
            event_handler=self.event_handler,
            added_dir=ADDED_TORRENTS_DIR,
            completed_dir=COMPLETED_TORRENTS_DIR,
            task_manager=self.task_manager,
            task_store=self.task_store,
        )

    def start(self):
        """启动监控器"""
        self.running = True
        self._setup_signal_handlers()

        # 等待qBittorrent启动
        self.qbt_client.wait_for_qbit()

        self.logger.info("qBittorrent监控器启动 - 永不丢失架构")
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
        # 1. 启动事件监控
        self.logger.info("启动事件监控...")
        self.directory_monitor.start()
        self.logger.info("事件监控已启动")

        # 2. 安全扫描目录文件（确保不会重复提交）
        self._safe_scan_existing_hash_files()

        # 3. 启动工作线程
        self.task_manager.start_all_workers()

        self.logger.info("所有启动任务加载完成，开始处理...")

    def _safe_scan_existing_hash_files(self):
        """安全扫描现有哈希文件"""
        self.logger.info("安全扫描监控目录中的哈希文件...")

        added_count = 0
        completed_count = 0

        try:
            # 扫描添加目录
            for hash_file in ADDED_TORRENTS_DIR.glob("*.hash"):
                success = self.task_manager.submit_task(
                    "added", hash_file.stem, str(hash_file)
                )
                if success:
                    added_count += 1
                else:
                    self.logger.warning(f"扫描提交失败: added - {hash_file.stem}")

            # 扫描完成目录
            for hash_file in COMPLETED_TORRENTS_DIR.glob("*.hash"):
                success = self.task_manager.submit_task(
                    "completed", hash_file.stem, str(hash_file)
                )
                if success:
                    completed_count += 1
                else:
                    self.logger.warning(f"扫描提交失败: completed - {hash_file.stem}")

            self.logger.info(
                f"安全扫描完成: {added_count}个添加文件, {completed_count}个完成文件"
            )

        except Exception as e:
            self.logger.error(f"安全扫描哈希文件失败: {e}")

    def _run_main_loop(self):
        """运行主循环"""
        try:
            while self.running:
                # 定期重新加载配置
                self.config.load_config()

                # 输出系统状态（可选）
                if self.config.debug_mode:
                    status = self.task_manager.get_system_status()
                    self.logger.debug(f"系统状态: {status}")

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

        # 关闭数据库连接
        self.task_store.close()

        self.logger.info("qBittorrent监控器已停止")

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
