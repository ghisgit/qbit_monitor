import signal
import sys
import threading
import time
import logging

from config.settings import Config
from utils.logging import setup_logging
from core.qbittorrent import QBittorrentClient
from core.file_operations import FileOperations
from core.events import EventHandler
from monitor.task_manager import TaskManager
from monitor.stalled_seed_monitor import StalledSeedMonitor


class QBitMonitor:
    """qBittorrent监控器 - 基于标签架构"""

    def __init__(self):
        self.config = Config()
        setup_logging(debug_mode=self.config.debug_mode)
        self.logger = logging.getLogger(__name__)
        self.running = False
        self.already_stopped = False
        self._init_components()

    def _init_components(self):
        """初始化所有组件"""
        self.config.set_logger(self.logger)
        self.config.load_config()

        # 核心组件
        self.qbt_client = QBittorrentClient(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
        )
        self.file_ops = FileOperations(self.config)
        self.event_handler = EventHandler(self.qbt_client, self.file_ops, self.config)

        # 任务管理器
        self.task_manager = TaskManager(
            event_handler=self.event_handler,
            qbt_client=self.qbt_client,
            config=self.config,
        )

        # 停滞种子监控
        self.stalled_monitor = StalledSeedMonitor(self.qbt_client, self.config)
        self.stalled_monitor_thread = None

    def start(self):
        """启动监控器"""
        self.running = True
        self.already_stopped = False  # 重置停止标志
        self._setup_signal_handlers()

        # 等待qBittorrent启动并连接
        self.qbt_client.wait_for_qbit()

        self.logger.info("qBittorrent监控器启动 - 简化标签架构")
        self.logger.info(f"最大工作线程数: {self.task_manager.max_workers}")
        self.logger.info(
            f"添加标签: {self.config.added_tag}, 完成标签: {self.config.completed_tag}"
        )

        # 启动所有组件
        self._start_safely()

        # 主循环
        self._run_main_loop()

    def _setup_signal_handlers(self):
        """设置信号处理器"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def _start_safely(self):
        """安全启动流程"""
        # 1. 启动任务管理器
        self.logger.info("启动任务管理器...")
        self.task_manager.start_all_workers()
        self.logger.info("任务管理器已启动")

        # 2. 启动停滞种子监控
        self._start_stalled_monitoring()

        self.logger.info("系统启动完成，开始处理任务...")

    def _start_stalled_monitoring(self):
        """启动停滞种子监控"""

        def stalled_monitor_loop():
            while self.running:
                try:
                    processed = self.stalled_monitor.scan_stalled_torrents()

                    if self.config.debug_mode:
                        summary = self.stalled_monitor.get_monitoring_summary()
                        if summary["total_tracked"] > 0:
                            self.logger.debug(f"停滞种子监控: {summary}")

                    time.sleep(self.stalled_monitor.check_interval)

                except Exception as e:
                    self.logger.error(f"停滞监控循环错误: {e}")
                    time.sleep(60)

        self.stalled_monitor_thread = threading.Thread(
            target=stalled_monitor_loop, name="stalled_monitor", daemon=True
        )
        self.stalled_monitor_thread.start()
        self.logger.info("停滞种子监控已启动")

    def _run_main_loop(self):
        """运行主循环"""
        try:
            while self.running:
                # 定期重新加载配置
                self.config.load_config()

                # 输出系统状态（调试模式）
                if self.config.debug_mode:
                    status = self.task_manager.get_system_status()
                    self.logger.debug(f"系统状态: {status}")

                time.sleep(self.config.check_interval)

        except KeyboardInterrupt:
            self.logger.info("收到键盘中断信号")
        except Exception as e:
            self.logger.error(f"主循环发生错误: {e}")
        finally:
            # 只有在未停止的情况下才调用stop
            if not self.already_stopped:
                self.stop()
            else:
                self.logger.debug("系统已停止，跳过重复停止操作")

    def _restore_processing_tags(self):
        """恢复处理中的标签到原始状态"""
        try:
            self.logger.info("正在恢复处理中的种子标签...")

            # 获取所有带有processing标签的种子
            processing_torrents = self.qbt_client.get_torrents_by_tag(
                tag=self.config.processing_tag
            )

            restored_count = 0
            for torrent in processing_torrents:
                torrent_hash = torrent.hash
                torrent_name = torrent.name

                # 检查种子是否在任务数据库中
                has_added_task = self.task_manager.task_store.task_exists(
                    torrent_hash, "added"
                )
                has_completed_task = self.task_manager.task_store.task_exists(
                    torrent_hash, "completed"
                )

                if has_added_task:
                    # 如果还有added任务，恢复为added标签
                    self.qbt_client.add_tag(torrent_hash, self.config.added_tag)
                    self.qbt_client.remove_tag(torrent_hash, self.config.processing_tag)
                    self.logger.debug(f"恢复种子 {torrent_name} 为added标签")
                    restored_count += 1
                elif has_completed_task:
                    # 如果还有completed任务，恢复为completed标签
                    self.qbt_client.add_tag(torrent_hash, self.config.completed_tag)
                    self.qbt_client.remove_tag(torrent_hash, self.config.processing_tag)
                    self.logger.debug(f"恢复种子 {torrent_name} 为completed标签")
                    restored_count += 1
                else:
                    # 不在任务数据库中，可能是处理中断，保留processing标签
                    self.logger.debug(
                        f"种子 {torrent_name} 不在任务库中，保留processing标签"
                    )

            if restored_count > 0:
                self.logger.info(f"成功恢复了 {restored_count} 个种子的标签")
            else:
                self.logger.info("没有需要恢复标签的种子")

        except Exception as e:
            self.logger.error(f"恢复处理中标签失败: {e}")

    def stop(self):
        """停止监控器"""
        # 防止重复停止
        if self.already_stopped:
            self.logger.debug("系统已经停止，跳过重复停止操作")
            return

        self.already_stopped = True
        self.running = False

        self.logger.info("开始停止监控器...")

        # 恢复处理中的种子标签
        self._restore_processing_tags()

        # 停止所有工作线程
        self.task_manager.stop_all_workers()
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
