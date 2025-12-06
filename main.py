"""
qBittorrent监控器主程序
适配新的配置结构
"""

import signal
import sys
import time
import logging
import traceback
from pathlib import Path
from typing import Optional

from config.settings import SimpleConfig
from utils.logging import LogConfig
from core import QBittorrentClient, FileManager, TaskManager
from monitor.stalled_monitor import StalledSeedMonitor


class ApplicationError(Exception):
    """应用程序异常基类"""

    pass


class QBittorrentMonitor:
    """qBittorrent监控器主类"""

    def __init__(self, config_file: str = "config.json"):
        """
        初始化监控器

        Args:
            config_file: 配置文件路径
        """
        self.config_file = config_file
        self.config: Optional[SimpleConfig] = None
        self.logger: Optional[logging.Logger] = None

        # 组件
        self.client: Optional[QBittorrentClient] = None
        self.file_manager: Optional[FileManager] = None
        self.task_manager: Optional[TaskManager] = None
        self.stalled_monitor: Optional[StalledSeedMonitor] = None

        # 运行状态
        self.running = False
        self.initialized = False

        # 初始化
        self._initialize()

    def _initialize(self):
        """初始化应用程序"""
        try:
            # 加载配置（使用SimpleConfig保持兼容）
            self.config = SimpleConfig(self.config_file)

            # 设置日志
            self.logger = LogConfig.setup_logging(
                log_file=self.config.log_file, debug_mode=self.config.debug_mode
            )

            # 确保必要的目录存在
            self._ensure_directories()

            # 输出配置摘要
            self._log_config_summary()

            # 初始化组件
            self._initialize_components()

            self.initialized = True
            self.logger.info("监控器初始化完成")

        except Exception as e:
            self._handle_initialization_error(e)

    def _ensure_directories(self):
        """确保必要的目录存在"""
        # 确保日志目录存在
        log_path = Path(self.config.log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # 确保数据库目录存在
        db_path = Path(self.config.db_file)
        db_path.parent.mkdir(parents=True, exist_ok=True)

    def _log_config_summary(self):
        """输出配置摘要"""
        summary_lines = [
            "=" * 60,
            "qBittorrent监控器配置摘要",
            "=" * 60,
            f"连接地址: {self.config.host}:{self.config.port}",
            f"标签设置: 添加={self.config.added_tag}, 完成={self.config.completed_tag}",
            f"工作线程: {self.config.max_workers}个",
            f"扫描间隔: {self.config.poll_interval}秒",
            f"文件模式: {len(self.config.file_patterns)}个",
            f"目录模式: {len(self.config.folder_patterns)}个",
            f"停滞监控: {self.config.min_stalled_minutes}分钟阈值",
            f"数据库文件: {self.config.db_file}",
            f"调试模式: {self.config.debug_mode}",
            "=" * 60,
        ]

        for line in summary_lines:
            self.logger.info(line)

    def _initialize_components(self):
        """初始化所有组件"""
        # qBittorrent客户端
        self.client = QBittorrentClient(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
        )

        # 文件管理器
        self.file_manager = FileManager(self.config)

        # 任务管理器（传入db_file配置）
        self.task_manager = TaskManager(
            client=self.client, file_manager=self.file_manager, config=self.config
        )

        # 停滞种子监控器
        self.stalled_monitor = StalledSeedMonitor(self.client, self.config)

    def _handle_initialization_error(self, error: Exception):
        """处理初始化错误"""
        error_msg = f"初始化失败: {error}"

        if self.logger:
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
        else:
            print(error_msg)
            traceback.print_exc()

        raise ApplicationError(error_msg) from error

    def start(self):
        """启动监控器"""
        if not self.initialized:
            raise ApplicationError("监控器未初始化")

        if self.running:
            self.logger.warning("监控器已经在运行")
            return

        try:
            self.running = True

            # 设置信号处理
            self._setup_signal_handlers()

            # 连接qBittorrent
            self._connect_qbittorrent()

            # 恢复处理中的种子
            self._recover_processing_torrents()

            # 启动组件
            self._start_components()

            # 运行主循环
            self._run_main_loop()

        except KeyboardInterrupt:
            self.logger.info("收到键盘中断信号")
        except Exception as e:
            self.logger.error(f"启动失败: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self.stop()

    def _setup_signal_handlers(self):
        """设置信号处理器"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _connect_qbittorrent(self):
        """连接qBittorrent"""
        self.logger.info("正在连接qBittorrent...")
        self.client.wait_for_connection()
        self.logger.info(
            f"成功连接到qBittorrent，版本: {self.client.get_app_version()}"
        )

    def _recover_processing_torrents(self):
        """恢复处理中的种子标签"""
        self.logger.info("检查处理中的种子...")

        try:
            processing_torrents = self.client.get_torrents_by_tag(
                self.config.processing_tag
            )

            if not processing_torrents:
                self.logger.info("没有发现处理中的种子")
                return

            self.logger.info(f"发现 {len(processing_torrents)} 个处理中的种子")

            recovered_count = 0
            for torrent in processing_torrents:
                try:
                    if self._recover_single_torrent(torrent):
                        recovered_count += 1
                except Exception as e:
                    self.logger.error(f"恢复种子 {torrent.name} 失败: {e}")

            self.logger.info(f"成功恢复了 {recovered_count} 个种子的标签")

        except Exception as e:
            self.logger.error(f"恢复处理中种子失败: {e}")

    def _recover_single_torrent(self, torrent) -> bool:
        """恢复单个种子"""
        # 检查任务数据库
        has_added_task = self.task_manager.task_store.task_exists(torrent.hash, "added")
        has_completed_task = self.task_manager.task_store.task_exists(
            torrent.hash, "completed"
        )

        if has_added_task:
            self.client.add_tag(torrent.hash, self.config.added_tag)
            self.client.remove_tag(torrent.hash, self.config.processing_tag)
            self.logger.info(f"恢复种子为 added 标签: {torrent.name}")
            return True

        elif has_completed_task:
            self.client.add_tag(torrent.hash, self.config.completed_tag)
            self.client.remove_tag(torrent.hash, self.config.processing_tag)
            self.logger.info(f"恢复种子为 completed 标签: {torrent.name}")
            return True

        else:
            # 根据种子状态决定
            if torrent.progress >= 1.0:
                self.client.add_tag(torrent.hash, self.config.completed_tag)
                self.client.remove_tag(torrent.hash, self.config.processing_tag)
                self.logger.info(f"恢复已完成种子为 completed 标签: {torrent.name}")
            else:
                self.client.add_tag(torrent.hash, self.config.added_tag)
                self.client.remove_tag(torrent.hash, self.config.processing_tag)
                self.logger.info(f"恢复未完成种子为 added 标签: {torrent.name}")
            return True

        return False

    def _start_components(self):
        """启动所有组件"""
        # 启动任务管理器
        self.task_manager.start()
        self.logger.info("任务管理器已启动")

        # 启动停滞监控器
        self.stalled_monitor.start()
        self.logger.info("停滞种子监控器已启动")

        self.logger.info("所有组件已启动，开始运行...")

    def _run_main_loop(self):
        """运行主循环"""
        last_status_time = time.time()
        status_interval = 60  # 状态输出间隔（秒）

        self.logger.info("进入主循环")

        while self.running:
            try:
                current_time = time.time()

                # 定期输出状态
                if current_time - last_status_time > status_interval:
                    last_status_time = current_time
                    self._log_system_status()

                # 等待
                time.sleep(self.config.check_interval)

            except Exception as e:
                self.logger.error(f"主循环错误: {e}")
                time.sleep(10)

    def _log_system_status(self):
        """记录系统状态"""
        if not self.config.debug_mode:
            return

        try:
            # 获取任务管理器状态
            task_status = self.task_manager.get_status()

            # 获取停滞监控摘要
            stalled_summary = self.stalled_monitor.get_monitoring_summary()

            status_info = {
                "tasks": task_status,
                "stalled_monitor": stalled_summary,
                "running": self.running,
            }

            self.logger.debug(f"系统状态: {status_info}")

        except Exception as e:
            self.logger.error(f"获取系统状态失败: {e}")

    def stop(self):
        """停止监控器"""
        if not self.running:
            return

        self.running = False
        self.logger.info("正在停止监控器...")

        try:
            # 恢复处理中的种子标签
            self._recover_processing_torrents_on_stop()

            # 停止组件
            self._stop_components()

            # 输出最终状态
            self._log_final_status()

        except Exception as e:
            self.logger.error(f"停止过程中发生错误: {e}")

        self.logger.info("监控器已停止")

    def _recover_processing_torrents_on_stop(self):
        """停止时恢复处理中的种子"""
        try:
            processing_torrents = self.client.get_torrents_by_tag(
                self.config.processing_tag
            )

            if not processing_torrents:
                return

            self.logger.info(f"停止时恢复 {len(processing_torrents)} 个处理中的种子")

            for torrent in processing_torrents:
                try:
                    if torrent.progress >= 1.0:
                        self.client.add_tag(torrent.hash, self.config.completed_tag)
                    else:
                        self.client.add_tag(torrent.hash, self.config.added_tag)

                    self.client.remove_tag(torrent.hash, self.config.processing_tag)

                except Exception as e:
                    self.logger.error(f"恢复种子 {torrent.name} 标签失败: {e}")

        except Exception as e:
            self.logger.error(f"停止时恢复种子标签失败: {e}")

    def _stop_components(self):
        """停止所有组件"""
        # 停止停滞监控器
        if self.stalled_monitor:
            self.stalled_monitor.stop()

        # 停止任务管理器
        if self.task_manager:
            self.task_manager.stop()

    def _log_final_status(self):
        """记录最终状态"""
        try:
            task_status = self.task_manager.get_status()
            self.logger.info(f"最终任务状态: {task_status}")
        except Exception as e:
            self.logger.error(f"获取最终状态失败: {e}")

    def _signal_handler(self, signum, frame):
        """信号处理函数"""
        signal_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
        self.logger.info(f"收到停止信号: {signal_name}")
        self.stop()
        sys.exit(0)


def main():
    """主函数入口"""
    try:
        # 创建并启动监控器
        monitor = QBittorrentMonitor()
        monitor.start()

    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)

    except ApplicationError as e:
        print(f"应用程序错误: {e}")
        sys.exit(1)

    except Exception as e:
        print(f"未处理的异常: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
