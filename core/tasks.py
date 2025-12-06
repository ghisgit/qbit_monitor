"""
任务管理器
适配新的配置结构
"""

import threading
import time
import logging
import os
from typing import List, Tuple
from .client import QBittorrentClient
from .files import FileManager
from .storage import TaskStore, Task


class TaskManager:
    """任务管理器"""

    def __init__(self, client: QBittorrentClient, file_manager: FileManager, config):
        """
        初始化任务管理器

        Args:
            client: qBittorrent客户端
            file_manager: 文件管理器
            config: 配置对象（SimpleConfig）
        """
        self.client = client
        self.file_manager = file_manager
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 初始化任务存储，使用配置中的db_file
        self.task_store = TaskStore(self.config.db_file)

        # 重置卡住的任务
        self.task_store.reset_stuck_tasks(0.5)

        # 线程管理
        self.running = True
        self.workers = []
        self.scanner_thread = None

        self.logger.info(f"任务管理器初始化完成，数据库: {self.config.db_file}")

    def start(self):
        """启动任务管理器"""
        try:
            # 启动扫描线程
            self._start_scanner()

            # 启动处理线程
            self._start_workers()

            self.logger.info(
                f"任务管理器已启动: "
                f"1个扫描线程, {self.config.max_workers}个工作线程"
            )

        except Exception as e:
            self.logger.error(f"启动任务管理器失败: {e}")
            raise

    def _start_scanner(self):
        """启动标签扫描器"""
        self.scanner_thread = threading.Thread(
            target=self._scan_loop, name="tag_scanner", daemon=True
        )
        self.scanner_thread.start()

    def _start_workers(self):
        """启动工作线程"""
        for i in range(self.config.max_workers):
            worker = threading.Thread(
                target=self._worker_loop, name=f"task_worker_{i}", daemon=True
            )
            self.workers.append(worker)
            worker.start()

    def _scan_loop(self):
        """标签扫描循环"""
        error_count = 0

        while self.running:
            try:
                # 扫描任务
                self._scan_added_tasks()
                self._scan_completed_tasks()

                # 定期清理旧任务
                self._periodic_cleanup()

                # 重置错误计数
                error_count = 0

                # 等待下一次扫描
                time.sleep(self.config.poll_interval)

            except Exception as e:
                error_count += 1
                self._handle_scan_error(e, error_count)

    def _scan_added_tasks(self):
        """扫描添加标签的任务"""
        try:
            added_torrents = self.client.get_torrents_by_tag(self.config.added_tag)

            for torrent in added_torrents:
                if not self.running:
                    break

                self._process_added_torrent(torrent)

        except Exception as e:
            self.logger.error(f"扫描添加任务失败: {e}")

    def _scan_completed_tasks(self):
        """扫描完成标签的任务"""
        try:
            completed_torrents = self.client.get_torrents_by_tag(
                self.config.completed_tag
            )

            for torrent in completed_torrents:
                if not self.running:
                    break

                self._process_completed_torrent(torrent)

        except Exception as e:
            self.logger.error(f"扫描完成任务失败: {e}")

    def _process_added_torrent(self, torrent):
        """处理新发现的added种子"""
        if not self.task_store.task_exists(torrent.hash, "added"):
            if self.task_store.save_task(torrent.hash, "added"):
                self.logger.info(f"发现新任务: {torrent.name} (状态: {torrent.state})")

                # 更新标签
                self.client.add_tag(torrent.hash, self.config.processing_tag)
                self.client.remove_tag(torrent.hash, self.config.added_tag)

    def _process_completed_torrent(self, torrent):
        """处理新发现的completed种子"""
        if not self.task_store.task_exists(torrent.hash, "completed"):
            if self.task_store.save_task(torrent.hash, "completed"):
                self.logger.info(f"发现完成种子: {torrent.name}")

                # 更新标签
                self.client.add_tag(torrent.hash, self.config.processing_tag)
                self.client.remove_tag(torrent.hash, self.config.completed_tag)

    def _periodic_cleanup(self):
        """定期清理"""
        if int(time.time()) % 3600 < self.config.poll_interval:
            self.task_store.cleanup_old_tasks(1)

    def _handle_scan_error(self, error: Exception, error_count: int):
        """处理扫描错误"""
        self.logger.error(f"扫描失败 (错误 {error_count}): {error}")

        if error_count >= 10:
            self.logger.error("扫描线程错误过多，暂停30秒")
            time.sleep(30)
        else:
            time.sleep(10)

    def _worker_loop(self):
        """工作线程循环"""
        thread_name = threading.current_thread().name

        while self.running:
            try:
                tasks = self.task_store.get_pending_tasks(limit=5)

                if tasks:
                    self.logger.debug(f"{thread_name} 获取到 {len(tasks)} 个任务")

                    for task in tasks:
                        if not self.running:
                            break

                        self._process_task(task)
                else:
                    # 没有任务时休眠
                    time.sleep(2)

            except Exception as e:
                self.logger.error(f"{thread_name} 工作循环错误: {e}")
                time.sleep(10)

    def _process_task(self, task: Task):
        """处理单个任务"""
        try:
            torrent = self.client.get_torrent_by_hash(task.torrent_hash)

            if not torrent:
                self._handle_missing_torrent(task)
                return

            if task.task_type == "added":
                success = self._process_added_task(torrent)
            else:  # completed
                success = self._process_completed_task(torrent)

            if success:
                self._complete_task_success(task, torrent)
            else:
                self._recover_task_on_failure(task, torrent)

        except Exception as e:
            self.logger.error(f"处理任务 {task.torrent_hash} 失败: {e}")
            self._recover_task_on_error(task)

    def _handle_missing_torrent(self, task: Task):
        """处理种子不存在的情况"""
        self.logger.debug(f"种子不存在，删除任务: {task.torrent_hash}")
        self.task_store.complete_task(task.torrent_hash, task.task_type)
        self.client.remove_tag(task.torrent_hash, self.config.processing_tag)

    def _process_added_task(self, torrent) -> bool:
        """处理新添加的种子"""
        try:
            files = self.client.get_torrent_files(torrent.hash)

            if not files:
                self.logger.warning(f"种子没有文件列表: {torrent.name}")
                return True

            files_to_disable = self._get_files_to_disable(files)

            if files_to_disable:
                success = self.client.set_file_priority(
                    torrent.hash, files_to_disable, 0
                )

                if success:
                    self.logger.info(
                        f"禁用 {len(files_to_disable)} 个文件: {torrent.name}"
                    )

                return success

            return True

        except Exception as e:
            self.logger.error(f"处理添加任务失败 {torrent.name}: {e}")
            return False

    def _get_files_to_disable(self, files: List[dict]) -> List[int]:
        """获取需要禁用的文件索引"""
        files_to_disable = []

        for file in files:
            if file["priority"] != 0 and self.file_manager.should_disable_file(
                file["name"]
            ):
                files_to_disable.append(file["index"])

        return files_to_disable

    def _process_completed_task(self, torrent) -> bool:
        """处理已完成的种子"""
        try:
            content_path = self._get_torrent_content_path(torrent)

            if not content_path or not os.path.exists(content_path):
                self.logger.warning(f"内容路径不存在: {content_path}")
                return True

            # 清理文件
            deleted_files, deleted_folders = self.file_manager.clean_directory(
                content_path
            )

            if deleted_files > 0 or deleted_folders > 0:
                self.logger.info(
                    f"清理完成: {torrent.name}, "
                    f"删除 {deleted_files} 个文件, {deleted_folders} 个目录"
                )

            return True

        except Exception as e:
            self.logger.error(f"处理完成种子失败 {torrent.name}: {e}")
            return False

    def _get_torrent_content_path(self, torrent) -> str:
        """获取种子的内容路径"""
        content_path = getattr(torrent, "content_path", "")

        if not content_path:
            save_path = getattr(torrent, "save_path", "")
            name = getattr(torrent, "name", "")

            if save_path and name:
                content_path = os.path.join(save_path, name)

        return content_path

    def _complete_task_success(self, task: Task, torrent):
        """任务成功完成"""
        self.task_store.complete_task(task.torrent_hash, task.task_type)
        self.client.remove_tag(torrent.hash, self.config.processing_tag)
        self.logger.info(f"成功处理任务: {torrent.name}")

    def _recover_task_on_failure(self, task: Task, torrent):
        """任务失败时恢复"""
        self._recover_torrent_tags(torrent)
        self.logger.warning(f"处理失败，任务将重试: {torrent.name}")

    def _recover_task_on_error(self, task: Task):
        """任务异常时恢复"""
        try:
            self.client.remove_tag(task.torrent_hash, self.config.processing_tag)
        except Exception:
            pass

    def _recover_torrent_tags(self, torrent):
        """恢复种子标签"""
        try:
            if torrent.progress >= 1.0:
                self.client.add_tag(torrent.hash, self.config.completed_tag)
            else:
                self.client.add_tag(torrent.hash, self.config.added_tag)

            self.client.remove_tag(torrent.hash, self.config.processing_tag)

        except Exception as e:
            self.logger.error(f"恢复种子标签失败 {torrent.name}: {e}")

    def stop(self):
        """停止任务管理器"""
        self.running = False

        # 等待线程结束
        self._wait_for_threads()

        # 关闭数据库
        self.task_store.close()

        self.logger.info("任务管理器已停止")

    def _wait_for_threads(self):
        """等待所有线程结束"""
        if self.scanner_thread and self.scanner_thread.is_alive():
            self.scanner_thread.join(timeout=5)

        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=5)

    def get_status(self) -> dict:
        """
        获取系统状态

        Returns:
            dict: 状态信息
        """
        try:
            stats = self.task_store.get_statistics()

            return {
                "task_stats": stats,
                "running": self.running,
                "active_workers": len([w for w in self.workers if w.is_alive()]),
                "scanner_alive": (
                    self.scanner_thread.is_alive() if self.scanner_thread else False
                ),
            }

        except Exception as e:
            return {"error": str(e)}
