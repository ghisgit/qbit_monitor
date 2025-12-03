import threading
import time
import logging
from persistence.task_store import TaskStore, Task
from circuit_breaker.breaker import SimpleCircuitBreaker
from circuit_breaker.health_checker import SimpleHealthChecker
from retry_engine.infinite_retry import RetryEngine


class TaskManager:
    """任务管理器 - 只有任务完成或种子不存在时才删除任务"""

    def __init__(self, event_handler, qbt_client, config):
        self.event_handler = event_handler
        self.qbt_client = qbt_client
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 存储
        self.task_store = TaskStore()

        # 组件
        self.circuit_breaker = SimpleCircuitBreaker(config)
        self.health_checker = SimpleHealthChecker(qbt_client)
        self.retry_engine = RetryEngine(config)

        # 工作线程管理
        self.workers = []
        self.running = True
        self.task_scanner_thread = None
        self.cleanup_thread = None

        # 配置
        self.max_workers = getattr(config, "max_workers", 3)
        self.batch_size = getattr(config, "batch_size", 5)
        self.poll_interval = getattr(config, "poll_interval", 10)

    def start_all_workers(self):
        """启动所有工作线程"""
        # 1. 启动任务扫描器
        self.task_scanner_thread = threading.Thread(
            target=self._tag_based_scanner, name="tag_scanner", daemon=True
        )
        self.task_scanner_thread.start()

        # 2. 启动任务处理线程
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._task_processing_worker,
                name=f"task_worker_{i}",
                daemon=True,
            )
            self.workers.append(worker)
            worker.start()

        # 3. 启动孤儿任务清理线程
        self.cleanup_thread = threading.Thread(
            target=self._orphan_task_cleanup_worker, name="orphan_cleanup", daemon=True
        )
        self.cleanup_thread.start()

        self.logger.info(
            f"启动了标签扫描器、{self.max_workers} 个任务处理线程和孤儿任务清理线程"
        )

    def _tag_based_scanner(self):
        """基于标签的任务扫描器"""
        self.logger.info("标签任务扫描器开始运行")

        while self.running:
            try:
                # 检查系统健康状态
                if self.health_checker.should_pause_processing():
                    self.logger.warning("标签扫描器因系统不健康暂停处理")
                    time.sleep(30)
                    continue

                # 检查熔断器状态
                if not self.circuit_breaker.can_execute():
                    self.logger.debug("标签扫描器因熔断器开启暂停处理")
                    time.sleep(10)
                    continue

                # 扫描添加标签的任务（排除metaDL状态）
                added_torrents = self.qbt_client.get_torrents_by_tag(
                    tag=self.config.added_tag,
                    exclude_states=["metaDL", "queuedDL", "forcedMetaDL"],
                )

                for torrent in added_torrents:
                    if not self.running:
                        break

                    if not self.task_store.task_exists(torrent.hash, "added"):
                        if self.task_store.save_task(torrent.hash, "added"):
                            self.logger.info(
                                f"发现新添加任务: {torrent.hash} - {torrent.name}"
                            )

                            # 更新标签
                            self.qbt_client.add_tag(
                                torrent.hash, self.config.processing_tag
                            )
                            self.qbt_client.remove_tag(
                                torrent.hash, self.config.added_tag
                            )

                # 扫描完成标签的任务
                completed_torrents = self.qbt_client.get_torrents_by_tag(
                    tag=self.config.completed_tag
                )

                for torrent in completed_torrents:
                    if not self.running:
                        break

                    if not self.task_store.task_exists(torrent.hash, "completed"):
                        if self.task_store.save_task(torrent.hash, "completed"):
                            self.logger.info(
                                f"发现新完成任务: {torrent.hash} - {torrent.name}"
                            )

                            # 更新标签
                            self.qbt_client.add_tag(
                                torrent.hash, self.config.processing_tag
                            )
                            self.qbt_client.remove_tag(
                                torrent.hash, self.config.completed_tag
                            )

                # 休眠后继续扫描
                time.sleep(self.poll_interval)

            except Exception as e:
                self.logger.error(f"标签扫描器运行失败: {e}")
                self.circuit_breaker.record_failure()
                time.sleep(30)

        self.logger.info("标签任务扫描器停止运行")

    def _task_processing_worker(self):
        """任务处理工作线程"""
        thread_name = threading.current_thread().name
        self.logger.debug(f"{thread_name} 开始运行")

        while self.running:
            try:
                # 检查系统健康状态
                if self.health_checker.should_pause_processing():
                    self.logger.warning(f"{thread_name} 因系统不健康暂停处理")
                    time.sleep(30)
                    continue

                # 检查熔断器状态
                if not self.circuit_breaker.can_execute():
                    self.logger.debug(f"{thread_name} 因熔断器开启暂停处理")
                    time.sleep(10)
                    continue

                # 获取待处理任务
                tasks = self.task_store.get_pending_tasks(limit=self.batch_size)

                if tasks:
                    self.logger.debug(f"{thread_name} 获取到 {len(tasks)} 个任务")

                    for task in tasks:
                        if not self.running:
                            break
                        self._process_single_task(task)

                    # 快速处理下一批任务
                    continue
                else:
                    # 没有任务，休眠
                    time.sleep(2)

            except Exception as e:
                self.logger.error(f"{thread_name} 处理失败: {e}")
                self.circuit_breaker.record_failure()
                time.sleep(10)

        self.logger.debug(f"{thread_name} 停止运行")

    def _process_single_task(self, task: Task):
        """处理单个任务"""
        thread_name = threading.current_thread().name

        try:
            # 检查熔断器状态
            if not self.circuit_breaker.can_execute():
                self.logger.warning(
                    f"{thread_name}: 任务 {task.torrent_hash} 因熔断器开启被跳过"
                )
                return

            # 执行任务处理
            self.logger.debug(
                f"{thread_name}: 开始处理任务: {task.torrent_hash} - {task.task_type}"
            )

            if task.task_type == "added":
                result = self.event_handler.process_torrent_addition(task.torrent_hash)
            else:  # completed
                result = self.event_handler.process_torrent_completion(
                    task.torrent_hash
                )

            # 处理结果
            if result == "success":
                self._handle_success(task)
                self.circuit_breaker.record_success()

                # 移除处理中标签
                self.qbt_client.remove_tag(
                    task.torrent_hash, self.config.processing_tag
                )

            elif result == "torrent_not_found":
                # 种子不存在，删除任务
                self._handle_torrent_not_found(task)

            else:
                self._handle_failure(task, result)

        except Exception as e:
            self.logger.error(
                f"{thread_name}: 任务处理异常: {task.torrent_hash}, 错误: {e}"
            )
            self._handle_failure(task, f"exception:{str(e)}")
            self.circuit_breaker.record_failure()

    def _handle_success(self, task: Task):
        """处理成功结果 - 任务完成，从数据库删除"""
        try:
            # 从数据库删除任务
            success = self.task_store.complete_task(task.torrent_hash, task.task_type)

            if success:
                self.logger.info(
                    f"任务处理成功并删除: {task.torrent_hash} - {task.task_type}"
                )
            else:
                # 这可能是正常情况，比如任务已被其他线程删除
                self.logger.debug(
                    f"任务删除可能已由其他线程完成: {task.torrent_hash} - {task.task_type}"
                )

        except Exception as e:
            self.logger.error(f"处理成功结果失败: {task.torrent_hash}, 错误: {e}")

    def _handle_torrent_not_found(self, task: Task):
        """处理种子不存在的情况 - 从数据库删除任务"""
        try:
            # 从数据库删除任务
            success = self.task_store.complete_task(task.torrent_hash, task.task_type)

            if success:
                self.logger.info(
                    f"种子不存在，删除任务: {task.torrent_hash} - {task.task_type}"
                )

                # 移除处理中标签（如果还存在）
                self.qbt_client.remove_tag(
                    task.torrent_hash, self.config.processing_tag
                )
            else:
                self.logger.warning(
                    f"种子不存在但删除任务失败（可能已删除）: {task.torrent_hash} - {task.task_type}"
                )

        except Exception as e:
            self.logger.error(f"处理种子不存在结果失败: {task.torrent_hash}, 错误: {e}")

    def _handle_failure(self, task: Task, failure_reason: str):
        """处理失败结果 - 使用重试引擎"""
        try:
            # 使用重试引擎计算下次重试时间
            next_retry_time = self.retry_engine.calculate_next_retry(
                task, failure_reason
            )

            if next_retry_time is None:
                # 达到最大重试次数，但仍然不删除任务
                self.logger.error(
                    f"任务达到最大重试次数但仍保留: {task.torrent_hash} - {failure_reason}"
                )

                # 获取重试策略信息
                retry_info = self.retry_engine.get_retry_info(failure_reason)

                # 安排较长时间后的重试（如1小时）
                self.task_store.schedule_retry(
                    torrent_hash=task.torrent_hash,
                    task_type=task.task_type,
                    next_retry_time=time.time() + 3600,  # 1小时后再试
                    failure_reason=f"max_retries_reached:{failure_reason}",
                )

                self.logger.warning(
                    f"任务 {task.torrent_hash} 达到最大重试次数，"
                    f"策略: {retry_info.get('strategy_name', 'unknown')}, "
                    f"将在1小时后重试"
                )
            else:
                # 安排重试
                success = self.task_store.schedule_retry(
                    torrent_hash=task.torrent_hash,
                    task_type=task.task_type,
                    next_retry_time=next_retry_time,
                    failure_reason=failure_reason,
                )

                if success:
                    # 获取重试策略信息用于日志
                    retry_info = self.retry_engine.get_retry_info(failure_reason)
                    retry_delay = next_retry_time - time.time()

                    self.logger.warning(
                        f"任务安排重试: {task.torrent_hash}, "
                        f"原因: {failure_reason}, "
                        f"策略: {retry_info.get('strategy_name', 'unknown')}, "
                        f"延迟: {retry_delay:.1f}秒, "
                        f"下次重试: {time.strftime('%H:%M:%S', time.localtime(next_retry_time))}"
                    )
                else:
                    self.logger.debug(
                        f"安排重试失败，任务可能已被删除: {task.torrent_hash}"
                    )

            # 只有真正的系统错误才记录熔断器失败
            if any(
                err in failure_reason for err in ["qbit_api_error", "network_error"]
            ):
                self.circuit_breaker.record_failure()

        except Exception as e:
            self.logger.error(f"处理失败结果失败: {task.torrent_hash}, 错误: {e}")

    def _orphan_task_cleanup_worker(self):
        """孤儿任务清理线程 - 清理种子已不存在的任务"""
        self.logger.info("孤儿任务清理线程开始运行")

        while self.running:
            try:
                # 每天清理一次孤儿任务
                self.task_store.cleanup_orphaned_tasks(
                    qbt_client=self.qbt_client, days=1  # 检查1天前更新的任务
                )

                # 休眠24小时
                for _ in range(1440):  # 24小时，每分钟检查一次是否停止
                    if not self.running:
                        break
                    time.sleep(60)

            except Exception as e:
                self.logger.error(f"孤儿任务清理失败: {e}")
                time.sleep(3600)  # 1小时后重试

        self.logger.info("孤儿任务清理线程停止运行")

    def stop_all_workers(self):
        """停止所有工作线程"""
        self.running = False
        self.logger.info("停止所有工作线程...")

        # 等待任务扫描器
        if self.task_scanner_thread and self.task_scanner_thread.is_alive():
            self.task_scanner_thread.join(timeout=10)

        # 等待工作线程
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=10)

        # 等待清理线程
        if self.cleanup_thread and self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=10)

        # 获取最终统计信息
        stats = self.task_store.get_task_stats()
        self.logger.info(f"任务统计: {stats}")

        # 关闭存储
        self.task_store.close()

        self.logger.info("所有工作线程已停止")

    def get_system_status(self) -> dict:
        """获取系统状态"""
        health_status = self.health_checker.get_status()
        breaker_status = self.circuit_breaker.get_status()
        task_stats = self.task_store.get_task_stats()

        return {
            "health": health_status,
            "circuit_breaker": breaker_status,
            "task_stats": task_stats,
            "active_workers": len([w for w in self.workers if w.is_alive()]),
            "running": self.running,
        }
