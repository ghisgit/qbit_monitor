import os
import threading
import time
import logging
import uuid
from typing import List
from persistence.task_store import TaskStore, Task
from circuit_breaker.breaker import CircuitBreaker
from circuit_breaker.health_checker import QBittorrentHealthChecker
from retry_engine.infinite_retry import InfiniteRetryEngine


class ResilientTaskManager:
    """弹性任务管理器 - 支持无限重试和熔断保护"""

    def __init__(
        self, event_handler, task_store: TaskStore, qbt_client, file_ops, config
    ):
        self.event_handler = event_handler
        self.task_store = task_store
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 核心组件
        self.circuit_breaker = CircuitBreaker(task_store)
        self.health_checker = QBittorrentHealthChecker(qbt_client)
        self.retry_engine = InfiniteRetryEngine()

        # 工作线程管理
        self.workers = []
        self.running = True

        # 性能调节
        self.batch_size = getattr(config, "batch_size", 5)
        self.poll_interval = getattr(config, "poll_interval", 10)
        self.max_workers = getattr(config, "max_workers", 3)

        # 熔断器状态跟踪
        self.last_breaker_check = time.time()
        self.consecutive_breaker_failures = 0

    def submit_task(
        self, task_type: str, torrent_hash: str, hash_file_path: str
    ) -> bool:
        """提交新任务 - 原子性操作"""
        try:
            # 生成唯一任务ID
            task_uuid = str(uuid.uuid4())

            # 创建任务对象
            current_time = time.time()
            task = Task(
                task_uuid=task_uuid,
                torrent_hash=torrent_hash,
                task_type=task_type,
                status="pending",
                hash_file_path=hash_file_path,
                created_time=current_time,
                updated_time=current_time,
            )

            # 保存到数据库
            success = self.task_store.save_task(task)

            if success:
                self.logger.info(f"任务提交成功: {task_type} - {torrent_hash}")

                # 只有数据库保存成功后才清理文件
                self._cleanup_hash_file(hash_file_path)
                return True
            else:
                self.logger.error(f"任务提交失败: {task_type} - {torrent_hash}")
                return False

        except Exception as e:
            self.logger.error(f"任务提交异常: {torrent_hash}, 错误: {e}")
            return False

    def start_all_workers(self):
        """启动所有工作线程"""
        for i in range(self.max_workers):
            worker = threading.Thread(
                target=self._database_polling_worker,
                name=f"task_worker_{i}",
                daemon=False,
            )
            self.workers.append(worker)
            worker.start()

        self.logger.info(f"启动了 {self.max_workers} 个数据库轮询工作线程")

    def _database_polling_worker(self):
        """数据库轮询工作线程 - 修复熔断器逻辑"""
        thread_name = threading.current_thread().name
        self.logger.debug(f"{thread_name} 开始运行")

        while self.running:
            try:
                # 检查系统健康状态
                if self.health_checker.should_pause_processing():
                    self.logger.warning(f"{thread_name} 因系统不健康暂停处理")
                    time.sleep(30)
                    continue

                # 检查熔断器状态 - 真正阻止执行
                if not self.circuit_breaker.can_execute("qbit_api"):
                    breaker_status = self.circuit_breaker.get_breaker_status("qbit_api")
                    self.logger.warning(
                        f"{thread_name} 因熔断器开启暂停处理。状态: {breaker_status['state']}, "
                        f"失败次数: {breaker_status['failure_count']}"
                    )
                    time.sleep(10)
                    continue

                # 根据健康状态调整批处理大小
                speed_factor = self.health_checker.get_processing_speed_factor()
                adjusted_batch_size = max(1, int(self.batch_size * speed_factor))

                # 获取待处理任务
                tasks = self.task_store.get_eligible_tasks(limit=adjusted_batch_size)

                if tasks:
                    self.logger.debug(f"{thread_name} 获取到 {len(tasks)} 个任务")

                    # 处理任务
                    for task in tasks:
                        if not self.running:
                            break
                        self._process_single_task(task)

                    # 快速处理下一批任务
                    continue
                else:
                    # 没有任务，根据系统负载调整休眠时间
                    sleep_time = self.poll_interval
                    if speed_factor < 0.5:
                        sleep_time *= 2  # 系统负载高时延长休眠

                    time.sleep(sleep_time)

            except Exception as e:
                self.logger.error(f"{thread_name} 轮询处理失败: {e}")
                self.circuit_breaker.record_failure("qbit_api")
                time.sleep(self.poll_interval * 2)

        self.logger.debug(f"{thread_name} 停止运行")

    def _process_single_task(self, task: Task):
        """处理单个任务 - 增强熔断器记录"""
        try:
            # 再次检查熔断器状态（防止状态在获取任务后变化）
            if not self.circuit_breaker.can_execute("qbit_api"):
                self.logger.warning(f"任务 {task.torrent_hash} 因熔断器开启被跳过")
                return

            # 直接执行任务处理
            if task.task_type == "added":
                result = self.event_handler.process_torrent_addition(
                    task.torrent_hash, task.hash_file_path
                )
            else:  # completed
                result = self.event_handler.process_torrent_completion(
                    task.torrent_hash, task.hash_file_path
                )

            # 处理结果 - 正确记录熔断器状态
            if result == "success":
                self._handle_success(task)
                self.circuit_breaker.record_success("qbit_api")  # 明确记录成功
            else:
                # 任何失败都安排重试并记录熔断器失败
                self._handle_retry(
                    task, result if result != "retry_later" else "unknown_error"
                )

                # 只有真正的系统错误才记录熔断器失败
                if result in ["qbit_api_error", "network_error", "metadata_not_ready"]:
                    self.circuit_breaker.record_failure("qbit_api")
                else:
                    # 业务逻辑错误不记录熔断器失败
                    self.logger.debug(f"业务逻辑错误，不记录熔断器失败: {result}")

        except Exception as e:
            self.logger.error(f"任务处理异常: {task.torrent_hash}, 错误: {e}")
            self._handle_retry(task, f"processing_exception:{str(e)}")
            self.circuit_breaker.record_failure("qbit_api")  # 异常时记录失败

    def _handle_success(self, task: Task):
        """处理成功结果"""
        try:
            success = self.task_store.update_task_after_processing(
                task_uuid=task.task_uuid, success=True
            )

            if success:
                self.logger.info(f"任务处理成功: {task.torrent_hash}")
                # 注意：熔断器成功记录已经在 _process_single_task 中处理
            else:
                self.logger.error(f"更新任务状态失败: {task.torrent_hash}")

        except Exception as e:
            self.logger.error(f"处理成功结果失败: {task.torrent_hash}, 错误: {e}")

    def _handle_retry(self, task: Task, failure_reason: str):
        """处理重试结果"""
        try:
            # 计算下次重试时间
            next_retry_time = self.retry_engine.calculate_next_retry(
                task, failure_reason
            )

            if next_retry_time is None:
                # 不应该继续重试，归档任务
                self.task_store.archive_task(
                    task.task_uuid, f"max_retries_reached:{failure_reason}"
                )
                self.logger.warning(f"任务达到最大重试次数: {task.torrent_hash}")
            else:
                # 安排重试
                success = self.task_store.update_task_after_processing(
                    task_uuid=task.task_uuid,
                    success=False,
                    failure_reason=failure_reason,
                    error_message=failure_reason,
                    next_retry_time=next_retry_time,
                )

                if success:
                    self.logger.debug(
                        f"任务安排重试: {task.torrent_hash}, 下次重试: {next_retry_time}"
                    )
                else:
                    self.logger.error(f"安排重试失败: {task.torrent_hash}")

        except Exception as e:
            self.logger.error(f"处理重试结果失败: {task.torrent_hash}, 错误: {e}")

    def _cleanup_hash_file(self, hash_file_path: str):
        """清理哈希文件"""
        try:
            if os.path.exists(hash_file_path):
                os.remove(hash_file_path)
                self.logger.debug(f"已删除哈希文件: {hash_file_path}")
        except Exception as e:
            self.logger.warning(f"删除哈希文件失败: {hash_file_path}, 错误: {e}")

    def stop_all_workers(self):
        """停止所有工作线程"""
        self.running = False
        self.logger.info("停止所有工作线程...")

        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=10)

        self.logger.info("所有工作线程已停止")

    def get_system_status(self) -> dict:
        """获取系统状态 - 包含熔断器信息"""
        health_status = self.health_checker.get_system_load_status()
        breaker_status = self.circuit_breaker.get_breaker_status("qbit_api")

        return {
            "health_status": health_status,
            "circuit_breaker": breaker_status,
            "active_workers": len([w for w in self.workers if w.is_alive()]),
            "running": self.running,
        }
