import threading
import time
import logging
from queue import Queue, Empty
from typing import Set
from persistence.task_store import TaskStore


class TaskWorkerManager:
    """任务工作线程管理器"""

    def __init__(self, event_handler, task_store: TaskStore, max_workers: int = 5):
        self.event_handler = event_handler
        self.task_store = task_store
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)

        # 任务队列和线程池
        self.queues = {"added": Queue(), "completed": Queue()}
        self.workers = {"added": set(), "completed": set()}
        self.locks = {"added": threading.Lock(), "completed": threading.Lock()}

        # 线程控制
        self.total_workers = 0
        self.total_lock = threading.Lock()
        self.running = True

    def submit_task(
        self,
        task_type: str,
        torrent_hash: str,
        hash_file_path: str,
        from_startup: bool = False,
    ):
        """提交任务"""
        try:
            if not from_startup:
                # 新任务：先持久化，再清理文件
                self.task_store.save_task(torrent_hash, task_type, hash_file_path)
                self._cleanup_hash_file(hash_file_path)

            self.queues[task_type].put((torrent_hash, hash_file_path))
            self.logger.debug(f"提交{task_type}任务: {torrent_hash}")
            self._start_worker_if_needed(task_type)

        except Exception as e:
            self.logger.error(f"提交{task_type}任务失败: {e}")

    def load_pending_tasks(self):
        """加载待处理任务"""
        self.logger.info("加载待处理任务...")

        for task_type in ["added", "completed"]:
            tasks = self.task_store.get_pending_tasks(task_type)
            for torrent_hash, _, hash_file_path in tasks:
                self.queues[task_type].put((torrent_hash, hash_file_path))
                self.logger.debug(f"加载{task_type}任务: {torrent_hash}")

            self.logger.info(f"加载{task_type}任务: {len(tasks)}个")

    def start_all_workers(self):
        """启动所有工作线程"""
        for task_type in ["added", "completed"]:
            self._start_worker_if_needed(task_type)

    def stop_all_workers(self):
        """停止所有工作线程"""
        self.running = False
        self.logger.info("停止所有工作线程...")

        for task_type, queue in self.queues.items():
            for _ in range(len(self.workers[task_type])):
                queue.put(None)

        for task_type, workers in self.workers.items():
            for worker in list(workers):
                worker.join(timeout=10)

        self.logger.info("所有工作线程已停止")

    def _cleanup_hash_file(self, hash_file_path: str):
        """清理哈希文件"""
        import os

        if os.path.exists(hash_file_path):
            os.remove(hash_file_path)
            self.logger.debug(f"删除哈希文件: {hash_file_path}")

    def _can_start_worker(self) -> bool:
        """检查是否可以启动新线程"""
        with self.total_lock:
            return self.total_workers < self.max_workers

    def _increment_total_workers(self):
        """增加总线程计数"""
        with self.total_lock:
            self.total_workers += 1

    def _decrement_total_workers(self):
        """减少总线程计数"""
        with self.total_lock:
            self.total_workers -= 1

    def _start_worker_if_needed(self, task_type: str):
        """如果需要，启动工作线程"""
        with self.locks[task_type]:
            if (
                not self.queues[task_type].empty()
                and len(self.workers[task_type]) < self.max_workers - 1
                and self._can_start_worker()
            ):

                worker = threading.Thread(
                    target=self._process_tasks,
                    args=(task_type,),
                    name=f"{task_type}_worker_{len(self.workers[task_type])}",
                    daemon=True,
                )
                self.workers[task_type].add(worker)
                self._increment_total_workers()
                worker.start()
                self.logger.debug(
                    f"启动{task_type}工作线程: {len(self.workers[task_type])}个"
                )

    def _process_tasks(self, task_type: str):
        """处理任务的工作线程"""
        thread_name = threading.current_thread().name

        while self.running:
            try:
                event = self.queues[task_type].get(timeout=5)
                if event is None:  # 停止信号
                    break

                torrent_hash, hash_file_path = event
                self.logger.debug(f"{thread_name} 处理{task_type}任务: {torrent_hash}")

                # 处理任务
                success = self._process_single_task(
                    task_type, torrent_hash, hash_file_path
                )

                if success:
                    # 任务完成，从存储中删除
                    self.task_store.delete_task(torrent_hash, task_type)
                else:
                    # 处理失败，重新加入队列
                    self.logger.debug(
                        f"{task_type}任务处理失败，重新加入队列: {torrent_hash}"
                    )
                    self.queues[task_type].put((torrent_hash, hash_file_path))

                self.queues[task_type].task_done()

            except Empty:
                if self.queues[task_type].empty():
                    break
            except Exception as e:
                self.logger.error(f"{thread_name} 处理{task_type}任务时发生错误: {e}")
                time.sleep(1)

        # 清理工作线程
        with self.locks[task_type]:
            self.workers[task_type].discard(threading.current_thread())
            self._decrement_total_workers()
            self.logger.debug(f"{task_type}工作线程停止: {thread_name}")

    def _process_single_task(
        self, task_type: str, torrent_hash: str, hash_file_path: str
    ) -> bool:
        """处理单个任务"""
        try:
            if task_type == "added":
                return self.event_handler.process_torrent_addition(
                    torrent_hash, hash_file_path
                )
            elif task_type == "completed":
                return self.event_handler.process_torrent_completion(
                    torrent_hash, hash_file_path
                )
            return False
        except Exception as e:
            self.logger.error(
                f"处理{task_type}任务时发生未捕获错误 {torrent_hash}: {e}"
            )
            return False
