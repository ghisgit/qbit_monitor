"""
停滞种子监控模块
适配新的配置结构
"""

import threading
import time
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict


@dataclass
class StalledSeedInfo:
    """停滞种子信息"""

    torrent_hash: str
    name: str
    progress: float
    state: str
    tracked_since: float
    priority_downgraded: bool = False

    def to_dict(self) -> Dict:
        """转换为字典"""
        return asdict(self)


class StalledSeedMonitor:
    """停滞种子监控器"""

    def __init__(self, client, config):
        """
        初始化监控器

        Args:
            client: qBittorrent客户端
            config: 配置对象（SimpleConfig）
        """
        self.client = client
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 跟踪中的种子
        self.tracked_seeds: Dict[str, StalledSeedInfo] = {}

        # 监控状态
        self.running = False
        self.monitor_thread = None

        self.logger.info("停滞种子监控器初始化完成")

    def start(self):
        """启动监控器"""
        if self.running:
            self.logger.warning("监控器已经在运行")
            return

        self.running = True
        self.monitor_thread = self._create_monitor_thread()
        self.monitor_thread.start()

        self.logger.info("停滞种子监控器已启动")

    def stop(self):
        """停止监控器"""
        if not self.running:
            return

        self.running = False

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)

        self.logger.info("停滞种子监控器已停止")

    def _create_monitor_thread(self) -> threading.Thread:
        """创建监控线程"""
        return threading.Thread(
            target=self._monitor_loop, name="stalled_monitor", daemon=True
        )

    def _monitor_loop(self):
        """监控循环"""
        error_count = 0
        max_errors = 5

        while self.running:
            try:
                # 扫描并处理停滞种子
                processed = self.scan_and_process()

                # 输出调试信息
                if self.config.debug_mode and processed:
                    self.logger.debug(f"处理了 {len(processed)} 个停滞种子")

                # 重置错误计数
                error_count = 0

                # 等待下一次扫描
                time.sleep(self.config.stalled_check_interval)

            except Exception as e:
                error_count += 1
                self._handle_monitor_error(e, error_count, max_errors)

    def _handle_monitor_error(
        self, error: Exception, error_count: int, max_errors: int
    ):
        """处理监控错误"""
        self.logger.error(f"停滞监控错误 (错误 {error_count}/{max_errors}): {error}")

        if error_count >= max_errors:
            self.logger.error("停滞监控错误过多，暂停60秒")
            time.sleep(60)
        else:
            time.sleep(30)

    def scan_and_process(self) -> List[Dict]:
        """
        扫描并处理停滞种子

        Returns:
            List[Dict]: 已处理的种子信息列表
        """
        try:
            stalled_torrents = self._get_stalled_torrents()
            processed_seeds = []

            current_time = time.time()

            for torrent in stalled_torrents:
                if not self.running:
                    break

                seed_info = self._process_stalled_torrent(torrent, current_time)
                if seed_info:
                    processed_seeds.append(seed_info.to_dict())

            # 清理已恢复的种子
            self._cleanup_recovered_seeds(stalled_torrents)

            return processed_seeds

        except Exception as e:
            self.logger.error(f"扫描停滞种子失败: {e}")
            return []

    def _get_stalled_torrents(self) -> List:
        """
        获取停滞种子列表

        Returns:
            List: 停滞种子列表
        """
        try:
            downloading = self.client.client.torrents_info(status_filter="downloading")

            stalled_torrents = [
                torrent
                for torrent in downloading
                if torrent.state == "stalledDL"
                and torrent.progress < self.config.progress_threshold
            ]

            if stalled_torrents and self.config.debug_mode:
                self.logger.debug(f"发现 {len(stalled_torrents)} 个停滞种子")

            return stalled_torrents

        except Exception as e:
            self.logger.error(f"获取停滞种子列表失败: {e}")
            return []

    def _process_stalled_torrent(
        self, torrent, current_time: float
    ) -> Optional[StalledSeedInfo]:
        """
        处理单个停滞种子

        Args:
            torrent: 种子对象
            current_time: 当前时间戳

        Returns:
            Optional[StalledSeedInfo]: 处理后的种子信息，如果不需要处理则返回None
        """
        torrent_hash = torrent.hash

        # 检查是否应该处理（进度未完成）
        if torrent.progress >= self.config.progress_threshold:
            return None

        # 获取或创建跟踪信息
        seed_info = self._get_or_create_seed_info(torrent, current_time)

        # 检查进度变化
        if self._has_progress_changed(seed_info, torrent.progress):
            self._reset_tracking(
                seed_info, torrent.progress, torrent.state, current_time
            )
            return None

        # 更新状态
        seed_info.progress = torrent.progress
        seed_info.state = torrent.state

        # 检查是否需要降低优先级
        if self._should_downgrade_priority(seed_info, current_time):
            if self._downgrade_torrent_priority(seed_info):
                seed_info.priority_downgraded = True
                return seed_info

        return None

    def _get_or_create_seed_info(self, torrent, current_time: float) -> StalledSeedInfo:
        """获取或创建种子跟踪信息"""
        torrent_hash = torrent.hash

        if torrent_hash in self.tracked_seeds:
            return self.tracked_seeds[torrent_hash]

        # 创建新的跟踪信息
        seed_info = StalledSeedInfo(
            torrent_hash=torrent_hash,
            name=torrent.name,
            progress=torrent.progress,
            state=torrent.state,
            tracked_since=current_time,
        )

        self.tracked_seeds[torrent_hash] = seed_info
        self.logger.info(
            f"开始跟踪停滞种子: {torrent.name} (进度: {torrent.progress:.1%})"
        )

        return seed_info

    def _has_progress_changed(
        self, seed_info: StalledSeedInfo, current_progress: float
    ) -> bool:
        """检查进度是否有变化"""
        return abs(current_progress - seed_info.progress) > 0.001

    def _reset_tracking(
        self,
        seed_info: StalledSeedInfo,
        progress: float,
        state: str,
        current_time: float,
    ):
        """重置跟踪信息"""
        seed_info.tracked_since = current_time
        seed_info.progress = progress
        seed_info.state = state

        self.logger.info(
            f"种子进度有变化，重置跟踪: {seed_info.name} (进度: {progress:.1%})"
        )

    def _should_downgrade_priority(
        self, seed_info: StalledSeedInfo, current_time: float
    ) -> bool:
        """检查是否需要降低优先级"""
        if seed_info.priority_downgraded:
            return False

        stalled_minutes = (current_time - seed_info.tracked_since) / 60
        return stalled_minutes >= self.config.min_stalled_minutes

    def _downgrade_torrent_priority(self, seed_info: StalledSeedInfo) -> bool:
        """降低种子优先级"""
        try:
            self.client.client.torrents_bottom_priority(
                torrent_hashes=seed_info.torrent_hash
            )

            stalled_minutes = (time.time() - seed_info.tracked_since) / 60

            self.logger.warning(
                f"停滞种子优先级已调低: {seed_info.name} "
                f"(进度: {seed_info.progress:.1%}, 状态: {seed_info.state}, "
                f"停滞: {stalled_minutes:.1f}分钟)"
            )

            return True

        except Exception as e:
            self.logger.error(f"降低种子优先级失败 {seed_info.name}: {e}")
            return False

    def _cleanup_recovered_seeds(self, current_stalled: List):
        """清理已恢复的种子"""
        current_hashes = {torrent.hash for torrent in current_stalled}
        recovered_hashes = set(self.tracked_seeds.keys()) - current_hashes

        for hash_val in recovered_hashes:
            seed_info = self.tracked_seeds[hash_val]

            if seed_info.priority_downgraded:
                self.logger.info(f"种子已恢复活动且优先级已降级: {seed_info.name}")
            else:
                self.logger.info(f"种子已恢复活动: {seed_info.name}")

            del self.tracked_seeds[hash_val]

    def get_monitoring_summary(self) -> Dict:
        """
        获取监控摘要

        Returns:
            Dict: 监控摘要信息
        """
        current_time = time.time()

        summary = {
            "total_tracked": len(self.tracked_seeds),
            "downgraded": len(
                [s for s in self.tracked_seeds.values() if s.priority_downgraded]
            ),
            "tracked_seeds": [],
        }

        # 按停滞时间分组
        time_groups = {"0-30min": 0, "30-60min": 0, "60min+": 0}

        for seed in self.tracked_seeds.values():
            stalled_minutes = (current_time - seed.tracked_since) / 60

            if stalled_minutes < 30:
                time_groups["0-30min"] += 1
            elif stalled_minutes < 60:
                time_groups["30-60min"] += 1
            else:
                time_groups["60min+"] += 1

            # 添加前10个种子的详细信息
            if len(summary["tracked_seeds"]) < 10:
                summary["tracked_seeds"].append(
                    {
                        "name": seed.name,
                        "progress": f"{seed.progress:.1%}",
                        "state": seed.state,
                        "stalled_minutes": f"{stalled_minutes:.1f}",
                        "priority_downgraded": seed.priority_downgraded,
                    }
                )

        summary["stalled_time_distribution"] = time_groups

        return summary

    def is_running(self) -> bool:
        """
        检查监控器是否在运行

        Returns:
            bool: 是否在运行
        """
        return self.running and self.monitor_thread and self.monitor_thread.is_alive()
