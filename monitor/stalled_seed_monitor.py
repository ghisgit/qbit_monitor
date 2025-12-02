import time
import logging
from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class StalledSeedInfo:
    """停滞种子信息"""

    torrent_hash: str
    name: str
    progress: float
    state: str
    tracked_since: float
    priority_downgraded: bool = False


class StalledSeedMonitor:
    """停滞种子监控器"""

    def __init__(self, qbt_client, config):
        self.qbt_client = qbt_client
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 跟踪中的停滞种子
        self.tracked_seeds: Dict[str, StalledSeedInfo] = {}

        # 配置参数
        self.min_stalled_minutes = getattr(config, "min_stalled_minutes", 30)
        self.check_interval = getattr(config, "stalled_check_interval", 300)
        self.progress_threshold = getattr(config, "progress_threshold", 0.95)

    def scan_stalled_torrents(self) -> List[StalledSeedInfo]:
        """扫描停滞种子并处理"""
        try:
            stalled_torrents = self._get_stalled_torrents()
            current_time = time.time()
            processed_seeds = []

            for torrent in stalled_torrents:
                seed_info = self._process_stalled_torrent(torrent, current_time)
                if seed_info:
                    processed_seeds.append(seed_info)

            # 清理已恢复的种子
            self._cleanup_recovered_seeds(stalled_torrents)
            return processed_seeds

        except Exception as e:
            self.logger.error(f"扫描停滞种子失败: {e}")
            return []

    def _get_stalled_torrents(self) -> List:
        """获取停滞状态的种子"""
        try:
            stalled_torrents = self.qbt_client.client.torrents_info(
                status_filter="downloading"
            )

            active_stalled = [
                torrent
                for torrent in stalled_torrents
                if torrent.progress < self.progress_threshold
                and torrent.state == "stalledDL"
            ]

            self.logger.debug(f"发现 {len(active_stalled)} 个活跃的停滞种子")
            return active_stalled

        except Exception as e:
            self.logger.error(f"获取停滞种子列表失败: {e}")
            return []

    def _process_stalled_torrent(
        self, torrent, current_time: float
    ) -> Optional[StalledSeedInfo]:
        """处理单个停滞种子"""
        torrent_hash = torrent.hash
        progress = torrent.progress
        name = torrent.name

        # 检查进度条件：只处理未完成的种子
        if progress >= self.progress_threshold:
            return None

        # 获取或创建跟踪信息
        if torrent_hash in self.tracked_seeds:
            seed_info = self.tracked_seeds[torrent_hash]
            # 检查进度是否有变化
            progress_changed = abs(progress - seed_info.progress) > 0.001

            if progress_changed:
                # 进度有变化，重置跟踪时间
                self.logger.info(
                    f"种子进度有变化，重置跟踪: {name} (进度: {progress:.1%})"
                )
                seed_info.tracked_since = current_time
                seed_info.progress = progress
                seed_info.state = torrent.state
            else:
                # 进度无变化，更新进度值和状态
                seed_info.progress = progress
                seed_info.state = torrent.state

        else:
            # 新发现的停滞种子
            seed_info = StalledSeedInfo(
                torrent_hash=torrent_hash,
                name=name,
                progress=progress,
                state=torrent.state,
                tracked_since=current_time,
            )
            self.tracked_seeds[torrent_hash] = seed_info
            self.logger.info(f"开始跟踪停滞种子: {name} (进度: {progress:.1%})")

        # 检查是否满足降级条件
        stalled_duration = (current_time - seed_info.tracked_since) / 60  # 分钟

        if (
            stalled_duration >= self.min_stalled_minutes
            and not seed_info.priority_downgraded
        ):
            success = self._lower_torrent_priority(seed_info)
            if success:
                seed_info.priority_downgraded = True
                return seed_info

        return None

    def _lower_torrent_priority(self, seed_info: StalledSeedInfo) -> bool:
        """降低种子优先级到最低"""
        try:
            # 使用qbittorrent-api设置最低优先级
            self.qbt_client.client.torrents_bottom_priority(
                torrent_hashes=seed_info.torrent_hash
            )

            self.logger.warning(
                f"停滞种子优先级已调低: {seed_info.name} "
                f"(进度: {seed_info.progress:.1%}, 状态: {seed_info.state}, "
                f"停滞: {(time.time() - seed_info.tracked_since) / 60:.1f}分钟)"
            )
            return True

        except Exception as e:
            self.logger.error(f"降低种子优先级失败 {seed_info.name}: {e}")
            return False

    def _cleanup_recovered_seeds(self, current_stalled: List):
        """清理已恢复的种子"""
        current_hashes = {torrent.hash for torrent in current_stalled}

        # 找出已恢复的种子（之前跟踪但现在不在stalled列表中）
        recovered_hashes = set(self.tracked_seeds.keys()) - current_hashes

        for hash_val in recovered_hashes:
            seed_info = self.tracked_seeds[hash_val]
            if seed_info.priority_downgraded:
                self.logger.info(f"种子已恢复活动且优先级已降级: {seed_info.name}")
            else:
                self.logger.info(f"种子已恢复活动: {seed_info.name}")
            del self.tracked_seeds[hash_val]

    def get_monitoring_summary(self) -> Dict:
        """获取监控摘要"""
        total_tracked = len(self.tracked_seeds)
        downgraded = len(
            [s for s in self.tracked_seeds.values() if s.priority_downgraded]
        )

        # 按停滞时间分组
        current_time = time.time()
        time_groups = {"0-30min": 0, "30-60min": 0, "60min+": 0}

        for seed in self.tracked_seeds.values():
            stalled_minutes = (current_time - seed.tracked_since) / 60
            if stalled_minutes < 30:
                time_groups["0-30min"] += 1
            elif stalled_minutes < 60:
                time_groups["30-60min"] += 1
            else:
                time_groups["60min+"] += 1

        return {
            "total_tracked": total_tracked,
            "downgraded": downgraded,
            "stalled_time_distribution": time_groups,
            "tracked_seeds": [
                {
                    "name": s.name,
                    "progress": f"{s.progress:.1%}",
                    "state": s.state,
                    "stalled_minutes": f"{(current_time - s.tracked_since) / 60:.1f}",
                    "priority_downgraded": s.priority_downgraded,
                }
                for s in list(self.tracked_seeds.values())[:10]
            ],
        }
