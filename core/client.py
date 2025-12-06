"""
qBittorrent API客户端
封装qBittorrent的所有API操作
"""

import logging
import time
from typing import List, Optional, Dict, Any
import qbittorrentapi


class QBittorrentClient:
    """qBittorrent API客户端"""

    def __init__(self, host: str, port: int, username: str = "", password: str = ""):
        """
        初始化客户端

        Args:
            host: qBittorrent主机地址
            port: qBittorrent端口
            username: 用户名（可选）
            password: 密码（可选）
        """
        self.logger = logging.getLogger(__name__)

        # 初始化客户端
        self.client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
            VERIFY_WEBUI_CERTIFICATE=False,
            REQUESTS_ARGS={"timeout": (5, 30)},
        )

        self.logger.info(f"初始化qBittorrent客户端: {host}:{port}")

    def connect(self) -> bool:
        """
        连接到qBittorrent

        Returns:
            bool: 连接是否成功
        """
        try:
            self.client.auth_log_in()
            version = self.client.app_version()
            self.logger.info(f"成功连接到qBittorrent，版本: {version}")
            return True

        except qbittorrentapi.LoginFailed as e:
            self.logger.error(f"qBittorrent登录失败: {e}")
            return False
        except Exception as e:
            self.logger.error(f"连接qBittorrent失败: {e}")
            return False

    def wait_for_connection(self, max_retries: int = 60, retry_interval: int = 5):
        """
        等待qBittorrent启动

        Args:
            max_retries: 最大重试次数
            retry_interval: 重试间隔（秒）

        Raises:
            ConnectionError: 连接失败
        """
        self.logger.info("等待qBittorrent启动...")

        for attempt in range(max_retries):
            if self.connect():
                return

            self.logger.debug(f"等待中... ({attempt + 1}/{max_retries})")
            time.sleep(retry_interval)

        raise ConnectionError("无法连接到qBittorrent，请检查服务是否运行")

    # === 标签操作 ===

    def get_torrents_by_tag(self, tag: str) -> List:
        """
        根据标签获取种子

        Args:
            tag: 标签名称
            exclude_states: 要排除的状态列表

        Returns:
            List: 种子列表
        """
        try:
            all_torrents = self.client.torrents_info()

            # 过滤包含指定标签的种子
            tagged_torrents = [
                torrent
                for torrent in all_torrents
                if tag in (torrent.tags or "").split(", ")
                and torrent.hash != torrent.name
            ]

            return tagged_torrents

        except Exception as e:
            self.logger.error(f"获取标签种子失败 {tag}: {e}")
            return []

    def add_tag(self, torrent_hash: str, tag: str):
        """
        为种子添加标签

        Args:
            torrent_hash: 种子哈希
            tag: 标签名称
        """
        try:
            self.client.torrents_add_tags(tags=tag, torrent_hashes=torrent_hash)
            self.logger.debug(f"为种子 {torrent_hash} 添加标签: {tag}")

        except Exception as e:
            self.logger.error(f"添加标签失败 {torrent_hash}, {tag}: {e}")

    def remove_tag(self, torrent_hash: str, tag: str):
        """
        移除种子的标签

        Args:
            torrent_hash: 种子哈希
            tag: 标签名称
        """
        try:
            self.client.torrents_remove_tags(tags=tag, torrent_hashes=torrent_hash)
            self.logger.debug(f"从种子 {torrent_hash} 移除标签: {tag}")

        except Exception as e:
            self.logger.error(f"移除标签失败 {torrent_hash}, {tag}: {e}")

    # === 种子操作 ===

    def get_torrent_by_hash(
        self, torrent_hash: str
    ) -> Optional[qbittorrentapi.TorrentDictionary]:
        """
        根据哈希获取种子信息

        Args:
            torrent_hash: 种子哈希

        Returns:
            Optional: 种子信息，如果不存在则返回None
        """
        try:
            torrent = self.client.torrents_info(torrent_hashes=torrent_hash)
            return torrent[0] if torrent else None

        except Exception as e:
            self.logger.error(f"获取种子信息失败 {torrent_hash}: {e}")
            return None

    def get_torrent_files(self, torrent_hash: str) -> List[Dict[str, Any]]:
        """
        获取种子的文件列表

        Args:
            torrent_hash: 种子哈希

        Returns:
            List[Dict]: 文件列表
        """
        try:
            files = self.client.torrents_files(torrent_hash=torrent_hash)

            return [
                {
                    "name": file.name,
                    "size": file.size,
                    "priority": file.priority,
                    "index": file.index,
                }
                for file in files
            ]

        except Exception as e:
            self.logger.error(f"获取种子文件列表失败 {torrent_hash}: {e}")
            return []

    # === 文件操作 ===

    def set_file_priority(
        self, torrent_hash: str, file_indexes: List[int], priority: int = 0
    ) -> bool:
        """
        设置文件下载优先级

        Args:
            torrent_hash: 种子哈希
            file_indexes: 文件索引列表
            priority: 优先级（0=不下载）

        Returns:
            bool: 操作是否成功
        """
        try:
            self.client.torrents_file_priority(
                torrent_hash=torrent_hash, file_ids=file_indexes, priority=priority
            )
            return True

        except Exception as e:
            self.logger.error(f"设置文件优先级失败 {torrent_hash}: {e}")
            return False

    def set_lowest_priority(self, torrent_hash: str) -> bool:
        """
        设置种子为最低优先级

        Args:
            torrent_hash: 种子哈希

        Returns:
            bool: 操作是否成功
        """
        try:
            self.client.torrents_bottom_priority(torrent_hashes=torrent_hash)
            self.logger.debug(f"设置种子 {torrent_hash} 为最低优先级")
            return True

        except Exception as e:
            self.logger.error(f"设置最低优先级失败 {torrent_hash}: {e}")
            return False

    # === 监控操作 ===

    def get_stalled_torrents(self) -> List:
        """
        获取停滞种子

        Returns:
            List: 停滞种子列表
        """
        try:
            downloading = self.client.torrents_info(status_filter="downloading")

            return [
                torrent
                for torrent in downloading
                if torrent.state == "stalledDL" and torrent.progress < 0.95
            ]

        except Exception as e:
            self.logger.error(f"获取停滞种子失败: {e}")
            return []

    # === 系统信息 ===

    def get_app_version(self) -> str:
        """
        获取qBittorrent版本

        Returns:
            str: 版本号
        """
        try:
            return self.client.app_version()
        except Exception as e:
            self.logger.error(f"获取qBittorrent版本失败: {e}")
            return "unknown"
