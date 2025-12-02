import logging
import time
from typing import List, Dict, Optional, Tuple
import qbittorrentapi


class QBittorrentClient:
    """qBittorrent API客户端 - 使用qbittorrent-api库"""

    def __init__(self, host: str, port: int, username: str = "", password: str = ""):
        self.logger = logging.getLogger(__name__)

        # 初始化qbittorrent-api客户端
        self.client = qbittorrentapi.Client(
            host=host,
            port=port,
            username=username,
            password=password,
            VERIFY_WEBUI_CERTIFICATE=False,
            REQUESTS_ARGS={"timeout": (5, 30)},  # 连接超时5秒，读取超时30秒
        )

        self.logger.info(f"初始化qBittorrent客户端: {host}:{port}")

    def connect(self) -> bool:
        """连接到qBittorrent"""
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

    def wait_for_qbit(self):
        """等待qBittorrent启动"""
        self.logger.info("等待qBittorrent启动...")
        max_retries = 60  # 最多等待5分钟

        for i in range(max_retries):
            try:
                if self.connect():
                    break
                else:
                    self.logger.debug(
                        f"qBittorrent尚未启动，等待... ({i+1}/{max_retries})"
                    )
                    time.sleep(5)
            except Exception as e:
                self.logger.debug(f"连接尝试失败: {e}")
                time.sleep(5)

        if not self.client.is_logged_in:
            raise ConnectionError("无法连接到qBittorrent，请检查服务是否运行")

    def get_torrent_by_hash(
        self, torrent_hash: str
    ) -> Optional[qbittorrentapi.TorrentDictionary]:
        """根据哈希获取种子信息"""
        try:
            torrent = self.client.torrents_info(torrent_hashes=torrent_hash)
            if torrent and len(torrent) > 0:
                return torrent[0]
            return None
        except Exception as e:
            self.logger.error(f"获取种子信息失败 {torrent_hash}: {e}")
            return None

    def get_torrents_by_tag(
        self, tag: str, exclude_states: List[str] = None
    ) -> List[qbittorrentapi.TorrentDictionary]:
        """根据标签获取种子"""
        try:
            all_torrents = self.client.torrents_info()

            # 过滤包含指定标签的种子
            tagged_torrents = [
                torrent
                for torrent in all_torrents
                if tag in (torrent.tags or "").split(", ")
            ]

            # 排除指定状态的种子
            if exclude_states:
                filtered_torrents = [
                    torrent
                    for torrent in tagged_torrents
                    if torrent.state not in exclude_states
                ]
                return filtered_torrents

            return tagged_torrents

        except Exception as e:
            self.logger.error(f"获取标签种子失败 {tag}: {e}")
            return []

    def add_tag(self, torrent_hash: str, tag: str) -> bool:
        """为种子添加标签"""
        try:
            self.client.torrents_add_tags(tags=tag, torrent_hashes=torrent_hash)
            self.logger.debug(f"为种子 {torrent_hash} 添加标签: {tag}")
            return True
        except Exception as e:
            self.logger.error(f"添加标签失败 {torrent_hash}, {tag}: {e}")
            return False

    def remove_tag(self, torrent_hash: str, tag: str) -> bool:
        """移除种子的标签"""
        try:
            self.client.torrents_remove_tags(tags=tag, torrent_hashes=torrent_hash)
            self.logger.debug(f"从种子 {torrent_hash} 移除标签: {tag}")
            return True
        except Exception as e:
            self.logger.error(f"移除标签失败 {torrent_hash}, {tag}: {e}")
            return False

    def set_torrent_file_priority(
        self, torrent_hash: str, file_indexes: List[int], priority: int = 0
    ) -> bool:
        """设置种子文件的下载优先级"""
        try:
            # qbittorrent-api 的API与直接requests不同
            # 这里使用设置文件优先级的方法
            self.client.torrents_file_priority(
                torrent_hash=torrent_hash, file_ids=file_indexes, priority=priority
            )
            return True
        except Exception as e:
            self.logger.error(f"设置文件优先级失败 {torrent_hash}: {e}")
            return False

    def start_torrent(self, torrent_hash: str) -> bool:
        """开始下载种子"""
        try:
            self.client.torrents_resume(torrent_hashes=torrent_hash)
            return True
        except Exception as e:
            self.logger.error(f"启动种子失败 {torrent_hash}: {e}")
            return False

    def get_torrent_files(self, torrent_hash: str) -> List[Dict]:
        """获取种子的文件列表"""
        try:
            files = self.client.torrents_files(torrent_hash=torrent_hash)
            # 转换为字典列表以便兼容旧代码
            file_list = []
            for file in files:
                file_list.append(
                    {
                        "name": file.name,
                        "size": file.size,
                        "progress": file.progress,
                        "priority": file.priority,
                        "index": file.index,
                    }
                )
            return file_list
        except Exception as e:
            self.logger.error(f"获取种子文件列表失败 {torrent_hash}: {e}")
            return []

    def test_connection(self) -> Tuple[bool, str]:
        """测试连接状态"""
        try:
            version = self.client.app_version()
            return True, f"connected_v{version}"
        except qbittorrentapi.LoginFailed as e:
            return False, f"login_failed:{str(e)}"
        except qbittorrentapi.APIConnectionError as e:
            return False, f"connection_error:{str(e)}"
        except Exception as e:
            return False, f"error:{str(e)}"

    def get_app_version(self) -> str:
        """获取qBittorrent版本"""
        try:
            return self.client.app_version()
        except Exception as e:
            self.logger.error(f"获取qBittorrent版本失败: {e}")
            return "unknown"

    def get_stalled_torrents(self) -> List[qbittorrentapi.TorrentDictionary]:
        """获取停滞状态的种子"""
        try:
            # 获取所有下载中的种子
            downloading = self.client.torrents_info(filter="downloading")

            # 过滤出进度长时间未变化的种子
            current_time = time.time()
            stalled_torrents = []

            for torrent in downloading:
                # 检查最后活动时间（如果可用）
                if hasattr(torrent, "last_activity"):
                    if current_time - torrent.last_activity > 300:  # 5分钟无活动
                        stalled_torrents.append(torrent)

            return stalled_torrents

        except Exception as e:
            self.logger.error(f"获取停滞种子失败: {e}")
            return []
