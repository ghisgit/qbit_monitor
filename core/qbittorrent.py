import time
import logging
from typing import List, Dict, Optional, Tuple
import requests


class QBittorrentClient:
    """qBittorrent API客户端"""

    def __init__(self, host: str, port: int):
        self.base_url = f"http://{host}:{port}"
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

    def start_torrent(self, torrent_hash: str) -> bool:
        """开始下载种子"""
        try:
            response = self.session.post(
                f"{self.base_url}/api/v2/torrents/start",
                data={"hashes": torrent_hash},
                timeout=10,
            )
            if response.status_code == 200:
                return True
            else:
                self.logger.error(f"启动种子失败: 状态码 {response.status_code}")
                return False
        except Exception as e:
            self.logger.error(f"启动种子失败时发生错误: {e}")
            return False

    def get_torrent_by_hash(
        self, torrent_hash: str, return_error_type: bool = False
    ) -> Optional[Dict]:
        """
        根据哈希获取种子信息

        Args:
            torrent_hash: 种子哈希
            return_error_type: 是否返回错误类型

        Returns:
            如果 return_error_type=False: 直接返回torrent字典或None
            如果 return_error_type=True: 返回(torrent, error_type)元组
        """
        try:
            response = self.session.get(
                f"{self.base_url}/api/v2/torrents/info",
                params={"hashes": torrent_hash},
                timeout=10,
            )

            if response.status_code == 200:
                torrents = response.json()

                # 如果返回空列表，表示种子不存在
                if not torrents:
                    self.logger.debug(f"种子不存在: {torrent_hash}")
                    if return_error_type:
                        return None, "not_found"
                    return None

                # 返回第一个种子信息
                torrent = torrents[0]
                self.logger.debug(f"成功获取种子信息: {torrent_hash}")

                if return_error_type:
                    return torrent, None
                return torrent

            else:
                # API返回错误状态码
                self.logger.error(
                    f"qBittorrent API错误: 状态码 {response.status_code}, 种子: {torrent_hash}"
                )
                if return_error_type:
                    return None, "api_error"
                return None

        except requests.exceptions.Timeout:
            self.logger.error(f"获取种子信息超时: {torrent_hash}")
            if return_error_type:
                return None, "network_error"
            return None
        except requests.exceptions.ConnectionError:
            self.logger.error(f"连接qBittorrent失败: {torrent_hash}")
            if return_error_type:
                return None, "network_error"
            return None
        except Exception as e:
            self.logger.error(f"获取种子信息时发生错误: {torrent_hash}, 错误: {e}")
            if return_error_type:
                return None, "api_error"
            return None

    def get_torrent_by_hash_with_error(
        self, torrent_hash: str
    ) -> Tuple[Optional[Dict], Optional[str]]:
        """获取种子信息并返回错误类型"""
        return self.get_torrent_by_hash(torrent_hash, return_error_type=True)

    def get_torrent_files(self, torrent_hash: str) -> List[Dict]:
        """获取种子的文件列表"""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v2/torrents/files",
                params={"hash": torrent_hash},
                timeout=10,
            )
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.error(
                    f"获取种子文件列表失败: 状态码 {response.status_code}"
                )
                return []
        except Exception as e:
            self.logger.error(f"获取种子文件列表时发生错误: {e}")
            return []

    def set_torrent_file_priority(
        self, torrent_hash: str, file_indexes: List[int], priority: int = 0
    ) -> bool:
        """设置种子文件的下载优先级"""
        try:
            id_str = "|".join(map(str, file_indexes))
            data = {"hash": torrent_hash, "id": id_str, "priority": priority}

            response = self.session.post(
                f"{self.base_url}/api/v2/torrents/filePrio", data=data, timeout=10
            )
            return response.status_code == 200

        except Exception as e:
            self.logger.error(f"设置文件优先级时发生错误: {e}")
            return False

    def wait_for_qbit(self):
        """等待qBittorrent启动"""
        self.logger.info("等待qBittorrent启动...")
        while True:
            try:
                response = self.session.get(
                    f"{self.base_url}/api/v2/app/version", timeout=5
                )
                if response.status_code == 200:
                    self.logger.info(f"qBittorrent已启动! 版本: {response.text}")
                    break
                time.sleep(5)
            except Exception as e:
                self.logger.debug(f"qBittorrent尚未启动，等待... {e}")
                time.sleep(5)

    def get_app_version(self) -> str:
        """获取qBittorrent版本"""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v2/app/version", timeout=5
            )
            if response.status_code == 200:
                return response.text
        except Exception as e:
            self.logger.error(f"获取qBittorrent版本失败: {e}")
        return "unknown"

    def test_connection(self) -> Tuple[bool, str]:
        """测试连接状态"""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v2/app/version", timeout=5
            )
            if response.status_code == 200:
                return True, "connected"
            else:
                return False, f"api_error_{response.status_code}"

        except requests.exceptions.Timeout:
            return False, "timeout"
        except requests.exceptions.ConnectionError:
            return False, "connection_error"
        except Exception as e:
            return False, f"error_{str(e)}"
