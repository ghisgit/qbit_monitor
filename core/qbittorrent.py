import requests
import logging
from typing import List, Dict, Optional


class QBittorrentClient:
    def __init__(self, host: str, port: int):
        self.base_url = f"http://{host}:{port}"
        self.session = requests.Session()
        self.logger = logging.getLogger(__name__)

    def get_torrent_by_hash(self, torrent_hash: str) -> Optional[Dict]:
        """根据哈希获取种子信息"""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v2/torrents/info", params={"hashes": torrent_hash}
            )
            if response.status_code == 200:
                torrents = response.json()
                return torrents[0] if torrents else None
        except Exception as e:
            self.logger.error(f"获取种子信息时发生错误: {e}")
        return None

    def get_torrent_files(self, torrent_hash: str) -> List[Dict]:
        """获取种子的文件列表"""
        try:
            response = self.session.get(
                f"{self.base_url}/api/v2/torrents/files", params={"hash": torrent_hash}
            )
            if response.status_code == 200:
                return response.json()
        except Exception as e:
            self.logger.error(f"获取种子文件时发生错误: {e}")
        return []

    def set_torrent_file_priority(
        self, torrent_hash: str, file_indexes: List[int], priority: int = 0
    ) -> bool:
        """设置种子文件的下载优先级"""
        try:
            id_str = "|".join(map(str, file_indexes))
            data = {"hash": torrent_hash, "id": id_str, "priority": priority}

            response = self.session.post(
                f"{self.base_url}/api/v2/torrents/filePrio", data=data
            )

            return response.status_code == 200
        except Exception as e:
            self.logger.error(f"设置文件优先级时发生错误: {e}")
            return False

    def wait_for_qbit(self):
        self.logger.info(f"wait qBittorrent start...")
        while True:
            try:
                response = self.session.get(f"{self.base_url}/api/v2/app/version")
                if response.status_code == 200:
                    self.logger.info(
                        f"qBittorrent started! qBittorrent version: {response.text}"
                    )
                    break
            except Exception as e:
                self.logger.error(f"qBittorrent not start, wait...")
