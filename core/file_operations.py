import os
import re
import shutil
import logging
from pathlib import Path
from typing import List


class FileOperations:
    """文件操作管理器"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def should_delete_file(self, filename: str, file_size: int) -> bool:
        """判断是否应该删除文件（包含文件大小检查）"""
        # 检查文件大小
        min_size_bytes = self.config.min_file_size_mb * 1024 * 1024
        if file_size < min_size_bytes:
            return True

        # 检查正则表达式匹配
        return self._match_patterns(filename, self.config.file_patterns)

    def should_delete_file_by_name(self, filename: str) -> bool:
        """判断是否应该删除文件（仅基于文件名）"""
        return self._match_patterns(filename, self.config.file_patterns)

    def should_disable_file(self, filename: str) -> bool:
        """判断是否应该禁用文件下载"""
        return self._match_patterns(filename, self.config.disable_file_patterns)

    def should_delete_folder(self, folder_name: str) -> bool:
        """判断是否应该删除文件夹"""
        return self._match_patterns(folder_name, self.config.folder_patterns)

    def _match_patterns(self, name: str, patterns: List[str]) -> bool:
        """匹配正则表达式模式"""
        for pattern in patterns:
            try:
                if re.match(pattern, name, re.IGNORECASE):
                    return True
            except re.error as e:
                self.logger.error(f"正则表达式错误: {pattern}, 错误: {e}")
        return False

    def get_torrent_content_directory(self, torrent: dict) -> str:
        """获取种子的内容目录"""
        content_path = torrent.get("content_path", "")

        if not content_path or not os.path.exists(content_path):
            return ""

        # 如果是目录，直接返回
        if os.path.isdir(content_path):
            return content_path
        else:
            # 如果是文件，返回其父目录
            return os.path.dirname(content_path)

    def is_path_in_content_directory(self, path: str, content_dir: str) -> bool:
        """检查路径是否在内容目录内"""
        try:
            norm_path = os.path.normpath(path)
            norm_content_dir = os.path.normpath(content_dir)
            return norm_path.startswith(norm_content_dir)
        except Exception as e:
            self.logger.error(f"检查路径是否在内容目录内时发生错误: {e}")
            return False

    def clean_empty_directories(self, directory: str, content_dir: str):
        """递归删除空目录，但限制在内容目录内"""
        try:
            current_dir = Path(directory)
            content_dir_path = Path(content_dir)

            # 向上遍历目录树，但限制在内容目录内
            while (
                current_dir.exists()
                and current_dir.is_dir()
                and self.is_path_in_content_directory(str(current_dir), content_dir)
                and current_dir != content_dir_path
            ):
                # 检查目录是否为空
                if not any(current_dir.iterdir()):
                    try:
                        current_dir.rmdir()
                        self.logger.info(f"已删除空目录: {current_dir}")
                        current_dir = current_dir.parent
                    except Exception:
                        break
                else:
                    break

        except Exception as e:
            self.logger.error(f"清理空目录时发生错误: {e}")

    def path_exists(self, path: str) -> bool:
        """检查路径是否存在"""
        return os.path.exists(path)
