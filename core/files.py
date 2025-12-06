"""
文件操作管理器
适配新的配置结构
"""

import re
import os
import shutil
import logging
from typing import List, Tuple, Pattern


class FileManager:
    """文件操作管理器"""

    def __init__(self, config):
        """
        初始化文件管理器

        Args:
            config: 配置对象（SimpleConfig）
        """
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 预编译正则表达式
        self.file_patterns = self._compile_patterns(config.file_patterns)
        self.folder_patterns = self._compile_patterns(config.folder_patterns)
        self.disable_patterns = self._compile_patterns(config.disable_file_patterns)

        self.logger.info(
            f"初始化文件管理器: "
            f"{len(self.file_patterns)}个文件模式, "
            f"{len(self.folder_patterns)}个目录模式, "
            f"{len(self.disable_patterns)}个禁用模式"
        )

    def _compile_patterns(self, patterns: List[str]) -> List[Pattern]:
        """
        编译正则表达式

        Args:
            patterns: 正则表达式字符串列表

        Returns:
            List[Pattern]: 编译后的正则表达式列表
        """
        compiled = []

        for pattern in patterns:
            try:
                compiled_pattern = re.compile(pattern, re.IGNORECASE)
                compiled.append(compiled_pattern)
            except re.error as e:
                self.logger.error(f"正则表达式错误 '{pattern}': {e}")

        return compiled

    # === 匹配检查 ===

    def should_delete_file(self, filename: str) -> bool:
        """
        检查文件是否应该删除

        Args:
            filename: 文件名

        Returns:
            bool: 是否应该删除
        """
        return self._match_patterns(filename, self.file_patterns)

    def should_delete_folder(self, foldername: str) -> bool:
        """
        检查目录是否应该删除

        Args:
            foldername: 目录名

        Returns:
            bool: 是否应该删除
        """
        return self._match_patterns(foldername, self.folder_patterns)

    def should_disable_file(self, filename: str) -> bool:
        """
        检查文件是否应该禁用

        Args:
            filename: 文件名

        Returns:
            bool: 是否应该禁用
        """
        return self._match_patterns(filename, self.disable_patterns)

    def _match_patterns(self, name: str, patterns: List[Pattern]) -> bool:
        """
        匹配正则表达式

        Args:
            name: 要匹配的名称
            patterns: 正则表达式列表

        Returns:
            bool: 是否匹配
        """
        for pattern in patterns:
            if pattern.search(name):
                return True
        return False

    # === 清理操作 ===

    def clean_directory(self, directory_path: str) -> Tuple[int, int]:
        """
        清理目录

        Args:
            directory_path: 目录路径

        Returns:
            Tuple[int, int]: (删除的文件数量, 删除的目录数量)
        """
        deleted_files = 0
        deleted_folders = 0

        if not os.path.exists(directory_path):
            return deleted_files, deleted_folders

        try:
            # 如果是文件而不是目录
            if os.path.isfile(directory_path):
                return self._clean_file(directory_path)

            # 递归清理目录
            return self._clean_directory_recursive(directory_path)

        except Exception as e:
            self.logger.error(f"清理目录失败 {directory_path}: {e}")
            return deleted_files, deleted_folders

    def _clean_file(self, file_path: str) -> Tuple[int, int]:
        """
        清理单个文件

        Args:
            file_path: 文件路径

        Returns:
            Tuple[int, int]: (删除的文件数量, 删除的目录数量)
        """
        filename = os.path.basename(file_path)

        if self.should_delete_file(filename):
            try:
                os.remove(file_path)
                self.logger.debug(f"删除文件: {file_path}")
                return 1, 0
            except Exception as e:
                self.logger.error(f"删除文件失败 {file_path}: {e}")

        return 0, 0

    def _clean_directory_recursive(self, directory_path: str) -> Tuple[int, int]:
        """
        递归清理目录

        Args:
            directory_path: 目录路径

        Returns:
            Tuple[int, int]: (删除的文件数量, 删除的目录数量)
        """
        deleted_files = 0
        deleted_folders = 0

        try:
            # 获取所有条目
            entries = list(os.scandir(directory_path))

            # 先处理子目录
            for entry in entries:
                if entry.is_dir():
                    files, folders = self._process_directory_entry(entry)
                    deleted_files += files
                    deleted_folders += folders

            # 再次扫描（因为可能删除了目录）
            entries = list(os.scandir(directory_path))

            # 处理文件
            for entry in entries:
                if entry.is_file():
                    files, folders = self._process_file_entry(entry)
                    deleted_files += files
                    deleted_folders += folders

            # 清理空目录
            self._clean_empty_directory(directory_path)

            return deleted_files, deleted_folders

        except Exception as e:
            self.logger.error(f"递归清理目录失败 {directory_path}: {e}")
            return deleted_files, deleted_folders

    def _process_directory_entry(self, entry: os.DirEntry) -> Tuple[int, int]:
        """
        处理目录条目

        Args:
            entry: 目录条目

        Returns:
            Tuple[int, int]: (删除的文件数量, 删除的目录数量)
        """
        if self.should_delete_folder(entry.name):
            try:
                shutil.rmtree(entry.path)
                self.logger.info(f"删除目录: {entry.path}")
                return 0, 1
            except Exception as e:
                self.logger.error(f"删除目录失败 {entry.path}: {e}")
                return 0, 0
        else:
            # 递归清理子目录
            return self._clean_directory_recursive(entry.path)

    def _process_file_entry(self, entry: os.DirEntry) -> Tuple[int, int]:
        """
        处理文件条目

        Args:
            entry: 文件条目

        Returns:
            Tuple[int, int]: (删除的文件数量, 删除的目录数量)
        """
        if self.should_delete_file(entry.name):
            try:
                os.remove(entry.path)
                self.logger.debug(f"删除文件: {entry.path}")
                return 1, 0
            except Exception as e:
                self.logger.error(f"删除文件失败 {entry.path}: {e}")

        return 0, 0

    def _clean_empty_directory(self, directory_path: str):
        """
        清理空目录

        Args:
            directory_path: 目录路径
        """
        try:
            if not any(os.scandir(directory_path)):
                os.rmdir(directory_path)
                self.logger.debug(f"删除空目录: {directory_path}")
        except Exception as e:
            self.logger.debug(f"无法删除目录 {directory_path}: {e}")
