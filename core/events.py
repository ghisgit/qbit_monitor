import os
import time
import logging
import shutil
from typing import List, Tuple, Optional
from persistence.task_store import Task


class EventHandler:
    def __init__(self, qbt_client, file_ops, config):
        self.qbt_client = qbt_client
        self.file_ops = file_ops
        self.config = config
        self.logger = logging.getLogger(__name__)

    def _get_and_validate_torrent(
        self, torrent_hash: str, task_type: str
    ) -> Tuple[Optional[dict], str]:
        """
        根据验证返回不同错误类型
        """
        try:
            # 获取种子信息（使用带错误类型的方法）
            torrent, error_type = self.qbt_client.get_torrent_by_hash_with_error(
                torrent_hash, return_error_type=True
            )

            if error_type == "not_found":
                # 种子确实不存在，需要重试（可能正在添加中）
                self.logger.warning(f"种子不存在: {torrent_hash}，移除任务")
                return None, "torrent_not_found"
            elif error_type == "api_error":
                # qBittorrent服务问题，需要重试
                self.logger.warning(
                    f"qBittorrent服务暂时不可用: {torrent_hash}, 错误类型: {error_type}"
                )
                return None, "qbit_api_error"
            elif error_type == "network_error":
                # 网络问题，需要重试
                self.logger.warning(
                    f"网络暂时不可用: {torrent_hash}, 错误类型: {error_type}"
                )
                return None, "network_error"
            elif torrent is None:
                # 其他未知错误，需要重试
                self.logger.warning(f"获取种子信息失败但原因未知: {torrent_hash}")
                return None, "retry_later"

            # 成功获取到种子信息，检查是否应该处理
            torrent_name = torrent.get("name", "Unknown")

            self.logger.debug(
                f"种子验证通过: {torrent_name}, "
                f"任务类型: {task_type}, 大小: {self._format_size(torrent.get('size', 0))}"
            )

            # 检查分类过滤
            if not self.should_process_torrent(torrent):
                self.logger.info(f"种子不符合处理条件: {torrent_name}")
                return None, "success"

            return torrent, "continue"

        except Exception as e:
            self.logger.warning(f"种子验证过程异常: {torrent_hash}, 错误: {e}")
            # 验证过程中出现异常，需要重试
            return None, "retry_later"

    def _check_metadata_ready(self, torrent_hash: str, torrent_name: str) -> bool:
        """检查种子元数据是否就绪"""
        try:
            files = self.qbt_client.get_torrent_files(torrent_hash)
            if not files:
                self.logger.debug(f"种子 {torrent_name} 元数据未就绪，需要延迟重试")
                return False
            return True
        except Exception as e:
            self.logger.error(f"检查元数据时发生错误 {torrent_name}: {e}")
            return False

    def should_process_torrent(self, torrent: dict) -> bool:
        """判断是否应该处理这个种子"""
        torrent_category = torrent.get("category", "")

        # 如果没有配置分类，处理所有种子
        if not self.config.categories:
            return True

        # 检查种子分类是否在允许的列表中
        return torrent_category in self.config.categories

    def get_files_to_disable(self, torrent: dict) -> List[int]:
        """获取需要禁用的文件索引列表（使用 disable_file_patterns）"""
        files_to_disable = []
        torrent_hash = torrent["hash"]
        torrent_name = torrent.get("name", "Unknown")

        try:
            # 获取种子文件列表
            files = self.qbt_client.get_torrent_files(torrent_hash)
            if not files:
                self.logger.debug(f"种子 {torrent_name} 文件列表为空")
                return []

            for file_info in files:
                file_name = file_info["name"]
                file_priority = file_info.get("priority", 1)
                file_index = file_info["index"]

                # 检查是否应该禁用文件下载（使用 disable_file_patterns）
                if self.file_ops.should_disable_file(file_name) and file_priority != 0:
                    files_to_disable.append(file_index)
                    self.logger.info(f"标记文件为不下载: {file_name}")

            self.logger.debug(
                f"种子 {torrent_name} 找到 {len(files_to_disable)} 个需要禁用的文件"
            )

        except Exception as e:
            self.logger.error(f"获取需要禁用的文件时发生错误 {torrent_name}: {e}")

        return files_to_disable

    def disable_files_for_torrent(self, torrent: dict) -> bool:
        """为种子禁用匹配的文件下载，返回是否成功"""
        torrent_hash = torrent["hash"]
        torrent_name = torrent.get("name", "Unknown")

        try:
            # 获取需要禁用的文件列表
            files_to_disable = self.get_files_to_disable(torrent)

            # 如果没有文件需要禁用，视为成功
            if not files_to_disable:
                self.logger.debug(f"种子 {torrent_name} 没有需要禁用的文件")
                return True

            # 在 qBittorrent 中禁用标记的文件
            self.logger.debug(f"准备禁用 {len(files_to_disable)} 个文件")
            success = self.qbt_client.set_torrent_file_priority(
                torrent_hash, files_to_disable, 0
            )

            if success:
                self.logger.info(
                    f"种子 {torrent_name} 成功禁用了 {len(files_to_disable)} 个文件"
                )
            else:
                self.logger.error(f"种子 {torrent_name} 禁用文件失败")

            return success

        except Exception as e:
            self.logger.error(f"为种子禁用文件时发生错误 {torrent_name}: {e}")
            return False

    def clean_torrent_files(self, torrent: dict) -> int:
        """清理单个种子的无用文件和文件夹（基于文件系统，不依赖种子信息）"""
        deleted_count = 0
        torrent_name = torrent.get("name", "Unknown")

        self.logger.debug(f"开始清理种子文件: {torrent_name}")

        try:
            # 获取种子的保存路径和内容目录
            save_path = torrent.get("save_path", "")
            if not save_path:
                self.logger.error(f"种子 {torrent_name} 没有 save_path")
                return 0

            content_dir = self.file_ops.get_torrent_content_directory(torrent)
            if not content_dir:
                self.logger.error(f"种子 {torrent_name} 没有有效的内容目录")
                return 0

            self.logger.info(f"开始清理种子: {torrent_name}, 内容目录: {content_dir}")

            # 如果配置了删除前先禁用，先禁用文件
            if self.config.disable_before_delete:
                self.logger.info(f"在删除前先禁用文件")
                self.disable_files_for_torrent(torrent)

                # 等待一段时间确保禁用操作生效
                if self.config.disable_delay > 0:
                    time.sleep(self.config.disable_delay)

            # 清理文件和文件夹（基于文件系统扫描）
            deleted_count = self.clean_directory(content_dir)

            self.logger.info(
                f"种子 {torrent_name} 清理完成，删除了 {deleted_count} 个文件/文件夹"
            )

        except Exception as e:
            self.logger.error(f"清理种子文件时发生错误 {torrent_name}: {e}")

        return deleted_count

    def clean_directory(self, directory: str) -> int:
        """清理目录中匹配的文件和文件夹（基于文件系统）"""
        deleted_count = 0

        if not os.path.exists(directory):
            self.logger.debug(f"目录不存在: {directory}")
            return 0

        try:
            # 遍历目录下的所有项目
            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)

                if os.path.isfile(item_path):
                    # 处理文件（使用 file_patterns）
                    if self.file_ops.should_delete_file_by_name(item):
                        try:
                            os.remove(item_path)
                            self.logger.info(f"已删除文件: {item_path}")
                            deleted_count += 1
                        except Exception as e:
                            self.logger.error(f"删除文件失败 {item_path}: {e}")

                elif os.path.isdir(item_path):
                    # 处理文件夹（使用 folder_patterns）
                    if self.file_ops.should_delete_folder(item):
                        try:
                            shutil.rmtree(item_path)
                            self.logger.info(f"已删除文件夹: {item_path}")
                            deleted_count += 1
                        except Exception as e:
                            self.logger.error(f"删除文件夹失败 {item_path}: {e}")
                    else:
                        # 递归清理子目录
                        deleted_count += self.clean_directory(item_path)

            # 清理空目录（使用 FileOperations 的方法）
            self.file_ops.clean_empty_directories(directory, directory)

        except Exception as e:
            self.logger.error(f"清理目录时发生错误 {directory}: {e}")

        return deleted_count

    def _format_size(self, size_bytes: int) -> str:
        """格式化文件大小显示"""
        if size_bytes == 0:
            return "0 B"

        size_names = ["B", "KB", "MB", "GB", "TB"]
        i = 0
        while size_bytes >= 1024 and i < len(size_names) - 1:
            size_bytes /= 1024.0
            i += 1

        return f"{size_bytes:.2f} {size_names[i]}"

    def process_torrent_addition(self, torrent_hash: str, hash_file_path: str) -> str:
        """处理新添加的种子，返回状态码"""
        self.logger.info(f"处理新添加的种子: {torrent_hash}")

        try:
            # 统一验证逻辑（包含detector功能）
            torrent, status = self._get_and_validate_torrent(torrent_hash, "added")
            if status != "continue":
                return status

            torrent_name = torrent.get("name", "Unknown")

            # 检查元数据是否就绪（添加任务特有逻辑）
            if not self._check_metadata_ready(torrent_hash, torrent_name):
                return "metadata_not_ready"

            # 禁用匹配的文件（添加任务特有逻辑）
            success = self.disable_files_for_torrent(torrent)

            if success:
                self.logger.info(f"成功处理新添加种子: {torrent_name}")
                return "success"
            else:
                self.logger.warning(f"种子 {torrent_name} 文件禁用失败，需要重试")
                return "retry_later"

        except Exception as e:
            self.logger.error(f"处理新添加种子 {torrent_hash} 时发生错误: {e}")
            return "retry_later"

    def process_torrent_completion(self, torrent_hash: str, hash_file_path: str) -> str:
        """处理已完成的种子，返回状态码"""
        self.logger.info(f"处理已完成的种子: {torrent_hash}")

        try:
            # 统一验证逻辑（包含detector功能）
            torrent, status = self._get_and_validate_torrent(torrent_hash, "completed")
            if status != "continue":
                return status

            torrent_name = torrent.get("name", "Unknown")

            # 清理文件（完成任务特有逻辑）
            deleted_count = self.clean_torrent_files(torrent)
            self.logger.info(
                f"成功处理已完成种子: {torrent_name}, 删除了 {deleted_count} 个文件/文件夹"
            )

            return "success"

        except Exception as e:
            self.logger.error(f"处理已完成种子 {torrent_hash} 时发生错误: {e}")
            return "retry_later"
