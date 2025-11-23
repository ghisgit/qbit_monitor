import os
import time
import logging
import shutil
from typing import List


class EventHandler:
    def __init__(self, qbt_client, file_ops, config):
        self.qbt_client = qbt_client
        self.file_ops = file_ops
        self.config = config
        self.logger = logging.getLogger(__name__)

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

    def process_torrent_addition(self, torrent_hash: str, hash_file_path: str) -> str:
        """处理新添加的种子，返回状态码"""
        self.logger.info(f"处理新添加的种子: {torrent_hash}")

        try:
            # 获取种子信息
            torrent = self.qbt_client.get_torrent_by_hash(torrent_hash)
            if not torrent:
                self.logger.warning(f"未找到种子: {torrent_hash}，可能已被删除")
                # 种子不存在，删除哈希文件并返回成功
                if os.path.exists(hash_file_path):
                    os.remove(hash_file_path)
                    self.logger.info(f"已删除不存在的种子哈希文件: {hash_file_path}")
                return "success"

            torrent_name = torrent.get("name", "Unknown")

            # 检查是否应该处理这个种子
            if not self.should_process_torrent(torrent):
                self.logger.info(f"种子不符合处理条件: {torrent_name}")
                if os.path.exists(hash_file_path):
                    os.remove(hash_file_path)
                return "success"

            # 检查是否可以获取文件列表（相当于等待元数据）
            files = self.qbt_client.get_torrent_files(torrent_hash)
            if not files:
                self.logger.debug(f"种子 {torrent_name} 元数据未就绪，需要延迟重试")
                return "retry_later"  # 返回重试状态

            # 禁用匹配的文件（使用 disable_file_patterns）
            success = self.disable_files_for_torrent(torrent)

            if success:
                self.logger.info(f"成功处理新添加种子: {torrent_name}")
                if os.path.exists(hash_file_path):
                    os.remove(hash_file_path)
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
            # 获取种子信息
            torrent = self.qbt_client.get_torrent_by_hash(torrent_hash)
            if not torrent:
                self.logger.warning(f"未找到种子: {torrent_hash}，可能已被删除")
                # 种子不存在，删除哈希文件并返回成功
                if os.path.exists(hash_file_path):
                    os.remove(hash_file_path)
                    self.logger.info(f"已删除不存在的种子哈希文件: {hash_file_path}")
                return "success"

            torrent_name = torrent.get("name", "Unknown")

            # 检查是否应该处理这个种子
            if not self.should_process_torrent(torrent):
                self.logger.info(f"种子不符合处理条件: {torrent_name}")
                if os.path.exists(hash_file_path):
                    os.remove(hash_file_path)
                return "success"

            # 清理文件（基于文件系统，使用 file_patterns 和 folder_patterns）
            deleted_count = self.clean_torrent_files(torrent)
            self.logger.info(
                f"成功处理已完成种子: {torrent_name}, 删除了 {deleted_count} 个文件/文件夹"
            )

            if os.path.exists(hash_file_path):
                os.remove(hash_file_path)
            return "success"

        except Exception as e:
            self.logger.error(f"处理已完成种子 {torrent_hash} 时发生错误: {e}")
            return "retry_later"
