import os
import time
import logging
import shutil
from typing import List, Optional


class EventHandler:
    """事件处理器"""

    def __init__(self, qbt_client, file_ops, config):
        self.qbt_client = qbt_client
        self.file_ops = file_ops
        self.config = config
        self.logger = logging.getLogger(__name__)

    def _get_torrent(self, torrent_hash: str) -> Optional[dict]:
        """获取种子信息"""
        try:
            torrent = self.qbt_client.get_torrent_by_hash(torrent_hash)

            if not torrent:
                self.logger.warning(f"种子不存在: {torrent_hash}")
                return None

            # 转换为字典
            torrent_dict = {
                "hash": torrent.hash,
                "name": torrent.name,
                "size": torrent.size,
                "progress": torrent.progress,
                "state": torrent.state,
                "category": torrent.category,
                "save_path": torrent.save_path,
                "content_path": torrent.content_path,
            }

            return torrent_dict

        except Exception as e:
            error_msg = str(e).lower()
            if "connection" in error_msg or "timeout" in error_msg:
                self.logger.warning(f"网络错误获取种子信息: {torrent_hash}")
                raise Exception("network_error")
            else:
                self.logger.warning(f"获取种子信息失败 {torrent_hash}: {e}")
                raise Exception("qbit_api_error")

    def should_process_torrent(self, torrent: dict) -> bool:
        """判断是否应该处理这个种子"""
        torrent_category = torrent.get("category", "")

        # 如果没有配置分类，处理所有种子
        if not self.config.categories:
            return True

        # 检查种子分类是否在允许的列表中
        return torrent_category in self.config.categories

    def get_files_to_disable(self, torrent: dict) -> List[int]:
        """获取需要禁用的文件索引列表"""
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

                # 检查是否应该禁用文件下载
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
        """为种子禁用匹配的文件下载"""
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
        """清理单个种子的无用文件和文件夹"""
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

            # 清理文件和文件夹
            deleted_count = self.clean_directory(content_dir)

            self.logger.info(
                f"种子 {torrent_name} 清理完成，删除了 {deleted_count} 个文件/文件夹"
            )

        except Exception as e:
            self.logger.error(f"清理种子文件时发生错误 {torrent_name}: {e}")

        return deleted_count

    def clean_directory(self, directory: str) -> int:
        """清理目录中匹配的文件和文件夹"""
        deleted_count = 0

        if not os.path.exists(directory):
            self.logger.debug(f"目录不存在: {directory}")
            return 0

        try:
            # 遍历目录下的所有项目
            for item in os.listdir(directory):
                item_path = os.path.join(directory, item)

                if os.path.isfile(item_path):
                    # 处理文件
                    if self.file_ops.should_delete_file_by_name(item):
                        try:
                            os.remove(item_path)
                            self.logger.info(f"已删除文件: {item_path}")
                            deleted_count += 1
                        except Exception as e:
                            self.logger.error(f"删除文件失败 {item_path}: {e}")

                elif os.path.isdir(item_path):
                    # 处理文件夹
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

            # 清理空目录
            self.file_ops.clean_empty_directories(directory, directory)

        except Exception as e:
            self.logger.error(f"清理目录时发生错误 {directory}: {e}")

        return deleted_count

    def process_torrent_addition(self, torrent_hash: str) -> str:
        """处理新添加的种子 - 移除metadata_not_ready检查"""
        self.logger.info(f"处理新添加的种子: {torrent_hash}")

        try:
            # 获取种子信息
            torrent = self._get_torrent(torrent_hash)
            if not torrent:
                return "torrent_not_found"

            torrent_name = torrent.get("name", "Unknown")

            # 检查分类过滤
            if not self.should_process_torrent(torrent):
                self.logger.info(f"种子不符合处理条件: {torrent_name}")
                return "success"

            # 禁用匹配的文件
            success = self.disable_files_for_torrent(torrent)

            if success:
                if self.config.disable_after_start:
                    start_success = self.qbt_client.start_torrent(torrent_hash)
                    if not start_success:
                        self.logger.warning(f"启动种子失败: {torrent_name}")
                        return "qbit_api_error"

                self.logger.info(f"成功处理新添加种子: {torrent_name}")
                return "success"
            else:
                self.logger.warning(f"种子 {torrent_name} 文件禁用失败")
                return "qbit_api_error"

        except Exception as e:
            error_msg = str(e)
            if error_msg == "network_error":
                return "network_error"
            elif error_msg == "qbit_api_error":
                return "qbit_api_error"
            else:
                self.logger.error(f"处理新添加种子 {torrent_hash} 时发生错误: {e}")
                return "retry_later"

    def process_torrent_completion(self, torrent_hash: str) -> str:
        """处理已完成的种子"""
        self.logger.info(f"处理已完成的种子: {torrent_hash}")

        try:
            # 获取种子信息
            torrent = self._get_torrent(torrent_hash)
            if not torrent:
                return "torrent_not_found"

            torrent_name = torrent.get("name", "Unknown")

            # 检查分类过滤
            if not self.should_process_torrent(torrent):
                self.logger.info(f"种子不符合处理条件: {torrent_name}")
                return "success"

            # 清理文件
            deleted_count = self.clean_torrent_files(torrent)
            self.logger.info(
                f"成功处理已完成种子: {torrent_name}, 删除了 {deleted_count} 个文件/文件夹"
            )

            return "success"

        except Exception as e:
            error_msg = str(e)
            if error_msg == "network_error":
                return "network_error"
            elif error_msg == "qbit_api_error":
                return "qbit_api_error"
            else:
                self.logger.error(f"处理已完成种子 {torrent_hash} 时发生错误: {e}")
                return "retry_later"
