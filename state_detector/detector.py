import logging
from typing import Tuple
from persistence.task_store import Task


class StateDetector:
    """任务状态检测器 - 简化有效判断逻辑"""

    def __init__(self, qbt_client, file_ops):
        self.qbt_client = qbt_client
        self.file_ops = file_ops
        self.logger = logging.getLogger(__name__)

    def is_task_valid(self, task: Task) -> Tuple[bool, str]:
        """检查任务是否仍然有效"""
        try:
            # 使用新的方法获取种子信息和错误类型
            torrent, error_type = self.qbt_client.get_torrent_by_hash_with_error(
                task.torrent_hash
            )

            if error_type == "not_found":
                return False, "torrent_not_found"
            elif error_type:
                self.logger.warning(
                    f"qBittorrent服务暂时不可用: {task.torrent_hash}, 错误类型: {error_type}"
                )
                return True, f"qbit_unavailable_{error_type}"
            elif torrent is None:
                self.logger.warning(f"获取种子信息失败但原因未知: {task.torrent_hash}")
                return True, "unknown_error"

            # 成功获取到种子信息，种子就是有效的
            torrent_name = torrent.get("name", "Unknown")
            self.logger.debug(f"种子有效: {torrent_name}, 任务类型: {task.task_type}")

            return True, "valid"

        except Exception as e:
            self.logger.warning(f"任务验证过程异常: {task.torrent_hash}, 错误: {e}")
            return True, "validation_exception"

    def _validate_added_task(self, task: Task, torrent: dict) -> Tuple[bool, str]:
        """验证添加任务的有效性 - 已合并到主逻辑，保留方法用于兼容性"""
        return True, "valid"

    def _validate_completed_task(self, task: Task, torrent: dict) -> Tuple[bool, str]:
        """验证完成任务的有效性 - 已合并到主逻辑，保留方法用于兼容性"""
        return True, "valid"

    def should_archive_task(self, task: Task, reason: str) -> bool:
        """判断是否应该归档任务"""
        # 只有明确的种子不存在原因才归档任务
        archive_reasons = {
            "torrent_not_found",  # 种子确实不存在
        }

        # 服务问题、网络问题、验证异常等都不归档
        temporary_reasons = {
            "qbit_unavailable_api_error",
            "qbit_unavailable_network_error",
            "unknown_error",
            "validation_exception",
            "validation_error",
            "valid",  # 任务有效当然不归档
        }

        if reason in archive_reasons:
            self.logger.info(f"任务需要归档: {task.torrent_hash}, 原因: {reason}")
            return True
        elif reason in temporary_reasons:
            self.logger.debug(
                f"任务暂时无效但不归档: {task.torrent_hash}, 原因: {reason}"
            )
            return False
        else:
            # 其他未知原因，默认不归档
            self.logger.debug(
                f"任务原因未知，不归档: {task.torrent_hash}, 原因: {reason}"
            )
            return False

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

    def get_torrent_status(self, task: Task) -> dict:
        """获取种子状态信息（用于调试）"""
        try:
            torrent, error_type = self.qbt_client.get_torrent_by_hash(task.torrent_hash)
            if error_type:
                return {"status": "error", "error_type": error_type}

            return {
                "status": "found",
                "name": torrent.get("name", "Unknown"),
                "state": torrent.get("state", "unknown"),
                "progress": torrent.get("progress", 0),
                "size": torrent.get("size", 0),
                "size_formatted": self._format_size(torrent.get("size", 0)),
                "ratio": torrent.get("ratio", 0),
                "category": torrent.get("category", ""),
                "save_path": torrent.get("save_path", ""),
            }
        except Exception as e:
            return {"status": "exception", "error": str(e)}

    def get_connection_status(self) -> Tuple[bool, str]:
        """获取qBittorrent连接状态"""
        return self.qbt_client.test_connection()
