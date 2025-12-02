import time
import logging
from typing import Optional
from persistence.task_store import Task
from .strategies import StrategyManager


class RetryEngine:
    """重试引擎 - 使用策略管理器"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.strategy_manager = StrategyManager()

    def calculate_next_retry(self, task: Task, failure_reason: str) -> Optional[float]:
        """计算下次重试时间 - 使用策略管理器"""
        try:
            # 获取对应的重试策略
            strategy = self.strategy_manager.get_strategy(failure_reason)

            # 检查是否应该继续重试
            if not strategy.should_retry(task.retry_count, failure_reason):
                self.logger.debug(
                    f"任务已达到最大重试次数: {task.torrent_hash}, "
                    f"原因: {failure_reason}, 重试次数: {task.retry_count}"
                )
                return None

            # 使用策略计算延迟
            delay = strategy.calculate_delay(task.retry_count, failure_reason)
            next_retry = time.time() + delay

            self.logger.debug(
                f"计算重试延迟: 任务={task.torrent_hash}, "
                f"原因={failure_reason}, 重试次数={task.retry_count}, "
                f"策略={strategy.config.name}, 延迟={delay:.1f}秒"
            )

            return next_retry

        except Exception as e:
            self.logger.error(f"计算重试时间失败: {e}")
            # 出错时使用默认延迟
            default_delay = 60
            return time.time() + default_delay

    def should_continue_retry(self, task: Task, failure_reason: str) -> bool:
        """判断是否应该继续重试"""
        try:
            strategy = self.strategy_manager.get_strategy(failure_reason)
            return strategy.should_retry(task.retry_count, failure_reason)
        except Exception as e:
            self.logger.error(f"检查重试状态失败: {e}")
            return True  # 出错时默认继续重试

    def get_retry_info(self, failure_reason: str) -> dict:
        """获取重试策略信息"""
        try:
            return self.strategy_manager.get_strategy_info(failure_reason)
        except Exception as e:
            self.logger.error(f"获取重试策略信息失败: {e}")
            return {}
