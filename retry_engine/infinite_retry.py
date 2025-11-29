import time
import logging
from typing import Dict, Optional
from persistence.task_store import Task
from .strategies import StrategyManager, RetryStrategy


class InfiniteRetryEngine:
    """无限重试引擎 - 使用策略系统"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.strategy_manager = StrategyManager()

    def calculate_next_retry(self, task: Task, failure_reason: str) -> Optional[float]:
        """计算下次重试时间"""
        strategy = self.strategy_manager.get_strategy(failure_reason)

        # 检查是否应该继续重试
        if not strategy.should_retry(task.retry_count, failure_reason):
            return None

        # 计算延迟
        delay = strategy.calculate_delay(task.retry_count, failure_reason)
        next_retry = time.time() + delay

        self.logger.debug(
            f"计算重试延迟: 任务={task.torrent_hash}, "
            f"原因={failure_reason}, 重试次数={task.retry_count}, "
            f"策略={strategy.config.name}, 延迟={delay:.1f}秒"
        )

        return next_retry

    def should_continue_retry(self, task: Task, failure_reason: str) -> bool:
        """判断是否应该继续重试"""
        strategy = self.strategy_manager.get_strategy(failure_reason)
        return strategy.should_retry(task.retry_count, failure_reason)

    def get_retry_strategy_info(self, failure_reason: str) -> Dict:
        """获取重试策略信息"""
        return self.strategy_manager.get_strategy_info(failure_reason)

    def list_all_strategies(self):
        """列出所有可用策略"""
        return self.strategy_manager.list_strategies()
