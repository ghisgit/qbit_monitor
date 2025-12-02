import random
import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from enum import Enum


class RetryStrategyType(Enum):
    """重试策略类型枚举"""

    EXPONENTIAL_BACKOFF = "exponential_backoff"
    FIXED_INTERVAL = "fixed_interval"
    LINEAR_BACKOFF = "linear_backoff"
    ADAPTIVE = "adaptive"


@dataclass
class RetryStrategyConfig:
    """重试策略配置"""

    name: str
    strategy_type: RetryStrategyType
    base_delay: int  # 基础延迟（秒）
    max_delay: int  # 最大延迟（秒）
    max_retries: Optional[int] = None  # 最大重试次数，None表示无限重试
    backoff_multiplier: float = 2.0  # 退避乘数
    jitter_factor: float = 0.1  # 随机抖动因子


class RetryStrategy:
    """重试策略基类"""

    def __init__(self, config: RetryStrategyConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)

    def calculate_delay(
        self, retry_count: int, last_error: Optional[str] = None
    ) -> float:
        """计算重试延迟"""
        raise NotImplementedError("子类必须实现此方法")

    def should_retry(self, retry_count: int, last_error: Optional[str] = None) -> bool:
        """判断是否应该继续重试"""
        if self.config.max_retries is None:
            return True
        return retry_count < self.config.max_retries

    def add_jitter(self, delay: float) -> float:
        """添加随机抖动"""
        jitter = delay * random.uniform(
            -self.config.jitter_factor, self.config.jitter_factor
        )
        return max(1, delay + jitter)  # 至少1秒


class ExponentialBackoffStrategy(RetryStrategy):
    """指数退避策略"""

    def calculate_delay(
        self, retry_count: int, last_error: Optional[str] = None
    ) -> float:
        """计算指数退避延迟"""
        if retry_count == 0:
            base_delay = self.config.base_delay
        else:
            base_delay = self.config.base_delay * (
                self.config.backoff_multiplier ** min(retry_count, 10)
            )

        # 应用最大延迟限制
        final_delay = min(base_delay, self.config.max_delay)

        # 添加抖动
        return self.add_jitter(final_delay)


class FixedIntervalStrategy(RetryStrategy):
    """固定间隔策略"""

    def calculate_delay(
        self, retry_count: int, last_error: Optional[str] = None
    ) -> float:
        """计算固定间隔延迟"""
        return self.add_jitter(self.config.base_delay)


class LinearBackoffStrategy(RetryStrategy):
    """线性退避策略"""

    def calculate_delay(
        self, retry_count: int, last_error: Optional[str] = None
    ) -> float:
        """计算线性退避延迟"""
        base_delay = self.config.base_delay * (1 + retry_count * 0.5)  # 每次增加50%
        final_delay = min(base_delay, self.config.max_delay)
        return self.add_jitter(final_delay)


class AdaptiveStrategy(RetryStrategy):
    """自适应策略"""

    def calculate_delay(
        self, retry_count: int, last_error: Optional[str] = None
    ) -> float:
        """计算自适应延迟"""
        # 根据错误类型调整基础延迟
        base_delay = self._get_adaptive_base_delay(last_error)

        # 应用退避
        if retry_count > 0:
            base_delay *= self.config.backoff_multiplier ** min(retry_count, 8)

        final_delay = min(base_delay, self.config.max_delay)
        return self.add_jitter(final_delay)

    def _get_adaptive_base_delay(self, last_error: Optional[str]) -> float:
        """根据错误类型获取自适应基础延迟"""
        if not last_error:
            return self.config.base_delay

        error_delays = {
            "qbit_api_error": 60,
            "network_error": 10,
            "torrent_not_found": 5,
        }

        for error_pattern, delay in error_delays.items():
            if error_pattern in last_error.lower():
                return delay

        return self.config.base_delay


class RetryStrategyFactory:
    """重试策略工厂"""

    @staticmethod
    def create_strategy(config: RetryStrategyConfig) -> RetryStrategy:
        """根据配置创建重试策略"""
        strategy_map = {
            RetryStrategyType.EXPONENTIAL_BACKOFF: ExponentialBackoffStrategy,
            RetryStrategyType.FIXED_INTERVAL: FixedIntervalStrategy,
            RetryStrategyType.LINEAR_BACKOFF: LinearBackoffStrategy,
            RetryStrategyType.ADAPTIVE: AdaptiveStrategy,
        }

        strategy_class = strategy_map.get(config.strategy_type)
        if not strategy_class:
            raise ValueError(f"未知的重试策略类型: {config.strategy_type}")

        return strategy_class(config)

    @staticmethod
    def get_default_strategies() -> Dict[str, RetryStrategyConfig]:
        """获取默认重试策略配置"""
        return {
            "qbit_api_error": RetryStrategyConfig(
                name="qbit_api_error",
                strategy_type=RetryStrategyType.EXPONENTIAL_BACKOFF,
                base_delay=60,
                max_delay=600,
                max_retries=None,  # 无限重试
                backoff_multiplier=2.0,
                jitter_factor=0.1,
            ),
            "network_error": RetryStrategyConfig(
                name="network_error",
                strategy_type=RetryStrategyType.LINEAR_BACKOFF,
                base_delay=10,
                max_delay=60,
                max_retries=None,  # 无限重试
                backoff_multiplier=1.2,
                jitter_factor=0.2,
            ),
            "torrent_not_found": RetryStrategyConfig(
                name="torrent_not_found",
                strategy_type=RetryStrategyType.EXPONENTIAL_BACKOFF,
                base_delay=5,
                max_delay=60,
                max_retries=3,  # 种子不存在有限重试
                backoff_multiplier=1,
                jitter_factor=0.1,
            ),
            "retry_later": RetryStrategyConfig(
                name="retry_later",
                strategy_type=RetryStrategyType.EXPONENTIAL_BACKOFF,
                base_delay=120,
                max_delay=1800,
                max_retries=None,  # 无限重试
                backoff_multiplier=2.0,
                jitter_factor=0.1,
            ),
            "processing_exception": RetryStrategyConfig(
                name="processing_exception",
                strategy_type=RetryStrategyType.EXPONENTIAL_BACKOFF,
                base_delay=30,
                max_delay=300,
                max_retries=None,  # 无限重试
                backoff_multiplier=1.5,
                jitter_factor=0.1,
            ),
        }


class StrategyManager:
    """策略管理器"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.strategies: Dict[str, RetryStrategy] = {}
        self._load_default_strategies()

    def _load_default_strategies(self):
        """加载默认策略"""
        default_configs = RetryStrategyFactory.get_default_strategies()

        for strategy_name, config in default_configs.items():
            try:
                strategy = RetryStrategyFactory.create_strategy(config)
                self.strategies[strategy_name] = strategy
                self.logger.debug(f"加载重试策略: {strategy_name}")
            except Exception as e:
                self.logger.error(f"加载策略失败 {strategy_name}: {e}")

    def get_strategy(self, failure_reason: str) -> RetryStrategy:
        """根据失败原因获取策略"""
        # 尝试精确匹配
        if failure_reason in self.strategies:
            return self.strategies[failure_reason]

        # 尝试错误类型匹配
        for strategy_name, strategy in self.strategies.items():
            if failure_reason.startswith(strategy_name):
                return strategy

        # 使用retry_later作为默认策略
        return self.strategies["retry_later"]

    def list_strategies(self) -> List[Dict[str, Any]]:
        """列出所有策略"""
        strategies_info = []
        for name, strategy in self.strategies.items():
            strategies_info.append(
                {
                    "name": name,
                    "config": asdict(strategy.config),
                    "type": strategy.config.strategy_type.value,
                }
            )
        return strategies_info

    def get_strategy_info(self, failure_reason: str) -> Dict[str, Any]:
        """获取策略信息"""
        strategy = self.get_strategy(failure_reason)
        return {
            "failure_reason": failure_reason,
            "strategy_name": strategy.config.name,
            "strategy_type": strategy.config.strategy_type.value,
            "base_delay": strategy.config.base_delay,
            "max_delay": strategy.config.max_delay,
            "max_retries": strategy.config.max_retries,
            "backoff_multiplier": strategy.config.backoff_multiplier,
            "jitter_factor": strategy.config.jitter_factor,
            "is_infinite": strategy.config.max_retries is None,
        }
