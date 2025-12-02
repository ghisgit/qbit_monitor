import time
import logging
from typing import Dict


class SimpleCircuitBreaker:
    """简化熔断器 - 内存实现"""

    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)

        # 熔断器状态
        self.state = "closed"  # 'closed', 'open', 'half_open'
        self.failure_count = 0
        self.success_count = 0
        self.last_state_change = time.time()

        # 配置
        self.failure_threshold = config.circuit_breaker_config.get(
            "failure_threshold", 3
        )
        self.success_threshold = config.circuit_breaker_config.get(
            "success_threshold", 2
        )
        self.timeout = config.circuit_breaker_config.get("timeout", 60)
        self.half_open_timeout = config.circuit_breaker_config.get(
            "half_open_timeout", 30
        )

    def can_execute(self) -> bool:
        """检查是否允许执行操作"""
        current_time = time.time()

        if self.state == "open":
            # 检查是否应该进入半开状态
            if current_time - self.last_state_change > self.timeout:
                self._set_state("half_open")
                self.logger.warning("熔断器进入半开状态")
                return True
            self.logger.debug("熔断器处于开启状态，拒绝请求")
            return False

        elif self.state == "half_open":
            # 半开状态：允许少量请求通过进行测试
            if current_time - self.last_state_change > self.half_open_timeout:
                return True
            return self.success_count < 1  # 半开状态下至少允许一个请求

        return True  # closed状态

    def record_success(self):
        """记录成功操作"""
        self.logger.debug("熔断器记录成功")

        if self.state == "half_open":
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                # 成功次数达到阈值，关闭熔断器
                self._set_state("closed")
                self.logger.info("熔断器成功恢复，转为关闭状态")
        else:
            # 在关闭状态下，重置失败计数
            self.failure_count = 0

    def record_failure(self):
        """记录失败操作"""
        self.failure_count += 1
        self.logger.warning(f"熔断器记录失败，失败次数: {self.failure_count}")

        if self.state == "half_open":
            # 半开状态下失败，立即重新打开熔断器
            self._set_state("open")
            self.logger.error("熔断器半开状态下失败，重新开启")
        elif self.failure_count >= self.failure_threshold:
            # 达到失败阈值，打开熔断器
            self._set_state("open")
            self.logger.error("熔断器达到失败阈值，开启熔断")

    def _set_state(self, new_state: str):
        """设置熔断器状态"""
        old_state = self.state
        self.state = new_state
        self.last_state_change = time.time()

        if new_state == "closed":
            self.failure_count = 0
            self.success_count = 0
        elif new_state == "half_open":
            self.success_count = 0

        self.logger.info(f"熔断器状态从 {old_state} 变更为: {new_state}")

    def get_status(self) -> Dict:
        """获取熔断器状态"""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "last_state_change": self.last_state_change,
        }
