import time
import logging
import json
from typing import Dict, Optional
from dataclasses import dataclass
from persistence.task_store import CircuitBreakerConfig, TaskStore


@dataclass
class BreakerState:
    state: str  # 'closed', 'open', 'half_open'
    failure_count: int
    success_count: int
    last_state_change: float
    last_failure_time: Optional[float]
    last_success_time: Optional[float]


class CircuitBreaker:
    """熔断器实现"""

    def __init__(self, task_store: TaskStore):
        self.task_store = task_store
        self.logger = logging.getLogger(__name__)

        # 预定义熔断器配置
        self.breaker_configs = {
            "qbit_api": CircuitBreakerConfig(
                failure_threshold=5,
                success_threshold=3,
                timeout=60,
                half_open_timeout=30,
            ),
            "file_operations": CircuitBreakerConfig(
                failure_threshold=10,
                success_threshold=5,
                timeout=30,
                half_open_timeout=15,
            ),
            "network": CircuitBreakerConfig(
                failure_threshold=8,
                success_threshold=4,
                timeout=45,
                half_open_timeout=20,
            ),
        }

        # 初始化熔断器状态
        self._init_breaker_states()

    def _init_breaker_states(self):
        """初始化熔断器状态"""
        for breaker_type in self.breaker_configs.keys():
            state = self._load_breaker_state(breaker_type)
            if not state:
                # 创建初始状态
                self._save_breaker_state(
                    breaker_type,
                    BreakerState(
                        state="closed",
                        failure_count=0,
                        success_count=0,
                        last_state_change=time.time(),
                        last_failure_time=None,
                        last_success_time=None,
                    ),
                )

    def can_execute(self, breaker_type: str) -> bool:
        """检查是否允许执行操作"""
        state = self._load_breaker_state(breaker_type)
        config = self.breaker_configs[breaker_type]

        if not state:
            return True

        if state.state == "open":
            # 检查是否应该进入半开状态
            if time.time() - state.last_state_change > config.timeout:
                self._set_breaker_state(breaker_type, "half_open")
                return True
            return False

        elif state.state == "half_open":
            # 半开状态：允许少量请求通过进行测试
            return self._allow_half_open_request(breaker_type, state, config)

        return True  # closed状态

    def record_success(self, breaker_type: str):
        """记录成功操作"""
        state = self._load_breaker_state(breaker_type)
        config = self.breaker_configs[breaker_type]

        if not state:
            return

        if state.state == "half_open":
            new_success_count = state.success_count + 1
            if new_success_count >= config.success_threshold:
                # 成功次数达到阈值，关闭熔断器
                self._set_breaker_state(breaker_type, "closed")
            else:
                # 更新成功计数
                self._update_breaker_state(
                    breaker_type,
                    {
                        "success_count": new_success_count,
                        "last_success_time": time.time(),
                    },
                )
        else:
            # 重置失败计数
            self._update_breaker_state(
                breaker_type, {"failure_count": 0, "last_success_time": time.time()}
            )

    def record_failure(self, breaker_type: str):
        """记录失败操作"""
        state = self._load_breaker_state(breaker_type)
        config = self.breaker_configs[breaker_type]

        if not state:
            state = BreakerState(
                state="closed",
                failure_count=0,
                success_count=0,
                last_state_change=time.time(),
                last_failure_time=None,
                last_success_time=None,
            )

        new_failure_count = state.failure_count + 1

        if state.state == "half_open":
            # 半开状态下失败，立即重新打开熔断器
            self._set_breaker_state(breaker_type, "open")
        elif new_failure_count >= config.failure_threshold:
            # 达到失败阈值，打开熔断器
            self._set_breaker_state(breaker_type, "open")
        else:
            # 更新失败计数
            self._update_breaker_state(
                breaker_type,
                {"failure_count": new_failure_count, "last_failure_time": time.time()},
            )

    def _allow_half_open_request(
        self, breaker_type: str, state: BreakerState, config: CircuitBreakerConfig
    ) -> bool:
        """半开状态下是否允许请求"""
        # 简单的策略：每half_open_timeout秒允许一个请求
        current_time = time.time()
        if current_time - state.last_state_change > config.half_open_timeout:
            return True
        return state.success_count < 1  # 半开状态下至少允许一个请求

    def _load_breaker_state(self, breaker_type: str) -> Optional[BreakerState]:
        """从数据库加载熔断器状态"""
        try:
            with self.task_store.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT state, failure_count, success_count, last_state_change, "
                    "last_failure_time, last_success_time FROM circuit_break_status "
                    "WHERE breaker_type = ?",
                    (breaker_type,),
                )
                result = cursor.fetchone()

                if result:
                    return BreakerState(
                        state=result[0],
                        failure_count=result[1],
                        success_count=result[2],
                        last_state_change=result[3],
                        last_failure_time=result[4],
                        last_success_time=result[5],
                    )
                return None

        except Exception as e:
            self.logger.error(f"加载熔断器状态失败: {e}")
            return None

    def _save_breaker_state(self, breaker_type: str, state: BreakerState):
        """保存熔断器状态到数据库"""
        try:
            with self.task_store.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO circuit_break_status 
                    (breaker_type, state, failure_count, success_count, last_state_change,
                     last_failure_time, last_success_time, config, created_time, updated_time)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        breaker_type,
                        state.state,
                        state.failure_count,
                        state.success_count,
                        state.last_state_change,
                        state.last_failure_time,
                        state.last_success_time,
                        json.dumps(self.breaker_configs[breaker_type].__dict__),
                        time.time(),
                        time.time(),
                    ),
                )

        except Exception as e:
            self.logger.error(f"保存熔断器状态失败: {e}")

    def _set_breaker_state(self, breaker_type: str, new_state: str):
        """设置熔断器状态"""
        state = self._load_breaker_state(breaker_type)
        if state:
            state.state = new_state
            state.last_state_change = time.time()
            if new_state == "closed":
                state.failure_count = 0
                state.success_count = 0
            elif new_state == "half_open":
                state.success_count = 0  # 重置成功计数

            self._save_breaker_state(breaker_type, state)

            self.logger.info(f"熔断器 {breaker_type} 状态变更为: {new_state}")

    def _update_breaker_state(self, breaker_type: str, updates: Dict):
        """更新熔断器状态"""
        state = self._load_breaker_state(breaker_type)
        if state:
            for key, value in updates.items():
                setattr(state, key, value)
            self._save_breaker_state(breaker_type, state)
