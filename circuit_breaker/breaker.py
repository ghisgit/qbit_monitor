import time
import logging
import json
from typing import Dict, Optional
from dataclasses import dataclass
from persistence.task_store import CircuitBreakerConfig, TaskStore


@dataclass
class BreakerState:
    """熔断器状态"""

    state: str  # 'closed', 'open', 'half_open'
    failure_count: int
    success_count: int
    last_state_change: float
    last_failure_time: Optional[float]
    last_success_time: Optional[float]


class CircuitBreaker:
    """熔断器实现 - 防止级联故障"""

    def __init__(self, task_store: TaskStore):
        self.task_store = task_store
        self.logger = logging.getLogger(__name__)

        # 预定义熔断器配置
        self.breaker_configs = {
            "qbit_api": CircuitBreakerConfig(
                failure_threshold=3,  # 降低阈值以便更快触发
                success_threshold=2,
                timeout=60,
                half_open_timeout=30,
            ),
            "file_operations": CircuitBreakerConfig(
                failure_threshold=5,
                success_threshold=3,
                timeout=30,
                half_open_timeout=15,
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
        """检查是否允许执行操作 - 核心熔断逻辑"""
        state = self._load_breaker_state(breaker_type)
        config = self.breaker_configs[breaker_type]

        if not state:
            return True

        current_time = time.time()

        if state.state == "open":
            # 检查是否应该进入半开状态
            if current_time - state.last_state_change > config.timeout:
                self._set_breaker_state(breaker_type, "half_open")
                self.logger.warning(f"熔断器 {breaker_type} 进入半开状态")
                return True
            self.logger.debug(f"熔断器 {breaker_type} 处于开启状态，拒绝请求")
            return False

        elif state.state == "half_open":
            # 半开状态：允许少量请求通过进行测试
            if self._allow_half_open_request(breaker_type, state, config):
                self.logger.debug(f"熔断器 {breaker_type} 半开状态允许测试请求")
                return True
            else:
                self.logger.debug(f"熔断器 {breaker_type} 半开状态限制请求")
                return False

        return True  # closed状态

    def record_success(self, breaker_type: str):
        """记录成功操作"""
        state = self._load_breaker_state(breaker_type)
        config = self.breaker_configs[breaker_type]

        if not state:
            return

        self.logger.debug(f"熔断器 {breaker_type} 记录成功")

        if state.state == "half_open":
            new_success_count = state.success_count + 1
            if new_success_count >= config.success_threshold:
                # 成功次数达到阈值，关闭熔断器
                self._set_breaker_state(breaker_type, "closed")
                self.logger.info(f"熔断器 {breaker_type} 成功恢复，转为关闭状态")
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
            # 在关闭状态下，重置失败计数
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
        self.logger.warning(
            f"熔断器 {breaker_type} 记录失败，失败次数: {new_failure_count}"
        )

        if state.state == "half_open":
            # 半开状态下失败，立即重新打开熔断器
            self._set_breaker_state(breaker_type, "open")
            self.logger.error(f"熔断器 {breaker_type} 半开状态下失败，重新开启")
        elif new_failure_count >= config.failure_threshold:
            # 达到失败阈值，打开熔断器
            self._set_breaker_state(breaker_type, "open")
            self.logger.error(f"熔断器 {breaker_type} 达到失败阈值，开启熔断")
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
        current_time = time.time()

        # 策略1：每half_open_timeout秒允许一个请求
        if current_time - state.last_state_change > config.half_open_timeout:
            return True

        # 策略2：限制半开状态下的并发请求数
        max_half_open_requests = 1
        return state.success_count < max_half_open_requests

    def get_breaker_status(self, breaker_type: str) -> Dict:
        """获取熔断器状态信息"""
        state = self._load_breaker_state(breaker_type)
        if not state:
            return {"state": "unknown", "error": "state_not_found"}

        config = self.breaker_configs.get(breaker_type, {})
        return {
            "state": state.state,
            "failure_count": state.failure_count,
            "success_count": state.success_count,
            "last_state_change": state.last_state_change,
            "last_failure_time": state.last_failure_time,
            "last_success_time": state.last_success_time,
            "config": config.__dict__ if config else {},
        }

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
            old_state = state.state
            state.state = new_state
            state.last_state_change = time.time()

            if new_state == "closed":
                state.failure_count = 0
                state.success_count = 0
            elif new_state == "half_open":
                state.success_count = 0  # 重置成功计数

            self._save_breaker_state(breaker_type, state)
            self.logger.info(
                f"熔断器 {breaker_type} 状态从 {old_state} 变更为: {new_state}"
            )

    def _update_breaker_state(self, breaker_type: str, updates: Dict):
        """更新熔断器状态"""
        state = self._load_breaker_state(breaker_type)
        if state:
            for key, value in updates.items():
                setattr(state, key, value)
            self._save_breaker_state(breaker_type, state)
