import time
import logging
from typing import Dict
from dataclasses import dataclass


@dataclass
class HealthStatus:
    """健康状态"""

    status: str  # 'healthy', 'degraded', 'unhealthy'
    response_time: float
    last_check: float
    error_message: str = None


class QBittorrentHealthChecker:
    """qBittorrent API健康检查器"""

    def __init__(self, qbt_client, check_interval: int = 30):
        self.qbt_client = qbt_client
        self.check_interval = check_interval
        self.logger = logging.getLogger(__name__)
        self.health_status = HealthStatus("healthy", 0, time.time())
        self.consecutive_failures = 0

    def check_health(self) -> HealthStatus:
        """执行健康检查"""
        current_time = time.time()

        # 检查间隔
        if current_time - self.health_status.last_check < self.check_interval:
            return self.health_status

        try:
            start_time = time.time()

            # 执行简单的API调用测试
            version = self.qbt_client.get_app_version()
            response_time = time.time() - start_time

            # 分析健康状态
            if response_time > 5.0:
                status = "degraded"
                self.consecutive_failures = 0
            else:
                status = "healthy"
                self.consecutive_failures = 0

            self.health_status = HealthStatus(
                status=status, response_time=response_time, last_check=current_time
            )

        except Exception as e:
            self.consecutive_failures += 1

            # 根据连续失败次数判断严重程度
            if self.consecutive_failures >= 3:
                status = "unhealthy"
            else:
                status = "degraded"

            self.health_status = HealthStatus(
                status=status,
                response_time=0,
                last_check=current_time,
                error_message=str(e),
            )

            self.logger.warning(
                f"qBittorrent健康检查失败: {e}, 连续失败: {self.consecutive_failures}"
            )

        return self.health_status

    def should_pause_processing(self) -> bool:
        """是否应该暂停处理"""
        health = self.check_health()
        return health.status == "unhealthy"

    def get_processing_speed_factor(self) -> float:
        """获取处理速度因子"""
        health = self.check_health()

        factors = {
            "healthy": 1.0,  # 全速处理
            "degraded": 0.3,  # 降速处理
            "unhealthy": 0.0,  # 暂停处理
        }

        return factors.get(health.status, 0.0)

    def get_system_load_status(self) -> Dict:
        """获取系统负载状态"""
        health = self.check_health()

        return {
            "health_status": health.status,
            "response_time": health.response_time,
            "consecutive_failures": self.consecutive_failures,
            "last_check": health.last_check,
            "error_message": health.error_message,
        }
