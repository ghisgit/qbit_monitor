import time
import logging
from typing import Dict


class SimpleHealthChecker:
    """简化健康检查器"""

    def __init__(self, qbt_client, check_interval: int = 30):
        self.qbt_client = qbt_client
        self.check_interval = check_interval
        self.logger = logging.getLogger(__name__)
        self.last_check = 0
        self.healthy = True
        self.consecutive_failures = 0

    def check_health(self) -> bool:
        """执行健康检查"""
        current_time = time.time()

        # 检查间隔
        if current_time - self.last_check < self.check_interval:
            return self.healthy

        try:
            # 测试连接
            version = self.qbt_client.get_app_version()
            self.healthy = True
            self.consecutive_failures = 0
            self.last_check = current_time

        except Exception as e:
            self.consecutive_failures += 1
            self.healthy = False
            self.last_check = current_time

            if self.consecutive_failures >= 3:
                self.logger.error(
                    f"qBittorrent健康检查失败: {e}, 连续失败: {self.consecutive_failures}"
                )
            else:
                self.logger.warning(f"qBittorrent健康检查失败: {e}")

        return self.healthy

    def should_pause_processing(self) -> bool:
        """是否应该暂停处理"""
        return not self.check_health()

    def get_status(self) -> Dict:
        """获取健康状态"""
        return {
            "healthy": self.healthy,
            "consecutive_failures": self.consecutive_failures,
            "last_check": self.last_check,
        }
