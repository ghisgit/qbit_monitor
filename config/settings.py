import json
from pathlib import Path
from typing import Dict, List, Any


class Config:
    """配置管理器 - 清理未使用的配置"""

    def __init__(self, config_file: str = "config.json"):
        self.config_file = Path(config_file)
        self.config_mtime = 0
        self._logger = None
        self._config = self._load_default_config()
        self.load_config()

    def _load_default_config(self) -> Dict:
        """加载默认配置 - 只保留实际使用的配置"""
        return {
            # qBittorrent连接配置
            "host": "localhost",
            "port": 8080,
            "username": "",
            "password": "",
            # 标签配置
            "added_tag": "added",
            "completed_tag": "completed",
            "processing_tag": "processing",
            # 处理配置
            "check_interval": 5,
            "min_file_size_mb": 1,
            "file_patterns": [],
            "folder_patterns": [],
            "disable_file_patterns": [],
            "categories": [],
            "debug_mode": False,
            "disable_after_start": True,
            "disable_before_delete": True,
            "disable_delay": 1,
            # 系统配置
            "max_workers": 3,
            "batch_size": 5,
            "poll_interval": 10,
            # 停滞监控配置
            "min_stalled_minutes": 30,
            "stalled_check_interval": 300,
            "progress_threshold": 0.95,
            # 熔断器配置
            "circuit_breaker": {
                "failure_threshold": 3,
                "success_threshold": 2,
                "timeout": 60,
                "half_open_timeout": 30,
            },
        }

    def load_config(self) -> bool:
        """加载配置文件"""
        try:
            if not self.config_file.exists():
                return False

            current_mtime = self.config_file.stat().st_mtime
            if current_mtime == self.config_mtime:
                return True

            self.config_mtime = current_mtime

            with open(self.config_file, "r", encoding="utf-8") as f:
                user_config = json.load(f)

            if self._logger:
                self._logger.info("检测到配置文件修改，重新加载")

            # 只更新实际存在的配置项
            for key, value in user_config.items():
                if key in self._config:
                    # 嵌套字典的特殊处理
                    if isinstance(value, dict) and isinstance(
                        self._config.get(key), dict
                    ):
                        self._config[key].update(value)
                    else:
                        self._config[key] = value
                else:
                    if self._logger:
                        self._logger.warning(f"忽略未定义的配置项: {key}")

            return True

        except Exception as e:
            if self._logger:
                self._logger.error(f"加载配置文件失败: {e}")
            return False

    def set_logger(self, logger):
        """设置日志记录器"""
        self._logger = logger

    # 配置属性访问器 - 只保留实际使用的
    @property
    def host(self) -> str:
        return self._config["host"]

    @property
    def port(self) -> int:
        return self._config["port"]

    @property
    def username(self) -> str:
        return self._config["username"]

    @property
    def password(self) -> str:
        return self._config["password"]

    @property
    def added_tag(self) -> str:
        return self._config["added_tag"]

    @property
    def completed_tag(self) -> str:
        return self._config["completed_tag"]

    @property
    def processing_tag(self) -> str:
        return self._config["processing_tag"]

    @property
    def check_interval(self) -> int:
        return self._config["check_interval"]

    @property
    def min_file_size_mb(self) -> float:
        return self._config["min_file_size_mb"]

    @property
    def file_patterns(self) -> List[str]:
        return self._config["file_patterns"]

    @property
    def folder_patterns(self) -> List[str]:
        return self._config["folder_patterns"]

    @property
    def disable_file_patterns(self) -> List[str]:
        return self._config["disable_file_patterns"]

    @property
    def categories(self) -> List[str]:
        return self._config["categories"]

    @property
    def debug_mode(self) -> bool:
        return self._config["debug_mode"]

    @property
    def disable_after_start(self) -> bool:
        return self._config["disable_after_start"]

    @property
    def disable_before_delete(self) -> bool:
        return self._config["disable_before_delete"]

    @property
    def disable_delay(self) -> int:
        return self._config["disable_delay"]

    @property
    def max_workers(self) -> int:
        return self._config["max_workers"]

    @property
    def batch_size(self) -> int:
        return self._config["batch_size"]

    @property
    def poll_interval(self) -> int:
        return self._config["poll_interval"]

    @property
    def min_stalled_minutes(self) -> int:
        return self._config["min_stalled_minutes"]

    @property
    def stalled_check_interval(self) -> int:
        return self._config["stalled_check_interval"]

    @property
    def progress_threshold(self) -> float:
        return self._config["progress_threshold"]

    @property
    def circuit_breaker_config(self) -> Dict:
        return self._config["circuit_breaker"]

    def get_all_config(self) -> Dict[str, Any]:
        """获取所有配置（用于调试）"""
        return self._config.copy()
