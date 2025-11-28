import json
import os
from pathlib import Path
from typing import Dict, List


class Config:
    def __init__(self, config_file: str = "config.json"):
        self.config_file = Path(config_file)
        self.config_mtime = 0
        self._logger = None
        self._config = self._load_default_config()
        self.load_config()

    def set_logger(self, logger):
        """设置日志记录器"""
        self._logger = logger

    def _log_debug(self, message: str):
        """记录调试日志"""
        if self._logger and hasattr(self._logger, "debug"):
            self._logger.debug(message)

    def _log_info(self, message: str):
        """记录信息日志"""
        if self._logger and hasattr(self._logger, "info"):
            self._logger.info(message)

    def _load_default_config(self) -> Dict:
        """加载默认配置"""
        return {
            "host": "localhost",
            "port": 8080,
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
            "max_workers": 5,
            "batch_size": 10,
            "poll_interval": 10,
        }

    def load_config(self) -> bool:
        """加载配置文件，只在有修改时记录日志"""
        try:
            if not self.config_file.exists():
                return False

            current_mtime = self.config_file.stat().st_mtime
            if current_mtime == self.config_mtime:
                return True  # 配置未修改，不记录日志

            self.config_mtime = current_mtime

            with open(self.config_file, "r", encoding="utf-8") as f:
                user_config = json.load(f)

            # 记录配置修改
            if self._logger:
                self._log_info("检测到配置文件修改，重新加载")

                # 在debug模式下打印修改项
                if hasattr(self, "debug_mode") and self.debug_mode:
                    changed_keys = []
                    for key, value in user_config.items():
                        if key in self._config and self._config[key] != value:
                            changed_keys.append(key)

                    if changed_keys:
                        self._log_debug(f"修改的配置项: {changed_keys}")

            self._config.update(user_config)
            return True

        except Exception as e:
            if self._logger:
                self._log_info(f"加载配置文件失败: {e}")
            return False

    @property
    def host(self) -> str:
        return self._config["host"]

    @property
    def port(self) -> int:
        return self._config["port"]

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
