"""
配置文件管理器
提供配置加载、验证和管理功能
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum


class ConfigError(Exception):
    """配置相关异常"""

    pass


class LogLevel(str, Enum):
    """日志级别枚举"""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class QBittorrentConfig:
    """qBittorrent连接配置"""

    host: str = "localhost"
    port: int = 8080
    username: str = ""
    password: str = ""

    def validate(self):
        """验证配置有效性"""
        if not self.host:
            raise ConfigError("qBittorrent主机地址不能为空")

        if not 1 <= self.port <= 65535:
            raise ConfigError(f"端口号必须在1-65535之间: {self.port}")

        return True

    def get_connection_string(self) -> str:
        """获取连接字符串"""
        auth_part = f"{self.username}:{self.password}@" if self.username else ""
        return f"http://{auth_part}{self.host}:{self.port}"


@dataclass
class TagConfig:
    """标签配置"""

    added: str = "added"
    completed: str = "completed"
    processing: str = "processing"

    def validate(self):
        """验证标签配置"""
        required_tags = [self.added, self.completed, self.processing]

        # 检查标签是否为空
        for tag_name, tag_value in [
            ("added", self.added),
            ("completed", self.completed),
            ("processing", self.processing),
        ]:
            if not tag_value.strip():
                raise ConfigError(f"{tag_name}标签不能为空")

        # 检查标签是否重复
        if len(set(required_tags)) != 3:
            raise ConfigError("标签名称不能重复")

        return True


@dataclass
class FilePatternsConfig:
    """文件模式配置"""

    file_patterns: List[str] = None
    folder_patterns: List[str] = None
    disable_file_patterns: List[str] = None

    def __post_init__(self):
        """初始化默认值"""
        if self.file_patterns is None:
            self.file_patterns = []
        if self.folder_patterns is None:
            self.folder_patterns = []
        if self.disable_file_patterns is None:
            self.disable_file_patterns = []

    def validate(self):
        """验证正则表达式模式"""
        patterns_to_validate = [
            ("file_patterns", self.file_patterns),
            ("folder_patterns", self.folder_patterns),
            ("disable_file_patterns", self.disable_file_patterns),
        ]

        for pattern_name, patterns in patterns_to_validate:
            for pattern in patterns:
                try:
                    re.compile(pattern)
                except re.error as e:
                    raise ConfigError(
                        f"{pattern_name}中的正则表达式错误 '{pattern}': {e}"
                    )

        return True

    def get_pattern_summary(self) -> Dict[str, int]:
        """获取模式统计摘要"""
        return {
            "file_patterns": len(self.file_patterns),
            "folder_patterns": len(self.folder_patterns),
            "disable_file_patterns": len(self.disable_file_patterns),
        }


@dataclass
class TaskConfig:
    """任务处理配置"""

    max_workers: int = 3
    poll_interval: int = 10
    check_interval: int = 5

    def validate(self):
        """验证任务配置"""
        if self.max_workers < 1:
            raise ConfigError(f"工作线程数必须大于0: {self.max_workers}")

        if self.poll_interval < 1:
            raise ConfigError(f"扫描间隔必须大于0: {self.poll_interval}")

        if self.check_interval < 1:
            raise ConfigError(f"检查间隔必须大于0: {self.check_interval}")

        return True


@dataclass
class StalledMonitorConfig:
    """停滞种子监控配置"""

    min_stalled_minutes: int = 30
    stalled_check_interval: int = 300
    progress_threshold: float = 0.95

    def validate(self):
        """验证监控配置"""
        if self.min_stalled_minutes < 1:
            raise ConfigError(f"停滞分钟数必须大于0: {self.min_stalled_minutes}")

        if self.stalled_check_interval < 10:
            raise ConfigError(
                f"监控检查间隔必须至少10秒: {self.stalled_check_interval}"
            )

        if not 0 <= self.progress_threshold <= 1:
            raise ConfigError(f"进度阈值必须在0-1之间: {self.progress_threshold}")

        return True

    def get_stalled_seconds(self) -> int:
        """获取停滞秒数"""
        return self.min_stalled_minutes * 60


@dataclass
class LogConfig:
    """日志配置"""

    debug_mode: bool = False
    log_file: str = "logs/qbit_monitor.log"
    log_level: LogLevel = LogLevel.INFO

    def validate(self):
        """验证日志配置"""
        if not self.log_file:
            raise ConfigError("日志文件路径不能为空")

        # 确保日志扩展名正确
        if not self.log_file.endswith(".log"):
            self.log_file = f"{self.log_file}.log"

        return True

    def get_log_level(self) -> int:
        """获取日志级别对应的数值"""
        level_map = {
            LogLevel.DEBUG: 10,  # logging.DEBUG
            LogLevel.INFO: 20,  # logging.INFO
            LogLevel.WARNING: 30,  # logging.WARNING
            LogLevel.ERROR: 40,  # logging.ERROR
        }
        return level_map.get(self.log_level, 20)

    def get_effective_log_level(self) -> int:
        """获取实际生效的日志级别（考虑debug_mode）"""
        if self.debug_mode:
            return 10  # logging.DEBUG
        return self.get_log_level()


@dataclass
class DatabaseConfig:
    """数据库配置"""

    db_file: str = "data/tasks.db"

    def validate(self):
        """验证数据库配置"""
        if not self.db_file:
            raise ConfigError("数据库文件路径不能为空")

        # 确保路径合法
        db_path = Path(self.db_file)
        if db_path.is_absolute() and not db_path.parent.exists():
            raise ConfigError(f"数据库目录不存在: {db_path.parent}")

        # 确保文件扩展名正确
        if not self.db_file.endswith(".db"):
            self.db_file = f"{self.db_file}.db"

        return True

    def get_db_path(self) -> Path:
        """获取数据库路径对象"""
        return Path(self.db_file)

    def get_db_dir(self) -> Path:
        """获取数据库目录"""
        return self.get_db_path().parent

    def ensure_directories(self):
        """确保数据库目录存在"""
        db_dir = self.get_db_dir()
        db_dir.mkdir(parents=True, exist_ok=True)


@dataclass
class Config:
    """
    主配置类
    整合所有子配置并提供统一接口
    """

    # 子配置
    qbittorrent: QBittorrentConfig
    tags: TagConfig
    patterns: FilePatternsConfig
    tasks: TaskConfig
    stalled_monitor: StalledMonitorConfig
    log: LogConfig
    database: DatabaseConfig

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "Config":
        """
        从字典创建配置对象

        Args:
            config_dict: 配置字典

        Returns:
            Config: 配置对象
        """
        # 提取各部分的配置
        qb_config = cls._extract_qbittorrent_config(config_dict)
        tag_config = cls._extract_tag_config(config_dict)
        pattern_config = cls._extract_pattern_config(config_dict)
        task_config = cls._extract_task_config(config_dict)
        stalled_config = cls._extract_stalled_config(config_dict)
        log_config = cls._extract_log_config(config_dict)
        db_config = cls._extract_database_config(config_dict)

        # 创建配置对象
        return cls(
            qbittorrent=qb_config,
            tags=tag_config,
            patterns=pattern_config,
            tasks=task_config,
            stalled_monitor=stalled_config,
            log=log_config,
            database=db_config,
        )

    @staticmethod
    def _extract_qbittorrent_config(data: Dict[str, Any]) -> QBittorrentConfig:
        """提取qBittorrent配置"""
        return QBittorrentConfig(
            host=data.get("host", "localhost"),
            port=data.get("port", 8080),
            username=data.get("username", ""),
            password=data.get("password", ""),
        )

    @staticmethod
    def _extract_tag_config(data: Dict[str, Any]) -> TagConfig:
        """提取标签配置"""
        return TagConfig(
            added=data.get("added_tag", "added"),
            completed=data.get("completed_tag", "completed"),
            processing=data.get("processing_tag", "processing"),
        )

    @staticmethod
    def _extract_pattern_config(data: Dict[str, Any]) -> FilePatternsConfig:
        """提取模式配置"""
        return FilePatternsConfig(
            file_patterns=data.get("file_patterns", []),
            folder_patterns=data.get("folder_patterns", []),
            disable_file_patterns=data.get("disable_file_patterns", []),
        )

    @staticmethod
    def _extract_task_config(data: Dict[str, Any]) -> TaskConfig:
        """提取任务配置"""
        return TaskConfig(
            max_workers=data.get("max_workers", 3),
            poll_interval=data.get("poll_interval", 10),
            check_interval=data.get("check_interval", 5),
        )

    @staticmethod
    def _extract_stalled_config(data: Dict[str, Any]) -> StalledMonitorConfig:
        """提取停滞监控配置"""
        return StalledMonitorConfig(
            min_stalled_minutes=data.get("min_stalled_minutes", 30),
            stalled_check_interval=data.get("stalled_check_interval", 300),
            progress_threshold=data.get("progress_threshold", 0.95),
        )

    @staticmethod
    def _extract_log_config(data: Dict[str, Any]) -> LogConfig:
        """提取日志配置"""
        debug_mode = data.get("debug_mode", False)

        # 根据debug_mode自动设置log_level
        log_level = LogLevel.DEBUG if debug_mode else LogLevel.INFO

        return LogConfig(
            debug_mode=debug_mode,
            log_file=data.get("log_file", "logs/qbit_monitor.log"),
            log_level=log_level,
        )

    @staticmethod
    def _extract_database_config(data: Dict[str, Any]) -> DatabaseConfig:
        """提取数据库配置"""
        return DatabaseConfig(db_file=data.get("db_file", "data/tasks.db"))

    def validate(self) -> bool:
        """
        验证所有配置

        Returns:
            bool: 配置是否有效

        Raises:
            ConfigError: 配置验证失败
        """
        validators = [
            self.qbittorrent.validate,
            self.tags.validate,
            self.patterns.validate,
            self.tasks.validate,
            self.stalled_monitor.validate,
            self.log.validate,
            self.database.validate,
        ]

        for validator in validators:
            validator()

        return True

    def get_summary(self) -> Dict[str, Any]:
        """
        获取配置摘要

        Returns:
            Dict[str, Any]: 配置摘要
        """
        return {
            "qbittorrent": {
                "host": self.qbittorrent.host,
                "port": self.qbittorrent.port,
                "username": self.qbittorrent.username,
                "has_password": bool(self.qbittorrent.password),
            },
            "tags": asdict(self.tags),
            "patterns": self.patterns.get_pattern_summary(),
            "tasks": asdict(self.tasks),
            "stalled_monitor": {
                "min_stalled_minutes": self.stalled_monitor.min_stalled_minutes,
                "stalled_check_interval": self.stalled_monitor.stalled_check_interval,
                "progress_threshold": self.stalled_monitor.progress_threshold,
            },
            "log": {
                "debug_mode": self.log.debug_mode,
                "log_file": self.log.log_file,
                "log_level": self.log.log_level.value,
            },
            "database": {"db_file": self.database.db_file},
        }

    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典格式

        Returns:
            Dict[str, Any]: 字典格式的配置
        """
        return {
            "host": self.qbittorrent.host,
            "port": self.qbittorrent.port,
            "username": self.qbittorrent.username,
            "password": self.qbittorrent.password,
            "added_tag": self.tags.added,
            "completed_tag": self.tags.completed,
            "processing_tag": self.tags.processing,
            "file_patterns": self.patterns.file_patterns,
            "folder_patterns": self.patterns.folder_patterns,
            "disable_file_patterns": self.patterns.disable_file_patterns,
            "max_workers": self.tasks.max_workers,
            "poll_interval": self.tasks.poll_interval,
            "check_interval": self.tasks.check_interval,
            "min_stalled_minutes": self.stalled_monitor.min_stalled_minutes,
            "stalled_check_interval": self.stalled_monitor.stalled_check_interval,
            "progress_threshold": self.stalled_monitor.progress_threshold,
            "debug_mode": self.log.debug_mode,
            "log_file": self.log.log_file,
            "db_file": self.database.db_file,
        }


class ConfigManager:
    """
    配置管理器
    负责配置的加载、保存和验证
    """

    # 默认配置模板
    DEFAULT_CONFIG_TEMPLATE = {
        "host": "localhost",
        "port": 8080,
        "username": "",
        "password": "",
        "added_tag": "added",
        "completed_tag": "completed",
        "processing_tag": "processing",
        "file_patterns": [
            r"\.sample\.(mp4|mkv|avi)$",
            r"\.nfo$",
            r"\.txt$",
            r"sample\.mp4$",
            r"\.sfv$",
            r"\.url$",
        ],
        "folder_patterns": [
            r"sample$",
            r"extras$",
            r"proof$",
            r"subs$",
            r"@eaDir$",
            r"\.AppleDouble$",
        ],
        "disable_file_patterns": [r"\.srt$", r"\.idx$", r"\.sub$"],
        "max_workers": 3,
        "poll_interval": 10,
        "check_interval": 5,
        "min_stalled_minutes": 30,
        "stalled_check_interval": 300,
        "progress_threshold": 0.95,
        "debug_mode": False,
        "log_file": "logs/qbit_monitor.log",
        "db_file": "data/tasks.db",
    }

    def __init__(self, config_file: str = "config.json"):
        """
        初始化配置管理器

        Args:
            config_file: 配置文件路径
        """
        self.config_file = Path(config_file)
        self.config: Optional[Config] = None

    def load(self) -> Config:
        """
        加载配置

        Returns:
            Config: 配置对象

        Raises:
            ConfigError: 配置加载失败
            FileNotFoundError: 配置文件不存在
        """
        try:
            # 检查配置文件是否存在
            if not self.config_file.exists():
                raise FileNotFoundError(f"配置文件不存在: {self.config_file}")

            # 读取配置文件
            with open(self.config_file, "r", encoding="utf-8") as f:
                config_data = json.load(f)

            # 创建配置对象
            self.config = Config.from_dict(config_data)

            # 验证配置
            self.config.validate()

            return self.config

        except json.JSONDecodeError as e:
            raise ConfigError(f"配置文件JSON格式错误: {e}")
        except Exception as e:
            raise ConfigError(f"加载配置文件失败: {e}")

    def create_default_config(self, overwrite: bool = False) -> bool:
        """
        创建默认配置文件

        Args:
            overwrite: 是否覆盖已存在的文件

        Returns:
            bool: 是否成功创建
        """
        try:
            # 检查文件是否已存在
            if self.config_file.exists() and not overwrite:
                return False

            # 确保目录存在
            self.config_file.parent.mkdir(parents=True, exist_ok=True)

            # 写入默认配置
            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(self.DEFAULT_CONFIG_TEMPLATE, f, indent=2, ensure_ascii=False)

            return True

        except Exception as e:
            raise ConfigError(f"创建默认配置文件失败: {e}")

    def save(self, config: Config) -> bool:
        """
        保存配置

        Args:
            config: 配置对象

        Returns:
            bool: 是否成功保存
        """
        try:
            # 确保目录存在
            self.config_file.parent.mkdir(parents=True, exist_ok=True)

            # 转换为字典并保存
            config_dict = config.to_dict()

            with open(self.config_file, "w", encoding="utf-8") as f:
                json.dump(config_dict, f, indent=2, ensure_ascii=False)

            return True

        except Exception as e:
            raise ConfigError(f"保存配置文件失败: {e}")

    def validate_config_file(self) -> Dict[str, Any]:
        """
        验证配置文件

        Returns:
            Dict[str, Any]: 验证结果
        """
        result = {"valid": False, "errors": [], "config": None}

        try:
            # 尝试加载配置
            config = self.load()

            result["valid"] = True
            result["config"] = config.get_summary()

        except ConfigError as e:
            result["errors"].append(str(e))
        except FileNotFoundError as e:
            result["errors"].append(str(e))
        except Exception as e:
            result["errors"].append(f"未知错误: {e}")

        return result

    def get_config_info(self) -> Dict[str, Any]:
        """
        获取配置信息

        Returns:
            Dict[str, Any]: 配置信息
        """
        info = {
            "config_file": str(self.config_file),
            "exists": self.config_file.exists(),
            "size": self.config_file.stat().st_size if self.config_file.exists() else 0,
            "last_modified": (
                self.config_file.stat().st_mtime if self.config_file.exists() else 0
            ),
        }

        if self.config:
            info["config_summary"] = self.config.get_summary()

        return info


class SimpleConfig:
    """
    简化配置接口（向后兼容）
    提供与旧版本相同的属性访问方式
    """

    def __init__(self, config_file: str = "config.json"):
        """
        初始化简化配置

        Args:
            config_file: 配置文件路径
        """
        self._manager = ConfigManager(config_file)
        self._config = None
        self._load_config()

    def _load_config(self):
        """加载配置"""
        try:
            self._config = self._manager.load()
        except FileNotFoundError:
            # 如果配置文件不存在，使用默认配置创建对象
            self._config = Config.from_dict(self._manager.DEFAULT_CONFIG_TEMPLATE)

    # === qBittorrent配置 ===
    @property
    def host(self) -> str:
        return self._config.qbittorrent.host

    @property
    def port(self) -> int:
        return self._config.qbittorrent.port

    @property
    def username(self) -> str:
        return self._config.qbittorrent.username

    @property
    def password(self) -> str:
        return self._config.qbittorrent.password

    # === 标签配置 ===
    @property
    def added_tag(self) -> str:
        return self._config.tags.added

    @property
    def completed_tag(self) -> str:
        return self._config.tags.completed

    @property
    def processing_tag(self) -> str:
        return self._config.tags.processing

    # === 文件模式配置 ===
    @property
    def file_patterns(self) -> List[str]:
        return self._config.patterns.file_patterns

    @property
    def folder_patterns(self) -> List[str]:
        return self._config.patterns.folder_patterns

    @property
    def disable_file_patterns(self) -> List[str]:
        return self._config.patterns.disable_file_patterns

    # === 任务配置 ===
    @property
    def max_workers(self) -> int:
        return self._config.tasks.max_workers

    @property
    def poll_interval(self) -> int:
        return self._config.tasks.poll_interval

    @property
    def check_interval(self) -> int:
        return self._config.tasks.check_interval

    # === 停滞监控配置 ===
    @property
    def min_stalled_minutes(self) -> int:
        return self._config.stalled_monitor.min_stalled_minutes

    @property
    def stalled_check_interval(self) -> int:
        return self._config.stalled_monitor.stalled_check_interval

    @property
    def progress_threshold(self) -> float:
        return self._config.stalled_monitor.progress_threshold

    # === 日志配置 ===
    @property
    def debug_mode(self) -> bool:
        return self._config.log.debug_mode

    @property
    def log_file(self) -> str:
        return self._config.log.log_file

    # === 数据库配置 ===
    @property
    def db_file(self) -> str:
        return self._config.database.db_file

    def get_all_config(self) -> Dict[str, Any]:
        """获取所有配置（向后兼容）"""
        return self._config.to_dict() if self._config else {}


# 导出常用类
__all__ = [
    "Config",
    "ConfigManager",
    "SimpleConfig",
    "ConfigError",
    "QBittorrentConfig",
    "TagConfig",
    "FilePatternsConfig",
    "TaskConfig",
    "StalledMonitorConfig",
    "LogConfig",
]
