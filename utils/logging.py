"""
日志配置模块
提供统一的日志配置和RotatingFileHandler支持
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Optional


class LogConfig:
    """日志配置管理器"""

    # 默认配置
    DEFAULT_CONFIG = {
        "log_file": "logs/qbit_monitor.log",
        "max_bytes": 10 * 1024 * 1024,  # 10MB
        "backup_count": 5,
        "encoding": "utf-8",
        "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        "date_format": "%Y-%m-%d %H:%M:%S",
    }

    @staticmethod
    def setup_logging(
        log_file: Optional[str] = None,
        debug_mode: bool = False,
        max_bytes: Optional[int] = None,
        backup_count: Optional[int] = None,
    ) -> logging.Logger:
        """
        设置日志配置

        Args:
            log_file: 日志文件路径，None则使用默认路径
            debug_mode: 是否为调试模式
            max_bytes: 日志文件最大字节数
            backup_count: 备份文件数量

        Returns:
            logging.Logger: 配置好的根日志记录器
        """
        # 确定日志文件路径
        if log_file is None:
            log_file = LogConfig.DEFAULT_CONFIG["log_file"]

        # 确保日志目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        # 设置日志级别
        log_level = logging.DEBUG if debug_mode else logging.INFO

        # 获取根日志记录器
        logger = logging.getLogger()
        logger.setLevel(log_level)

        # 清除已有处理器
        LogConfig._clear_existing_handlers(logger)

        # 添加文件处理器
        file_handler = LogConfig._create_file_handler(
            log_path, log_level, max_bytes, backup_count
        )
        logger.addHandler(file_handler)

        # 添加控制台处理器
        console_handler = LogConfig._create_console_handler(log_level)
        logger.addHandler(console_handler)

        # 记录初始化信息
        logger.info(f"日志系统初始化完成 - 级别: {logging.getLevelName(log_level)}")
        logger.info(
            f"日志文件: {log_path} (最大: {max_bytes or LogConfig.DEFAULT_CONFIG['max_bytes']} 字节)"
        )

        return logger

    @staticmethod
    def _clear_existing_handlers(logger: logging.Logger):
        """清除已有的日志处理器"""
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    @staticmethod
    def _create_file_handler(
        log_path: Path,
        level: int,
        max_bytes: Optional[int] = None,
        backup_count: Optional[int] = None,
    ) -> logging.Handler:
        """
        创建文件处理器

        Args:
            log_path: 日志文件路径
            level: 日志级别
            max_bytes: 文件最大字节数
            backup_count: 备份文件数量

        Returns:
            logging.Handler: 文件处理器
        """
        # 使用默认值或传入值
        max_bytes = max_bytes or LogConfig.DEFAULT_CONFIG["max_bytes"]
        backup_count = backup_count or LogConfig.DEFAULT_CONFIG["backup_count"]

        # 创建RotatingFileHandler
        handler = logging.handlers.RotatingFileHandler(
            filename=str(log_path),
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding=LogConfig.DEFAULT_CONFIG["encoding"],
        )

        handler.setLevel(level)
        handler.setFormatter(LogConfig._create_formatter())

        return handler

    @staticmethod
    def _create_console_handler(level: int) -> logging.Handler:
        """
        创建控制台处理器

        Args:
            level: 日志级别

        Returns:
            logging.Handler: 控制台处理器
        """
        handler = logging.StreamHandler()
        handler.setLevel(level)

        # 控制台使用简化格式
        console_format = "%(asctime)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(
            console_format, datefmt=LogConfig.DEFAULT_CONFIG["date_format"]
        )
        handler.setFormatter(formatter)

        return handler

    @staticmethod
    def _create_formatter() -> logging.Formatter:
        """创建日志格式器"""
        return logging.Formatter(
            LogConfig.DEFAULT_CONFIG["log_format"],
            datefmt=LogConfig.DEFAULT_CONFIG["date_format"],
        )

    @staticmethod
    def get_logger(name: str) -> logging.Logger:
        """
        获取指定名称的日志记录器

        Args:
            name: 日志记录器名称

        Returns:
            logging.Logger: 日志记录器
        """
        return logging.getLogger(name)
