import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path


def setup_logging(log_file: Path, debug_mode: bool = False):
    """设置日志配置"""
    log_level = logging.DEBUG if debug_mode else logging.INFO

    logger = logging.getLogger()
    logger.setLevel(log_level)

    # 清除已有的处理器
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # 创建格式化器
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # 文件处理器 - 使用轮转日志
    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"  # 10MB
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)

    # 添加处理器到日志器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # 记录初始日志
    logger.info(f"日志系统初始化完成，日志级别: {logging.getLevelName(log_level)}")
    logger.info(f"日志文件: {log_file}")
