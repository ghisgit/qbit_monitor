import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from config.paths import LOG_DIR, LOG_FILE


def setup_logging(log_file: Path = None, debug_mode: bool = False):
    """设置日志配置"""

    # 如果没有指定日志文件，使用默认路径
    if log_file is None:
        log_file = LOG_FILE

    # 确保日志目录存在
    log_file.parent.mkdir(parents=True, exist_ok=True)

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

    try:
        # 文件处理器 - 使用轮转日志
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"  # 10MB
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    except Exception as e:
        print(f"无法创建文件日志处理器: {e}")
        # 如果文件日志失败，只使用控制台日志

    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # 记录初始日志
    logger.info(f"日志系统初始化完成，日志级别: {logging.getLevelName(log_level)}")
    logger.info(f"日志文件: {log_file}")
