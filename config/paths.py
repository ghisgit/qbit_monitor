import os
from pathlib import Path

# 基础目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 监控目录
ADDED_TORRENTS_DIR = BASE_DIR / "scripts/added_torrents"
COMPLETED_TORRENTS_DIR = BASE_DIR / "scripts/completed_torrents"

# 日志目录和文件
LOG_DIR = BASE_DIR / "logs"
LOG_FILE = LOG_DIR / "qbit_monitor.log"


# 数据目录
DATA_DIR = BASE_DIR / "data"
DATA_FILE = DATA_DIR / "tasks.db"


def ensure_directories():
    """确保所有必要的目录都存在"""
    directories = [
        ADDED_TORRENTS_DIR,
        COMPLETED_TORRENTS_DIR,
        LOG_DIR,
        DATA_DIR,
    ]

    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        print(f"确保目录存在: {directory}")


# 在模块导入时自动创建目录
ensure_directories()
