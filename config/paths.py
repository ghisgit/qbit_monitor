"""路径配置"""

import os
from pathlib import Path

# 基础目录
BASE_DIR = Path(__file__).resolve().parent.parent

# 监控目录
ADDED_TORRENTS_DIR = BASE_DIR / "scripts/added_torrents"
COMPLETED_TORRENTS_DIR = BASE_DIR / "scripts/completed_torrents"

# 日志文件
LOG_FILE = BASE_DIR / "logs/qbit_monitor.log"

# 确保目录存在
ADDED_TORRENTS_DIR.mkdir(exist_ok=True)
COMPLETED_TORRENTS_DIR.mkdir(exist_ok=True)
