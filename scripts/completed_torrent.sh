#!/bin/bash
# qBittorrent 完成种子时调用
# 参数: %I - 种子哈希

TORRENT_HASH="$1"
COMPLETED_DIR="/scripts/completed_torrents"

echo "$TORRENT_HASH" > "$COMPLETED_DIR/${TORRENT_HASH}.hash"