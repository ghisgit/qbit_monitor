#!/bin/bash
# qBittorrent 添加种子时调用
# 参数: %I - 种子哈希

TORRENT_HASH="$1"
ADDED_DIR="/scripts/added_torrents"

echo "$TORRENT_HASH" > "$ADDED_DIR/${TORRENT_HASH}.hash"