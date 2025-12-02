#!/bin/bash
# qBittorrent 完成种子时调用
# 参数: %I - 种子哈希

TORRENT_HASH="$1"
TAG="completed"

curl -d "hashes=$TORRENT_HASH&tags=$TAG" http://127.0.0.1:8080/api/v2/torrents/addTags
