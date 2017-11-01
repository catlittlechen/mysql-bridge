#!/bin/bash

log_path='/data/mysql-bridge/binlog'
cd $log_path;

# gzip 保留1天
find . -type f -mtime +1 ! -iname '*.gz' -iname "binlog-*" -exec gzip -f -v {} \;

# clean 保留3天
find . -type f -mtime +3 -iname '*.gz' -iname "binlog-*" -exec rm -f {} \;
