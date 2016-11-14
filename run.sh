#!/bin/sh

# log_spider在10.0.0.210上部署时的启动命令
nohup /home/murphy/anaconda2-4.0.0/bin/scrapy crawl log_spider 2>&1 1>./log/nohup.$(date +%Y%m%d).log &