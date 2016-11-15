#!/bin/sh

# crontab任务启动时并不知道当前用户的环境变量. crontab执行脚本run.sh启动log_spider时, 会使用hdfs命令下载ttk_shown日志到本地, 但
# hdfs命令的执行需要知道JAVA环境变量. 一般可以通过source ~/.bash_profile来使用当前用户的环境变量, 但.bash_profile中并没有配置
# JAVA环境变量, 故需要在run.sh脚本中手动配置

# JAVA环境变量
export JAVA_HOME=/usr/local/jdk
export JRE_Home=/usr/local/jdk/jre
export CLASSPATH=:/usr/local/jdk/lib:/usr/local/jdk/jre/lib

# Python
export PATH="/home/murphy/anaconda2-4.0.0/bin:$PATH"

# 启动log_spider
nohup scrapy crawl log_spider 1>./log/nohup.$(date +%Y%m%d-%H%M%S).log 2>&1 &