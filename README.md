## **run.sh和run.py的区别**
    参考scrapy启动： https://doc.scrapy.org/en/latest/topics/practices.html
    run.py 通过API方式启动spider, run.py启动多个spider时, 多个spider共享一个进程
    run.sh 通过脚本方式启动spider, run.sh启动多个spider时, 每个spider独享一个进程
    
    需要注意的是, 调试时建议使用run.py, 生产环境部署时建议使用run.sh

## **启动log_spider(一天采集一次)**
    log_spider将今天的ttk_shown日志下载下来,分析日志得出没有采集的京东商品spid, 然后采集这些商品的详情页
    1. 确认run.py/run.sh中配置的是log_spider
    2. 确认setting.py中日志文件名配置的是log_spider
    3. 日志一般推迟一个半小时推到HDFS上, 故要确保在程序执行时间内, 日志会被全部推到HDFS上
    
    需要注意的是, 由于log_spider启动时会清空当前目录下的ttk_logs目录, 故当一个log_spider正在运行时(也即
    ttk_logs不为空, 且正在被使用时), 不能启动第二个log_spider, 否则会清空第一个log_spider正在使用的数据