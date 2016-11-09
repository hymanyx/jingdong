run.sh和run.py的区别:
    参考scrapy启动： https://doc.scrapy.org/en/latest/topics/practices.html.
    run.sh 通过脚本方式启动spider, run.sh启动多个spider时, 每个spider独享一个进程.
    run.py 通过API方式启动spider, run.py启动多个spider时, 多个spider共有一个进程.
    建议调试时使用run.py, 生产环境部署时使用run.sh.

启动log_spider(一天采集一次)：
    log_spider将昨天的ttk_shown日志下载下来,分析日志得出没有采集的京东商品spid, 然后采集这些商品的详情页.
    1. 确认run.py中配置的是log_spider
    2. 确认setting.py中日志文件配置为当前日期
    3. log_spider启动时一般在今天的凌晨2点后, 因为要确保昨天的ttk_shown日志已经被全部传到HDFS上了