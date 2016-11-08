启动log_spider(一天采集一次)：
    log_spider将昨天的ttk_shown日志下载下来,分析日志得出没有采集的京东商品spid, 然后采集这些商品的详情页. 因为一个log_spider分析一天的日志需要8个小时,故用了4个log_spider来分析一天的日志.

    log_spider_1分析0, 4, 8, 12, 16, 20点共6个小时的日志, 下载下来的日志存储在/tmp/ttk_shown/ttk_shown_1目录底下, 分析完日志后会删除掉这些日志;
    log_spider_2分析1, 5, 9, 13, 17, 21点共6个小时的日志, 下载下来的日志存储在/tmp/ttk_shown/ttk_shown_2目录底下, 分析完日志后会删除掉这些日志;
    log_spider_3分析2, 6, 10, 14, 18, 22点共6个小时的日志, 下载下来的日志存储在/tmp/ttk_shown/ttk_shown_3目录底下, 分析完日志后会删除掉这些日志;
    log_spider_4分析3, 7, 11, 15, 19, 23点共6个小时的日志, 下载下来的日志存储在/tmp/ttk_shown/ttk_shown_4目录底下, 分析完日志后会删除掉这些日志;

    1. 确认run.py中配置的是log_spider_1, log_spider_2, log_spider_3, log_spider_4
    2. 确认setting.py中日志文件配置为当前日期
    3. log_spider启动时一般在今天的凌晨2点后, 因为要确保昨天的ttk_shown日志已经被全部传到HDFS上了
    4. 使用crontab部署log_spider: nohup python run.py &