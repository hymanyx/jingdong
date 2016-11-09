# -*- coding: utf-8 -*-
"""
spider的启动: https://doc.scrapy.org/en/latest/topics/practices.html

程序中使用的是在一个进程中启动多个spider的方式

TODO - 在多个进程中运行多个spider的方式也许会加快爬取速度
"""
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())
# process.crawl('category_spider')
# process.crawl('cps_spider')
process.crawl('log_spider')
process.start()
