# -*- coding: utf-8 -*-

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())
# process.crawl('category_spider')
# process.crawl('cps_spider')
# process.crawl('log_spider_1')
# process.crawl('log_spider_2')
# process.crawl('log_spider_3')
# process.crawl('log_spider_4')
process.start()
