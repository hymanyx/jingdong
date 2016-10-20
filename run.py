# -*- coding: utf-8 -*-

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

process = CrawlerProcess(get_project_settings())
# process.crawl('product_spider')
# process.crawl('keyword_spider')
process.crawl('cps_spider')
process.start()
