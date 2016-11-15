# -*- coding: utf-8 -*-

# Scrapy settings for jingdong project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#     http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
#     http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
import datetime

BOT_NAME = 'jingdong'

SPIDER_MODULES = ['jingdong.spiders']
NEWSPIDER_MODULE = 'jingdong.spiders'

# Logging
LOG_LEVEL = 'INFO'
LOG_FILE = './log/xxx_spider.{0:s}.log'.format(datetime.datetime.now().strftime('%Y%m%d-%H%M%S'))

# Crawl responsibly by identifying yourself (and your website) on the user-agent
# USER_AGENT = 'jingdong (+http://www.yourdomain.com)'

# see https://techblog.willshouse.com/2012/01/03/most-common-user-agents/
# USER_AGENT_LIST = './UserAgents.txt'

# Timeout
# DNS_TIMEOUT = 10
# DOWNLOAD_TIMEOUT = 60

# Obey robots.txt rules
# ROBOTSTXT_OBEY = False

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See http://scrapy.readthedocs.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 0.01
# The download delay setting will honor only one of:
# CONCURRENT_REQUESTS_PER_DOMAIN = 16
# CONCURRENT_REQUESTS_PER_IP = 16

# Disable cookies (enabled by default)
# COOKIES_ENABLED = False
# COOKIES_DEBUG = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/spider-middleware.html
# SPIDER_MIDDLEWARES = {
#    'jingdong.middlewares.MyCustomSpiderMiddleware': 543,
# }

# Retry many times since proxies often fail
# RETRY_TIMES = 3
# Retry on most error codes since proxies fail for different reasons
# RETRY_HTTP_CODES = [500, 503, 504, 400, 403, 404, 408]

# Enable or disable downloader middlewares
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html
# DOWNLOADER_MIDDLEWARES = {
#     # 'jingdong.middlewares.RandomUserAgentMiddleware': 400,
#     # 'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
#     #
#     # 'jingdong.middlewares.RandomHttpProxyMiddleware': 100,
#     # 'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
#     # 'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
# }

# Enable or disable extensions
# Enable or disable extensions
# See http://scrapy.readthedocs.org/en/latest/topics/extensions.html
# EXTENSIONS = {
#    'scrapy.extensions.telnet.TelnetConsole': None,
# }

# Configure item pipelines
# See http://scrapy.readthedocs.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {
#     'jingdong.pipelines.JingdongPipeline': 300
# }

# Setting for JingdongPipeline
# MONGO_URI = '199.155.122.197:27017'
# MONGO_DATABASE = 'jingdong'
# MONGO_COLLECTION = 'product_info_20161008'
# FAILURE_FILE = './failure_20161008.json'

# Enable and configure the AutoThrottle extension (disabled by default)
# See http://doc.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to
# each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See http://scrapy.readthedocs.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'
