# -*- coding: utf-8 -*-
"""Scrapy Middleware to set a random User-Agent for every Request.

Downloader Middleware which uses a file containing a list of
user-agents and sets a random one for each request.
"""

import random
from scrapy import signals
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from jingdong.dynamicip import DynamicIP


class RandomUserAgentMiddleware(UserAgentMiddleware):
    def __init__(self, settings, user_agent='Scrapy'):
        super(RandomUserAgentMiddleware, self).__init__()
        self.user_agent = user_agent
        user_agent_list_file = settings.get('USER_AGENT_LIST')
        if not user_agent_list_file:
            # If USER_AGENT_LIST_FILE settings is not set,
            # Use the default USER_AGENT or whatever was
            # passed to the middleware.
            ua = settings.get('USER_AGENT', user_agent)
            self.user_agent_list = [ua]
        else:
            with open(user_agent_list_file, 'r') as f:
                self.user_agent_list = [line.strip() for line in f.readlines()]

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler.settings)
        crawler.signals.connect(obj.spider_opened, signal=signals.spider_opened)
        return obj

    def process_request(self, request, spider):
        user_agent = random.choice(self.user_agent_list)
        if user_agent:
            request.headers.setdefault('User-Agent', user_agent)


# see https://github.com/aivarsk/scrapy-proxies
class RandomHttpProxyMiddleware(object):
    def __init__(self):
        self.dynamic_ip = DynamicIP('199.155.122.131:2181', '/adsl_proxy/lock')
        self.dynamic_ip.run()

    def process_request(self, request, spider):
        meta = request.meta
        if ('is_proxy' in meta) and (meta['is_proxy']):
            proxy = self.dynamic_ip.get_proxy()
            if proxy:
                request.meta['proxy'] = "http://%s:3128" % proxy
        else:
            pass
