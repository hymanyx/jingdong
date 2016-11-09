# -*- coding: utf-8 -*-
"""
该程序通过分析今天的ttk_shown日志，从中获取到未采集的京东商品spid, 通过爬详情页的方式采集商品.

TODO:
    1. 由于我们一个一个的采详情页, 收集product_item到batch_product_items, 收集到50个时才批量获取价格。这样就会产生一个问题:
       当爬虫结束时, 如果batch_product_items没有收集到50个, 那么batch_product_items中的商品就会被丢弃。
"""

import scrapy
import subprocess
import datetime
import glob
import json
from pybloom import ScalableBloomFilter
from jingdong.items import JingdongProductItem
from jingdong.spiders.util import get_categories

# 待下载日志的时间
yestoday = datetime.datetime.now() - datetime.timedelta(days=1)
log_date = yestoday.strftime("%Y-%m-%d")
mongo_date = yestoday.strftime("%Y%m%d")


def get_spids():
    """获取前一天ttk_shown日志中所有未采集的京东商品spid
    """
    # ttk_show日志本地存储路径
    path = '/tmp/ttk_shown'

    # 删除上次意外终止时残留的ttk_shown日志
    local_logs = glob.glob('{0:s}/ttk_shown.log.*.log'.format(path))
    for local_log in local_logs:
        child = subprocess.Popen(['/bin/rm', '-rf', local_log])
        child.wait()

    # 获取昨天24个小时的ttk_shown日志
    for hour in xrange(0, 24, 1):
        # 下载HDFS上的ttk_shown日志到本地
        hdfs_log = '/logs/flume-logs/ttk/ttk_shown/{0:s}/{0:s}-{1:02d}/ttk_shown.log.*.log'.format(log_date, hour)
        child = subprocess.Popen(['hdfs', 'dfs', '-get', hdfs_log, path])
        child.wait()

        # 解析本地的ttk_show日志获取未采集的京东商品spid (NOTE: spid未去重)
        local_logs = glob.glob('{0:s}/ttk_shown.log.*.log'.format(path))
        for local_log in local_logs:
            # 解析本地ttk_shown日志
            print "current ttk_shown log: [{0:s}-{1:02d}, {2:s}]".format(log_date, hour, local_log)
            with open(local_log, 'r') as fin:
                for i, line in enumerate(fin):
                    try:
                        log = json.loads(line.split('\t')[1])
                        if (log['website'] == 'jd.com') and (log['stored'] == 0) and (log['spid'].isdigit()):
                            yield log['spid']
                    except Exception as e:
                        print "parse {0:d}th line error in ttk_shown log [{1:s}-{2:02d}, {3:s}], {4:s}"\
                            .format(i + 1, log_date, hour, local_log, line)
            # 删除本地ttk_shown日志
            child = subprocess.Popen(['/bin/rm', '-rf', local_log])
            child.wait()


class LogSpider(scrapy.Spider):
    name = "log_spider"
    start_urls = []

    custom_settings = {
        # 'DOWNLOAD_DELAY': 0.001,
        'ROBOTSTXT_OBEY': False,
        'DNS_TIMEOUT': 10,
        'DOWNLOAD_TIMEOUT': 60,
        'CONCURRENT_REQUESTS': 32,
        'COOKIES_ENABLED': False,
        'COOKIES_DEBUG': False,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 503, 504, 400, 403, 404, 408],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'jingdong.middlewares.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'jingdong.middlewares.RandomHttpProxyMiddleware': 100,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
        },
        'ITEM_PIPELINES': {
            'jingdong.pipelines.JingdongPipeline': 300,
        },
        'MONGO_URI': '199.155.122.32:27018',
        'MONGO_DATABASE': 'jingdong',
        'MONGO_COLLECTION': 'log_product_info_' + mongo_date,
    }

    filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
    categories = get_categories()
    batch_product_items = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(LogSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.logger.info('Spider closed: %s, left %d products' % (spider.name, len(self.batch_product_items)))

    def start_requests(self):
        """重载父类的start_request方法"""
        for spid in get_spids():
            if self.filter.add(spid):
                pass
            else:
                url = 'https://item.jd.com/{0:s}.html'.format(spid)
                meta = {'is_proxy': True, 'spid': spid, 'nick': ''}
                yield scrapy.Request(url=url, meta=meta, callback=self.parse_detail_page)

    def parse_detail_page(self, response):
        """解析详情页
        :param response: parse函数中url所表示的页面
        """
        # 从页面中解析出一个商品的部分信息
        product_item = JingdongProductItem()
        spid = response.meta['spid']
        nick = response.meta['nick']

        # 网页被重定向
        if spid not in response.url:
            self.logger.error("parse product %s's detail page error, redirect to %s" % (spid, response.url))
        # 网页未重定向
        else:
            # cid
            begin = response.body.find('cat: [') + len('cat: [')
            end = response.body[begin:].find('],') + begin
            cid = response.body[begin: end]

            # 类目过滤
            if cid not in self.categories.keys():
                self.logger.error("product %s's category %s is not in mysql" % (spid, cid))
            else:
                try:
                    product_item['cid'] = cid
                    product_item['spid'] = spid
                    product_item['url'] = response.url
                    product_item['website'] = 'jd.com'
                    product_item['isCPS'] = False

                    # title
                    begin = response.body.find('name: \'') + len('name: \'')
                    end = response.body[begin:].find('\',') + begin
                    product_item['title'] = response.body[begin: end].decode("unicode_escape")

                    # imageUrl
                    begin = response.body.find('src: \'') + len('src: \'')
                    end = response.body[begin:].find('\',') + begin
                    product_item['imageUrl'] = 'http://img13.360buyimg.com/n1/' + response.body[begin: end]

                    # nick
                    nick1 = response.xpath('//*[@class="crumb-wrap"]/div/div[@class="contact fr clearfix"]/div[1]/div/a/text()').extract()   # 右上
                    nick2 = response.xpath('//*[@class="crumb-wrap"]/div/div[@class="contact fr clearfix"]/div[1]/div/em/text()').extract()  # 右上
                    nick3 = response.xpath('//*[@id="popbox"]/div/div[1]/h3/a/@title').extract()     # 左边
                    nick4 = response.xpath('//*[@id="extInfo"]/div[@class="seller-infor"]/em/text()').extract() # 右边
                    nick = (nick1 if nick1 else nick)
                    nick = (nick2 if nick2 else nick)
                    nick = (nick3 if nick3 else nick)
                    nick = (nick4 if nick4 else nick)
                    product_item['nick'] = nick[0]

                    if len(self.batch_product_items) < 50:
                        self.batch_product_items.append(product_item)
                    else:
                        spids = [item['spid'] for item in self.batch_product_items]
                        price_url = 'http://p.3.cn/prices/mgets?type=1&skuIds=J_' + ',J_'.join(spids)
                        meta = {'is_proxy': True, 'product_items': self.batch_product_items, 'retry': 0}
                        self.batch_product_items = []
                        yield scrapy.Request(url=price_url, meta=meta, callback=self.parse_price_and_comment)
                except Exception, e:
                    self.logger.error("parse product %s's detail page error: %s, %s" % (spid, e, response.url))

    def parse_price_and_comment(self, response):
        """解析价格, 并构造获取评论的请求
        :param response: parse函数中的price_url的响应
        """
        meta = response.meta
        product_items = meta['product_items']

        # 解析价格
        prices = json.loads(response.body)
        if len(prices) != len(product_items):
            self.logger.error(
                "retry: %s. get product prices error, we should get %d, but actually get %d - %s" % (meta['retry'], len(product_items), len(prices), response.url))
            meta['retry'] += 1
            yield scrapy.Request(url=response.url, meta=meta, callback=self.parse_price_and_comment, dont_filter=True)
        else:
            # 所有下架商品在product_items中的位置下表
            indexs = []

            # 通过价格过滤出下架商品
            for index, price in enumerate(prices):
                spid = price['id'].split('_')[1]  # 京东商品id
                promotion_price = int(float(price['p']) * 100)  # 促销价
                original_price = int(float(price['m']) * 100)  # 原价
                if (0 >= promotion_price) or (0 >= original_price):
                    self.logger.error("product %s's price error: promotion price (%s), original price(%s), %s" % (
                        spid, promotion_price, original_price, product_items[index]['url']))
                    indexs.append(index)
                else:
                    product_items[index]['promoPrice'] = promotion_price
                    product_items[index]['price'] = original_price

            # 从后往前删除下架商品
            for index in indexs[::-1]:
                product_items.pop(index)

            # 构造商品评论请求
            spids = [product_item['spid'] for product_item in product_items]
            comment_url = 'http://club.jd.com/comment/productCommentSummaries.action?my=pinglun&referenceIds=' + ','.join(spids)
            meta['product_items'] = product_items
            meta['retry'] = 0
            yield scrapy.Request(url=comment_url, meta=meta, callback=self.parse_comment)

    def parse_comment(self, response):
        """解析评论
        :param response: parse_price_and_comment函数中comment_url的响应
        """
        meta = response.meta
        product_items = meta['product_items']

        # 解析评论
        comments = json.loads(response.body.decode('gbk'))
        comments = comments['CommentsCount']
        if len(comments) != len(product_items):
            self.logger.error(
                "retry: %s. get product comments error, we should get %d, but actually get %d - %s" % (meta['retry'], len(product_items), len(comments), response.url))
            meta['retry'] += 1
            yield scrapy.Request(url=response.url, meta=meta, callback=self.parse_comment, dont_filter=True)
        else:
            for index, comment in enumerate(comments):
                product_items[index]['volume'] = comment['CommentCount']         # 商品评论数
                product_items[index]['feedbackCount'] = comment['CommentCount']  # 商品评论数
                product_items[index]['rate'] = {
                    'good': str(comment['GoodRateShow']) + '%',
                    'general': str(comment['GeneralRateShow']) + '%',
                    'poor': str(comment['PoorRateShow']) + '%'
                }

            # yield所有product_item
            for product_item in product_items:
                yield product_item
