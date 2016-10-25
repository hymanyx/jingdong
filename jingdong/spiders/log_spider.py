# -*- coding: utf-8 -*-

import scrapy
import subprocess
import datetime
import glob
import json
from jingdong.items import JdProductItem
from itertools import islice


def split_dict(my_dict, max_size_of_sub_dict):
    """将一个python字典分割成多个小字典, 且每个小字典的元素数量不多于max_size_of_sub_dict
    :param my_dict: 待分割的python字典
    :param max_size_of_sub_dict：小字典中的最大元素数
    """
    it = iter(my_dict)
    for i in xrange(0, len(my_dict), max_size_of_sub_dict):
        yield {key: my_dict[key] for key in islice(it, max_size_of_sub_dict)}


def get_spids():
    """获取前一天ttk_shown日志中所有未采集的京东商品spid
    """
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    spids = set()
    for i in xrange(24):
        # 下载HDFS上的ttk_shown日志到本地
        log_file = '/logs/flume-logs/ttk/ttk_shown/' + yesterday + '/' + yesterday + '-{0:02d}'.format(
            i) + '/ttk_shown.log.*.log'
        subprocess.check_call(['hdfs', 'dfs', '-get', log_file, '/tmp/ttk_show'])

        # 获取下载到本地的ttk_shown的文件路径
        files = glob.glob('/tmp/ttk_show/ttk_shown.log.*.log')

        # 在日志中获取未采集的京东商品spid
        print "current ttk_shown log: ", log_file
        with open(files[0], 'r') as fin:
            for line in fin:
                log = {}
                try:
                    log = json.loads(line.split('\t')[1])
                    if (log['website'] == 'jd.com') and (log['stored'] == 0) and (log['spid'].isdigit()):
                        if log['spid'] not in spids:
                            spids.add(log['spid'])
                            yield log['spid']
                except Exception as e:
                    print e, log_file, line

        # 删除下载到本地的ttk_shown日志
        subprocess.check_call(['/bin/rm', '-rf', files[0]])


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
        'RETRY_HTTP_CODES': [500, 503, 504, 400, 403, 404, 408, 111],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
            'jingdong.middlewares.RandomUserAgentMiddleware': 400,
            'scrapy.downloadermiddlewares.useragent.UserAgentMiddleware': None,
            'jingdong.middlewares.RandomHttpProxyMiddleware': 100,
            'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
        },
        'ITEM_PIPELINES': {
            'jingdong.pipelines.JdLogPipeline': 300,
        },
        'MONGO_URI': '199.155.122.197:27017',
        'MONGO_DATABASE': 'jingdong',
        'MONGO_COLLECTION': 'log_product_info_' + (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d"),
    }

    def start_requests(self):
        """重载父类的start_request方法"""
        num = 0
        for spid in get_spids():
            num += 1
            product_item = JdProductItem()
            product_item['spid'] = spid
            url = 'https://item.jd.com/{0:s}.html'.format(spid)
            meta = {'is_proxy': True, 'product_item': product_item}
            yield scrapy.Request(url=url, meta=meta, callback=self.parse_detail_page)
        self.logger.info('get %s spids from ttk_shown logs' % num)

    def parse_detail_page(self, response):
        """解析详情页
        :param response: parse函数中url所表示的页面
        """
        # 从页面中解析出一个商品的部分信息
        product_item = response.meta['product_item']
        try:
            # cid
            cid1 = response.xpath('/html/body/div[4]/div/div[1]/div[5]/a/@href').extract()
            cid2 = response.xpath('//*[@id="root-nav"]/div/div/span[1]/a[2]/@href').extract()
            product_item['cid'] = (cid1[0].split('=')[-1] if cid1 else cid2[0].split('=')[-1])

            # nick
            nick1 = response.xpath('/html/body/div[4]/div/div[2]/div[1]/div/em/text()').extract()
            nick2 = response.xpath('//*[@id="popbox"]/div/div[1]/h3/a/@title').extract()
            nick3 = response.xpath('//*[@id="extInfo"]/div[2]/em/text()').extract()
            nick = (nick1 if nick1 else nick2)
            nick = (nick if nick else nick3)
            product_item['nick'] = nick[0]

            # title
            title1 = response.xpath('/html/body/div[5]/div/div[2]/div[1]/text()').extract()
            title2 = response.xpath('//*[@id="name"]/h1/text()').extract()
            product_item['title'] = (title1[0] if title1 else title2[0])

            # imageUrl
            imageUrl1 = response.xpath('//*[@id="spec-list"]/div/ul/li[1]/img/@data-url').extract()
            imageUrl2 = response.xpath('//*[@id="spec-list"]/ul/li[1]/img/@data-url').extract()
            imageUrl = (imageUrl1[0] if imageUrl1 else imageUrl2[0])
            product_item['imageUrl'] = 'http://img13.360buyimg.com/n1/' + imageUrl

            # url, website
            product_item['url'] = response.url
            product_item['website'] = 'jd.com'

            price_url = 'http://p.3.cn/prices/mgets?type=1&skuIds=' + 'J_' + product_item['spid']
            meta = {'is_proxy': True, 'product_item': product_item}
            yield scrapy.Request(url=price_url, meta=meta, callback=self.parse_price_and_comment)
        except Exception, e:
            self.logger.error("parse product %s's detail page error: %s, %s, %s" % (product_item['spid'], e, response.url, product_item))

    def parse_price_and_comment(self, response):
        """解析价格, 并构造获取评论的请求
        :param response: parse函数中的price_url的响应
        """
        # 解析价格
        product_item = response.meta['product_item']
        try:
            prices = json.loads(response.body)
            if len(prices) != 1:
                self.logger.error("get product %s's prices error, we should get %d, but actually get %d" % (product_item['spid'], 1, len(prices)))
            else:
                promotion_price = int(float(prices[0]['p']) * 100)  # 促销价
                original_price = int(float(prices[0]['m']) * 100)  # 原价
                # 价格过滤
                if (0 >= promotion_price) or (0 >= original_price):
                    self.logger.error("product %s's price error: promotion price (%s), original price(%s)" % (
                        product_item['spid'], promotion_price, original_price))
                else:
                    product_item['promoPrice'] = promotion_price
                    product_item['price'] = original_price

                    comment_url = 'http://club.jd.com/comment/productCommentSummaries.action?my=pinglun&referenceIds=' + \
                                  product_item['spid']
                    meta = {'is_proxy': True, 'product_item': product_item}
                    yield scrapy.Request(url=comment_url, meta=meta, callback=self.parse_comment)
        except Exception, e:
            self.logger.error("can not get %s's price, %s, %s, %s" % (product_item['spid'], e, response.url, response.body))

    def parse_comment(self, response):
        """解析评论
        :param response: parse_price_and_comment函数中comment_url的响应
        """
        product_item = response.meta['product_item']
        try:
            comments = json.loads(response.body.decode('gbk'))
            comments = comments['CommentsCount']
            if len(comments) != 1:
                self.logger.error("get product %s's comments error, we should get %d, but actually get %d" % (product_item['spid'], 1, len(comments)))
            else:
                product_item['volume'] = comments[0]['CommentCount']
                product_item['feedbackCount'] = comments[0]['CommentCount']
                product_item['rate'] = {
                    'good': str(comments[0]['GoodRateShow']) + '%',
                    'general': str(comments[0]['GeneralRateShow']) + '%',
                    'poor': str(comments[0]['PoorRateShow']) + '%'
                }
                yield product_item
        except Exception, e:
            self.logger.error("can not get %s's comment, %s, %s, %s" % (product_item['spid'], e, response.url, response.body))
