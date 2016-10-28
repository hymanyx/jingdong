# -*- coding: utf-8 -*-
"""
该程序通过爬取京东联盟中的CPS商品，从中获取到京东CPS商品信息, 通过爬详情页的方式采集商品.

TODO:
    1. 由于我们一个一个的采详情页, 收集product_item到batch_product_items, 收集到50个时才批量获取价格。这样就会产生一个问题:
       当爬虫结束时, 如果batch_product_items没有收集到50个, 那么batch_product_items中的商品就会被丢弃。
"""

import json
import scrapy
import mysql.connector
import datetime
from pybloom import ScalableBloomFilter
from jingdong.items import JingdongProductItem


def get_categories():
    """从MySql中获取所有的京东三级类目"""
    config = {'user': 'predictwr', 'password': 'Wrk7predict32K8qpWR', 'host': '192.168.3.57',
              'database': 'tts_category_predict'}
    query = "SELECT category_code, category_name FROM back_category_other WHERE website='jd.com' AND collect_flag=1"
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(query)

    categories = {}
    for category_code, category_name in cursor:
        category_code = category_code.replace('-', ',')
        category_name = category_name.replace('-', ',')
        categories[category_code] = category_name

    conn.close()
    return categories


def make_url(pageIndex=1, pageSize=50, property='pcPrice', sort='desc', adownerType='',pcRate='',
             wlRate='', category='', category1='0', condition=1, fromPrice='', toPrice=''):
    """构造url用来获取一页CPS商品
    :param pageIndex: 页面序号. 默认为1
    :param pageSize: 返回页面中CPS商品的数量. 默认为50
    :param property: 返回页面中CPS商品的排序方式.
    :param sort: 返回页面中CPS商品按property升序或降序排列.
    :param adownerType: 商品卖家类型, 自营为g, 商家为p，
    :param pcRate: PC佣金比例, 仅当property参数为pcCommissionShare时, 返回页面中CPS商品的PC佣金比例才大于该值
    :param wlRate: 无线佣金比例, 仅当property参数为wlCommissionShare时, 返回页面中CPS商品的无线佣金比例才大于该值
    :param category1: CPS商品一级类目，默认为0(即所有类目)
    :param category: CPS商品二级类目
    :param condition: 不知道作用, 但一般情况下为1
    :param fromPrice: 返回页面中CPS商品的最低价格
    :param toPrice: 返回页面中CPS商品的最高价格
    """
    url = 'https://media.jd.com/gotoadv/goods?pageIndex={0:d}&pageSize={1:d}&property={2:s}' \
          '&sort={3:s}&adownerType={4:s}&pcRate={5:s}&wlRate={6:s}&category={7:s}&category1=' \
          '{8:s}&condition={9:d}&fromPrice={10:s}&toPrice={11:s}&keyword='\
        .format(pageIndex, pageSize, property, sort, adownerType, pcRate,
                wlRate, category, category1, condition, fromPrice, toPrice)
    return url


class CPSSpider(scrapy.Spider):
    name = "cps_spider"
    start_urls = ["https://media.jd.com/gotoadv/goods"]

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
        'DNS_TIMEOUT': 10,
        'DOWNLOAD_TIMEOUT': 60,
        'CONCURRENT_REQUESTS': 64,
        'COOKIES_ENABLED': False,
        'COOKIES_DEBUG': False,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 503, 504, 400, 403, 404, 408],
        'DOWNLOADER_MIDDLEWARES': {
            'scrapy.downloadermiddlewares.retry.RetryMiddleware': 90,
        },
        'ITEM_PIPELINES': {
            'jingdong.pipelines.JingdongPipeline': 300
        },

        'MONGO_URI': '199.155.122.32:27018',
        'MONGO_DATABASE': 'jingdong',
        'MONGO_COLLECTION': 'cps_product_info_' + datetime.datetime.now().strftime("%Y%m%d"),
    }

    headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate, sdch, br',
        'Accept-Language': 'zh-CN,zh;q=0.8',
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': 'ssid="xU8w2LBHTIyRGJXe9z6diQ=="; __jdv=95931165|direct|-|none|-|1476087645644; TrackID=14RhAtAnpomMFDu4jyc6-FEo8U1DVJ70aTR4f1cPl7tSe01Lmb0j32_cRKRhM8xtUtyBel4gZOtaTwCRxZYTlZmk6fJ0cLRMqeBMEYrbuLGU; pinId=9xKiLqfMaUWrY0sm6eD6abV9-x-f3wj7; pin=jd_6b7bc9f99418a; unick=jd_taotao189; _tp=dcFIOq%2BdGyg%2Bms7lwxsyREiqs1RZcwq61dzP4877n%2Bg%3D; _pst=jd_6b7bc9f99418a; ceshi3.com=DJ4j4kKyjeJ2oTmQsmrBHqjiyDL4FpVk0eUwjaw17eocPjl-cUmRGbt8wIgVIGAQ; ipLocation=%u5317%u4EAC; areaId=1; ipLoc-djd=1-72-2799-0; thor=5EBDEF795C7E5A1890D6A2D45C708A405DDF296FDCE33EB8672BDB32DA55F685A0BC78388580BACB68EC83C50F8472E43992D887EBE110A98526DC9D492F901ABB8DE3770097374AB263C1CEEEF48C387CE35568D0AD92E0717685F0ECCA9A6A1279858CB71423B21540FF8555125DE7C53CDF76B5844964B01B814888F06E9938A7C4BEFA61294A56BD63E97D8481550DDCB1285AE59DE4BE0883561549A056; __jda=108460702.1844883239.1476087646.1476087646.1476087646.1; __jdb=108460702.13.1844883239|1.1476087646; __jdc=108460702; __jdu=1844883239; Hm_lvt_96d9d92b8a4aac83bc206b6c9fb2844a=1476079615,1476087746; Hm_lpvt_96d9d92b8a4aac83bc206b6c9fb2844a=1476092550',
        'Host': 'media.jd.com',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
    }

    filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
    categories = get_categories()
    batch_product_items = []

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(CPSSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=scrapy.signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.logger.info('Spider closed: %s, left %d products' % (spider.name, len(self.batch_product_items)))

    def start_requests(self):
        """重载父类的start_request方法, 目的是为请求加入header和cookies"""
        for url in self.start_urls:
            yield scrapy.Request(url=url, headers=self.headers, callback=self.parse)

    def parse(self, response):
        """解析start_urls中链接所表示的页面, 从中提取出所有一级类目, 并构造url获取该一级类目的list页
        :param response: start_urls中链接所表示的页面
        """
        # 从页面中解析出所有一级类目
        categories = response.xpath('//*[@id="mainCatList"]/option/@value').extract()

        # 去掉部分无用类目
        categories.remove('0')      # 全部类目
        categories.remove('1713')   # 图书
        categories.remove('4938')   # 本地生活/旅游出行
        categories.remove('6322')   # 养生保健
        categories.remove('6323')   # 家用器械

        # 构造按价格从高到低排序的url请求, (从返回页面中)解析出每个类目中所有商品的最高价
        for category in categories:
            # if category != "1320":
            #     continue
            meta = {
                'pageIndex': 1,
                'category1': category
            }
            url = make_url(**meta)
            yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_secondary_category)
            # break

    def parse_secondary_category(self, response):
        """解析parse函数中url链接所表示的页面, 从中提取出一级类目中所有商品的最高价, 并为该一级类目构造带有价格区间的多个请求
        :param response: parse函数中url链接所表示的页面
        """
        # 从response中获得meta
        meta = response.meta.copy()
        meta.pop('download_timeout')
        meta.pop('download_latency')
        meta.pop('download_slot')
        meta.pop('depth')

        # 采集该一级类目下商品的经营类型, 及二级类目
        ad_owner_types = response.xpath('//*[@id="a_block"]/div[1]/div/label[*]/a/@data-value').extract()[1:]
        categories = response.xpath('//*[@id="two_categories"]/li[*]/label/a/@data-value').extract()

        # 构造带有经营类型和二级类目的请求
        for category in categories:
            for ad_owner_type in ad_owner_types:
                # ad_owner_type = u'p'
                meta['category'] = category
                meta['adownerType'] = ad_owner_type
                meta['property'] = 'pcPrice'
                meta['sort'] = 'desc'
                url = make_url(**meta)
                yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_highest_price)
            #     break
            # break

    def parse_highest_price(self, response):
        """解析parse_secondary_category函数中url链接所表示的页面, 从中提取出二级类目中所有商品的最高价, 并为该二级类目构造带有价格区间的多个请求
        :param response: parse函数中url链接所表示的页面
        """
        # 从response中获得meta
        meta = response.meta.copy()
        meta.pop('download_timeout')
        meta.pop('download_latency')
        meta.pop('download_slot')
        meta.pop('depth')

        # 过滤掉没有商品的页面, 并解析出商品页面的最高价
        data = response.xpath('//*[@id="container"]/div[2]/div[2]/div[4]/table/tbody/tr[1]/td[1]/span/text()').extract()
        if data:
            # data不为空的时候, data[0]的值为"没有查询到符合条件的数据"
            pass
        else:
            # 解析出一个二级类目中所有商品的最高价
            pc_price = response.xpath('//*[@id="container"]/div[2]/div[2]/div[4]/table/tbody/tr[1]/td[2]/text()').extract()[0]
            pc_price = pc_price.replace(u'\r\n', '').replace(u' ', '').replace(u'￥', '').replace(u'PC：', '').replace(',', '')

            # 当前价格区间
            from_price = 0
            to_price = int(float(pc_price) + 1)    # 加1是因为float转int会损失精度

            # 为该二级类目构造带有价格区间的多个请求
            prices = [from_price, to_price]
            for i in range(len(prices) - 1):
                meta['fromPrice'] = str(prices[i] + 0.01)
                meta['toPrice'] = str(prices[i + 1] + 0.0)
                url = make_url(**meta)
                yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_list_page)

    def parse_list_page(self, response):
        """解析parse函数中一个url所表示的页面, 并从selector中解析出商品信息
        :param response: parse函数中一个请求的响应.
        """
        # 从response中获得meta
        meta = response.meta.copy()
        meta.pop('download_timeout')
        meta.pop('download_latency')
        meta.pop('download_slot')
        meta.pop('depth')

        # 过滤掉没有商品的页面
        data = response.xpath('//*[@id="container"]/div[2]/div[2]/div[4]/table/tbody/tr[1]/td[1]/span/text()').extract()
        if data:
            # data不为空的时候, data[0]的值为"没有查询到符合条件的数据"
            pass
        else:
            # 从页面中获取页面总数page_num
            page_num = response.xpath('//*[@id="container"]/div[2]/div[2]/div[5]/ul[1]/li[1]/text()').extract()[0][1:-1]
            page_num = int(page_num)

            # 从response.meta中获取当前页面的价格区间
            from_price = int(meta['fromPrice'][:-3])
            to_price = int(meta['toPrice'][:-2])
            price_interval = to_price - from_price

            # 页面总数大于100, 但价格区间不是最小的情况: 以更小的价格区间获取页面
            if (page_num > 100) and (price_interval > 1):
                prices = [i for i in range(from_price, to_price, price_interval / 2)]
                prices.append(to_price)
                for i in range(len(prices) - 1):
                    meta['pageIndex'] = 1
                    meta['fromPrice'] = str(prices[i] + 0.01)
                    meta['toPrice'] = str(prices[i + 1] + 0.0)
                    url = make_url(**meta)
                    yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_list_page)

            # 页面总数大于100, 价格区间已经最小, pc佣金比例不存在的情况: 以不同的pc佣金比例获取页面
            if (page_num > 100) and (price_interval == 1) and ('pcRate' not in response.meta):
                pc_rate_interval = 1                     # PC佣金比率间隔
                pc_rates = [i for i in range(1, 100, pc_rate_interval)]
                for pc_rate in pc_rates:
                    meta['pageIndex'] = 1
                    meta['property'] = 'pcCommissionShare'
                    meta['sort'] = 'asc'
                    meta['pcRate'] = str(pc_rate)
                    url = make_url(**meta)
                    yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_list_page)

            # 页面总数大于100, 价格区间已经最小, pc佣金比例也已经存在的情况： 只能获取前100页的商品了, 大于100页的商品实在无能为力了
            if (page_num > 100) and (price_interval == 1) and ('pcRate' in response.meta):
                self.logger.error("prase first 100 pages: page_num(%d), price_inverval(%d), pc_rate(%s) %s" % (page_num, price_interval, meta['pcRate'], response.url))
                page_num = 100

            # 页面总数不大于100的情况: 解析当前页面中的商品
            if page_num <= 100:
                # 解析一个页面中的所有商品
                selectors = response.xpath('//*[@id="container"]/div[2]/div[2]/div[4]/table/tbody/tr[*]')
                for index, selector in enumerate(selectors):
                    try:
                        # 从selector中解析出商品信息
                        url = selector.xpath('td[1]/div[@class="dis_inline_k offset20 dis_ine_p_k"]/p[1]/a/@href').extract()[0]
                        spid = url.split('/')[-1].split('.')[0]
                        nick1 = selector.xpath('td[1]/div[@class="dis_inline_k offset20 dis_ine_p_k"]/p[2]/em/text()').extract()
                        nick2 = selector.xpath('td[1]/div[@class="dis_inline_k offset20 dis_ine_p_k"]/p[2]/a/text()').extract()
                        nick = nick1[0] if nick1 else nick2[1]
                        nick = nick.replace('\r', '').replace('\n', '').replace(' ', '')

                        # 解析详情页
                        detail_meta = {"is_proxy": True, "spid": spid, "nick": nick}
                        yield scrapy.Request(url=url, meta=detail_meta, callback=self.parse_detail_page)
                    except Exception as e:
                        self.logger.error('parse product error(%s) in page: %s' % (e, response.url))
                        pass

                # 构造下一页请求并解析
                if meta['pageIndex'] < page_num:
                    meta['pageIndex'] += 1
                    url = make_url(**meta)
                    yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_list_page)

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
                # pass
                self.logger.error("product %s's category %s is not in mysql" % (spid, cid))
            else:
                try:
                    product_item['cid'] = cid
                    product_item['spid'] = spid
                    product_item['url'] = response.url
                    product_item['website'] = 'jd.com'
                    product_item['isCPS'] = True

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
                    nick3 = response.xpath('//*[@id="extInfo"]/div[@class="seller-infor"]/em/text()').extract()     # 右边
                    nick4 = response.xpath('//*[@id="popbox"]/div/div[1]/h3/a/@title').extract()    # 左边
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
                        meta = {'is_proxy': True, 'product_items': self.batch_product_items}
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
                "get product prices error, we should get %d, but actually get %d - %s" % (len(product_items), len(prices), response.url))
            yield scrapy.Request(url=response.url, meta=meta, callback=self.parse_price_and_comment)
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
                "get product comments error, we should get %d, but actually get %d - %s" % (len(product_items), len(comments), response.url))
            yield scrapy.Request(url=response.url, meta=meta, callback=self.parse_comment)
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
