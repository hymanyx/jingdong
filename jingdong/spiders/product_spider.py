# -*- coding: utf-8 -*-

import ast
import json
import scrapy
import datetime
import mysql.connector
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
        categories[category_code] = category_name

    conn.close()
    return categories


class ProductSpider(scrapy.Spider):
    name = "product_spider"
    start_urls = []

    custom_settings = {
        'USER_AGENT_LIST': './UserAgents.txt',
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
        'MONGO_URI': '199.155.122.196:27017',
        'MONGO_DATABASE': 'jingdong',
        'MONGO_COLLECTION': 'product_info_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S"),
        'FAILURE_FILE': './failure_' + datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    }

    def start_requests(self):
        """ 为每个类目构造List页请求
        :param response: start_urls中的url所代表的页面, 在这里对其不做任何处理. 使用start_urls的作用是让spider能够进入parse函数执行我们的代码.
        """
        categories = get_categories()
        for category_code, category_name in categories.iteritems():
            url = 'http://list.jd.com/list.html?cat=' + category_code
            meta = {
                'category_code': category_code,
                'category_name': category_name,
                'is_proxy': False,
                'page_index': 1,  # 当前页面索引号
                'parsed_product_num': 0,  # 解析出的商品总数
            }
            self.logger.info("make request for category %s - %s, %s" % (category_code, category_name, url))
            yield scrapy.Request(url=url, meta=meta, callback=self.parse_list_page)

    def parse_list_page(self, response):
        """ 解析List页
        :param response: 获取到的List页的http响应
        """
        # (在List页的html文件中)解析出商品总数
        total_product_num = \
        response.xpath('//*[@id="J_selector"]/div[@class="s-title"]/div[@class="st-ext"]/span/text()').extract()[0]

        # (在List页的html文件中)获取变量slaveWareList的值, 并将其转换成dict
        start = response.body.find("var slaveWareList =") + len("var slaveWareList =")  # 定位变量slaveWareList内容的起始位置
        end = response.body.find("var aosList =")  # 定位变量slaveWareList内容的终止位置
        slave_ware_list = response.body[start:end].replace('\n', '').replace(' ',
                                                                             '')  # 定位到的变量slaveWareList内容的结尾有多余的空格和回车符, 故去掉多余的空格和回车符
        slave_ware_list = slave_ware_list[:-1]  # 变量slaveWareList内容的结尾还有一个分号(;), 去掉这个分号
        slave_products_map = dict()
        try:
            slave_products_map = ast.literal_eval(slave_ware_list)  # 将变量slaveWareList内容转换成dict
        except Exception, e:
            self.logger.error("can not convert slaveVareList string to dict %s, %s" % (e, response.url))
            return

        # (在List页的html文件中)通过xPath定位所有商品的html描述
        product_selectors = []  # 一个List页中所有商品的selector
        selectors = response.xpath('//*[@id="plist"]/ul/li[*]/div')
        for selector in selectors:
            div_type = selector.xpath('@class').extract()[0]  # div_type的值为'gl-i-wrap'或'gl-i-wrap j-sku-item'
            if 'gl-i-wrap j-sku-item' == div_type:  # div_type的值为gl-i-wrap j-sku-item', 不能再拆分了
                product_selectors.append(selector)
            elif 'gl-i-wrap' == div_type:  # div_type的值为'gl-i-wrap j-sku-item', 继续拆分更多的商品html描述
                product_selectors.extend(selector.xpath('div/div[2]/div'))
            else:
                self.logger.error("can not handle new type: %s, %s" % (div_type, response.url))

        # 从所有商品对应的html描述中解析出商品部分信息
        product_items = dict()  # key时spid, value是解析出的商品信息
        for product_selector in product_selectors:
            product_items_ = self.parse_item(selector=product_selector, list_url=response.url,
                                             cid=response.meta['category_code'], slave_products_map=slave_products_map)
            for product_item in product_items_:
                if product_item:
                    spid = product_item['spid']
                    product_items[spid] = product_item
                else:
                    pass
        response.meta['parsed_product_num'] += len(product_items)
        self.logger.info("current page is %s, parse %s/%s products of category %s - %s" % (response.url,
                                                                                           response.meta[
                                                                                               'parsed_product_num'],
                                                                                           total_product_num,
                                                                                           response.meta[
                                                                                               'category_code'],
                                                                                           response.meta[
                                                                                               'category_name']))

        # 够造商品价格链接, 并获取价格(需要注意的是,每次最多只能获取100个商品的价格,故我在这里分割了product_items_dict)
        for sub_product_items in split_dict(product_items, 100):
            price_url = 'http://p.3.cn/prices/mgets?type=1&skuIds='
            for spid in sub_product_items.keys():
                price_url = price_url + 'J_' + spid + ','
            meta = {
                'is_proxy': True,
                'product_items': sub_product_items
            }
            yield scrapy.Request(url=price_url, meta=meta, callback=self.parse_price_and_comment)

        # 够造下一个List页请求
        url = response.xpath('//*[@id="J_bottomPage"]/span[1]/a[10]/@href').extract()
        if url:
            url = "http://list.jd.com" + url[0]
            response.meta['page_index'] += 1
            yield scrapy.Request(url=url, meta=response.meta, callback=self.parse_list_page)
        else:
            self.logger.info("finally parse %s/%s products of category %s - %s" % (response.meta['parsed_product_num'],
                                                                                   total_product_num,
                                                                                   response.meta['category_code'],
                                                                                   response.meta['category_name']))

    def parse_item(self, selector, list_url, cid, slave_products_map):
        """ 解析List页中的一个selector(在List页的html文件中通过xPath定位到的商品描述)对应的主商品和slave商品
        :param selector: 在List页中定位到的一个商品html描述
        :param list_url: selector中商品所属的List页，用于日志输出调试
        :param cid: List页中所有商品的类目号
        :param slave_products_map: 从List页中解析出的所有slave商品
        """
        product_items = []  # 元素类型为JdProductItem, 表示一个selector中解析出的所有商品(包括主商品和slave商品)

        # 解析出主商品
        main_product_item = JdProductItem()
        try:
            # 从页面中解析主商品信息
            main_product_item['cid'] = cid
            main_product_item['spid'] = selector.xpath('@data-sku').extract()[0]
            main_product_item['title'] = selector.xpath('div[@class="p-name"]/a/em/text()').extract()[0]
            main_product_item['url'] = selector.xpath('div[@class="p-name"]/a/@href').extract()[0]
            nick = selector.xpath('div[@class="p-shop"]/@data-shop_name').extract()
            main_product_item['nick'] = (nick if nick else ['京东自营'.decode('utf-8')])[0]
            imageUrl1 = selector.xpath('div[@class="p-img"]/a/img/@src').extract()
            imageUrl2 = selector.xpath('div[@class="p-img"]/a/img/@data-lazy-img').extract()
            main_product_item['imageUrl'] = (imageUrl1 if imageUrl1 else imageUrl2)[0]
            main_product_item['website'] = 'jd.com'
        except Exception as e:
            self.logger.error("parse product item error in list page: %s, %s, %s" % (list_url, e, main_product_item))
            return None

        # 将主商品加入列表
        product_items.append(main_product_item)

        # 解析出主商品的所有slave商品
        spid = int(main_product_item['spid'])
        if spid in slave_products_map.keys():
            for slave_products in slave_products_map[spid]:
                for (slave_spid, slave_product) in slave_products.items():
                    slave_product_item = JdProductItem()
                    slave_product_item['cid'] = cid
                    slave_product_item['spid'] = slave_spid
                    slave_product_item['title'] = (
                    slave_product['name'] if slave_product['name'] else main_product_item['title'])
                    slave_product_item['url'] = '//item.jd.com/' + slave_spid + '.html'
                    slave_product_item['imageUrl'] = 'http://img14.360buyimg.com/n1/' + slave_product['imageUrl']
                    slave_product_item['nick'] = main_product_item['nick']
                    slave_product_item['website'] = 'jd.com'
                    product_items.append(slave_product_item)

        # 返回所有主从商品
        return product_items

    def parse_price_and_comment(self, response):
        """解析评论及价格
        :param response: parse_list函数中的price_url的响应
        """
        meta = response.meta
        product_items = meta['product_items']

        # 解析价格
        prices = json.loads(response.body)
        if len(prices) != len(product_items):
            self.logger.error(
                "get product prices error, we should get %d, but actually get %d" % (len(product_items), len(prices)))
        else:
            for price in prices:
                spid = price['id'].split('_')[1]  # 京东商品id
                promotion_price = int(float(price['p']) * 100)  # 促销价
                original_price = int(float(price['m']) * 100)  # 原价
                # 价格过滤
                if (0 >= promotion_price) or (0 >= original_price):
                    self.logger.error("product %s's price error: promotion price (%s), original price(%s)" % (
                        spid, promotion_price, original_price))
                    product_items.pop(spid)  # 价格错误一般由下架引起,下架商品直接删除,没必要获取其评论
                else:
                    product_items[spid]['promoPrice'] = promotion_price
                    product_items[spid]['price'] = original_price

        # 构造商品评论请求
        comment_url = 'http://club.jd.com/comment/productCommentSummaries.action?my=pinglun&referenceIds='
        for spid in product_items.keys():
            comment_url = comment_url + spid + ','
        meta['product_items'] = product_items
        yield scrapy.Request(url=comment_url, meta=meta, callback=self.parse_comment)

    def parse_comment(self, response):
        """解析评论
        :param response: parse_price_and_comment函数中comment_url的响应
        """
        product_items = response.meta['product_items']

        # 解析评论
        comments = json.loads(response.body.decode('gbk'))
        comments = comments['CommentsCount']
        if len(comments) != len(product_items):
            self.logger.error(
                "get product comments error, we should get %d, but actually get %d" % (
                len(product_items), len(comments)))
        else:
            for comment in comments:
                spid = str(comment['SkuId'])                            # 京东商品id
                comment_count = comment['CommentCount']                 # 商品评论数
                good_rate = str(comment['GoodRateShow']) + '%'          # 好评率
                general_rate = str(comment['GeneralRateShow']) + '%'    # 中评率
                poor_rate = str(comment['PoorRateShow']) + '%'          # 差评率
                rate = {'good': good_rate, 'general': general_rate, 'poor': poor_rate}

                product_items[spid]['volume'] = comment_count
                product_items[spid]['feedbackCount'] = comment_count
                product_items[spid]['rate'] = rate

        # yield所有product_item
        for product_item in product_items.values():
            yield product_item
