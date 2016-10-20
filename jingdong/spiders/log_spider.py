# -*- coding: utf-8 -*-

import ast
import json
import scrapy
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

def get_spids(date):
    """Get history log, and get spids from it
    """
    import subprocess
    import datetime
    import glob
    import json

class ProductSpider(scrapy.Spider):
    name = "log_spider"
    start_urls = ["http://www.jd.com/allSort.aspx"]

    def parse(self, response):
        """ 为每个类目构造List页请求
        :param response: start_urls中的url所代表的页面, 在这里对其不做任何处理. 使用start_urls的作用是让spider能够进入parse函数执行我们的代码.
        """
        categories = get_categories()
        for category in categories:
            url = 'http://list.jd.com/list.html?cat=' + category  # 类目入口页
            meta = {
                'request-type': 'list',                           # 请求类型, middleware模块通过请求类型来决定是否使用代理
                'cid': category                                   # 三级类目
            }
            yield scrapy.Request(url=url, meta=meta, callback=self.parse_list)

    def parse_list(self, response):
        """ 解析List页
        :param response: 获取到的List页的http响应
        """

        # (在List页的html文件中)获取变量slaveWareList的值, 并将其转换成dict
        start = response.body.find("var slaveWareList =") + len("var slaveWareList =")  # 定位变量slaveWareList内容的起始位置
        end = response.body.find("var aosList =")                                       # 定位变量slaveWareList内容的终止位置
        slave_ware_list = response.body[start:end].replace('\n', '').replace(' ', '')   # 定位到的变量slaveWareList内容的结尾有多余的空格和回车符, 故去掉多余的空格和回车符
        slave_ware_list = slave_ware_list[:-1]                                          # 变量slaveWareList内容的结尾还有一个分号(;), 去掉这个分号
        slave_products_map = dict()
        try:
            slave_products_map = ast.literal_eval(slave_ware_list)                      # 将变量slaveWareList内容转换成dict
        except Exception, e:
            self.logger.error("can not convert string to dict %s, %s" % (e, response.url))
            return

        # (在List页的html文件中)通过xPath定位所有商品的html描述
        selectors = response.xpath('//*[@id="plist"]/ul/li[*]/div')
        if not selectors:
            self.logger.error("can not navigate products item in list page: %s" % response.url)
            return

        # 每个商品的html描述还可能进行更细的拆分
        product_selectors = []                                # 一个List页中所有商品的selector
        for selector in selectors:
            div_type = selector.xpath('@class').extract()[0]  # div_type的值为'gl-i-wrap'或'gl-i-wrap j-sku-item'
            if 'gl-i-wrap j-sku-item' == div_type:            # div_type的值为gl-i-wrap j-sku-item', 不能再拆分了
                product_selectors.append(selector)
            elif 'gl-i-wrap' == div_type:                     # div_type的值为'gl-i-wrap j-sku-item', 继续拆分更多的商品html描述
                selectors_ = selector.xpath('div/div[2]/div')
                for selector_ in selectors_:
                    product_selectors.append(selector_)
            else:
                self.logger.error("can not handle new type: %s, %s" % (div_type, response.url))

        # 从所有商品对应的html描述中解析出商品部分信息
        product_items_dict = dict()  # key时spid, value是解析出的商品信息
        for product_selector in product_selectors:
            product_items = self.parse_item(selector=product_selector, list_url=response.url, cid=response.meta['cid'], slave_products_map=slave_products_map)
            for product_item in product_items:
                if product_item:
                    spid = product_item['spid']
                    product_items_dict[spid] = product_item

        # 够造商品价格链接, 并获取价格(需要注意的是,每次最多只能获取100个商品的价格,故我在这里分割了product_items_dict)
        for sub_product_items_dict in split_dict(product_items_dict, 100):
            price_url = 'http://p.3.cn/prices/mgets?type=1&skuIds='
            for spid in sub_product_items_dict.keys():
                price_url = price_url + 'J_' + spid + ','
            meta = {'request-type': 'price', 'product_items_dict': sub_product_items_dict, 'product_items_num': len(sub_product_items_dict)}
            yield scrapy.Request(url=price_url, meta=meta, callback=self.parse_price_and_comment)

        # 够造获取下一个List页的链接, 并获取下一个List页
        next_page = response.xpath('//*[@id="J_bottomPage"]/span[1]/a[10]/@href').extract()
        if next_page:
            next_page = "http://list.jd.com" + next_page[0]
            yield scrapy.Request(url=next_page, meta=response.meta, callback=self.parse_list)
        else:
            self.logger.error("can not get next page of page %s", response.url)

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
            spid = selector.xpath('@data-sku').extract()
            title = selector.xpath('div[@class="p-name"]/a/em/text()').extract()
            url = selector.xpath('div[@class="p-name"]/a/@href').extract()
            nick = selector.xpath('div[@class="p-shop"]/@data-shop_name').extract()
            nick = (nick if nick else ['京东自营'.decode('utf-8')])
            imageUrl1 = selector.xpath('div[@class="p-img"]/a/img/@src').extract()
            imageUrl2 = selector.xpath('div[@class="p-img"]/a/img/@data-lazy-img').extract()
            imageUrl = (imageUrl1 if imageUrl1 else imageUrl2)

            # 构造商品主商品item
            main_product_item['cid'] = cid
            main_product_item['spid'] = spid[0]
            main_product_item['title'] = title[0]
            main_product_item['url'] = url[0]
            main_product_item['imageUrl'] = imageUrl[0]
            main_product_item['nick'] = nick[0]
            main_product_item['website'] = 'jd.com'
        except Exception as e:
            self.logger.error("parse product item error in list page: %s, %s, %s" % (list_url, e, main_product_item))
            return None
        product_items.append(main_product_item)

        # 解析出主商品的所有slave商品
        if int(spid) in slave_products_map.keys():
            for slave_products in slave_products_map[int(spid)]:
                for (slave_spid, slave_product) in slave_products.items():
                    slave_product_item = JdProductItem()
                    slave_product_item['cid'] = cid
                    slave_product_item['spid'] = slave_spid
                    slave_product_item['title'] = (slave_product['name'] if slave_product['name'] else main_product_item['title'])
                    slave_product_item['url'] = '//item.jd.com/' + slave_spid + '.html'
                    slave_product_item['imageUrl'] = '//img14.360buyimg.com/n7/' + slave_product['imageUrl']
                    slave_product_item['nick'] = main_product_item['nick']
                    slave_product_item['website'] = 'jd.com'
                    product_items.append(slave_product_item)
        else:
            # self.logger.error("product %s is not in slave_products_map %s" % (spid, list_url))
            pass

        return product_items

    def parse_price_and_comment(self, response):
        """解析评论及价格
        :param response: parse_list函数中的price_url的响应
        """

        product_items_dict = response.meta['product_items_dict']
        product_items_num = response.meta['product_items_num']

        # 解析价格
        prices = json.loads(response.body)
        if len(prices) != product_items_num:
            self.logger.error("get product prices error, we should get %d, but actually get %d" % (product_items_num, len(prices)))
        else:
            for price in prices:
                spid = price['id'].split('_')[1]                # 京东商品id
                promotion_price = int(float(price['p']) * 100)  # 促销价
                original_price = int(float(price['m']) * 100)   # 原价
                # 价格过滤
                if (0 >= promotion_price) or (0 >= original_price):
                    self.logger.error("product %s's price error: promotion price (%s), original price(%s)" % (
                    spid, promotion_price, original_price))
                    product_items_dict.pop(spid)                # 价格错误一般由下架引起,下架商品直接删除,没必要获取其评论
                else:
                    product_items_dict[spid]['promoPrice'] = promotion_price
                    product_items_dict[spid]['price'] = original_price

        # 构造商品评论链接, 获取评论并在parse_comment中解析
        comment_url = 'http://club.jd.com/comment/productCommentSummaries.action?my=pinglun&referenceIds='
        for spid in product_items_dict.keys():
            comment_url = comment_url + spid + ','
        meta = {'request-type': 'comment', 'product_items_dict': product_items_dict, 'product_items_num': len(product_items_dict)}
        yield scrapy.Request(url=comment_url, meta=meta, callback=self.parse_comment)

    def parse_comment(self, response):
        """解析评论
        :param response: parse_price_and_comment函数中comment_url的响应
        """
        product_items_dict = response.meta['product_items_dict']
        product_items_num = response.meta['product_items_num']

        # 解析评论
        comments = json.loads(response.body.decode('gbk'))
        comments = comments['CommentsCount']
        if len(comments) != product_items_num:
            self.logger.error(
                "get product comments error, we should get %d, but actually get %d" % (product_items_num, len(comments)))
        else:
            for comment in comments:
                spid = str(comment['SkuId'])                         # 京东商品id
                comment_count = comment['CommentCount']              # 商品评论数
                good_rate = str(comment['GoodRateShow']) + '%'       # 好评率
                general_rate = str(comment['GeneralRateShow']) + '%' # 中评率
                poor_rate = str(comment['PoorRateShow']) + '%'       # 差评率
                rate = {'good': good_rate, 'general': general_rate, 'poor': poor_rate}

                product_items_dict[spid]['volume'] = comment_count
                product_items_dict[spid]['feedbackCount'] = comment_count
                product_items_dict[spid]['rate'] = rate

        # yield所有product_item
        for product_item in product_items_dict.values():
            # self.logger.info("%s" % product_item)
            yield product_item
