# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


PRODUCT_ITEM_FIELD_NUM = 12  # JdProductItem中字段总数


# 商品item(共12个字段)
class JdProductItem(scrapy.Item):
    cid = scrapy.Field()            # 类目    string
    nick = scrapy.Field()           # 店铺名称 string
    spid = scrapy.Field()           # 商品id   string
    url = scrapy.Field()            # 商品URL  string
    imageUrl = scrapy.Field()       # 图片URL string
    title = scrapy.Field()          # 标题    string
    website = scrapy.Field()        # 站点     string
    price = scrapy.Field()          # 价格    int
    promoPrice = scrapy.Field()     # 促销价  int
    volume = scrapy.Field()         # 销量    int
    feedbackCount = scrapy.Field()  # 评论数  int
    rate = scrapy.Field()           # 评价     json


# CPS商品item
class JdCPSProductItem(scrapy.Item):
    cid = scrapy.Field()            # 类目    string
    imageUrl = scrapy.Field()       # 图片URL string
    title = scrapy.Field()          # 标题    string
    nick = scrapy.Field()           # 店铺名称 string
    spid = scrapy.Field()           # 商品id   string
    url = scrapy.Field()            # 商品URL  string
    startDate = scrapy.Field()      # 推广开始时间
    endDate = scrapy.Field()        # 推广结束时间
    website = scrapy.Field()        # 站点     string
    isCps = scrapy.Field()          #
