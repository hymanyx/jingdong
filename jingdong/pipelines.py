# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import logging
import pymongo
from jingdong.items import *
from pybloom import ScalableBloomFilter
from scrapy.exceptions import DropItem


class JingdongPipeline(object):
    """将JdProductItem类型的item插入mongo库"""

    def __init__(self, mongo_uri, mongo_db, mongo_collection, failure_file):
        self.client = pymongo.MongoClient(mongo_uri)
        self.collection = self.client[mongo_db][mongo_collection]
        self.failure_file = open(failure_file, "w")
        self.product_items = []
        self.total_items = 0
        self.item_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION'),
            failure_file=crawler.settings.get("FAILURE_FILE")
        )

    def close_spider(self, spider):
        if self.product_items:
            self.total_items += len(self.product_items)
            self.collection.insert_many(self.product_items)
            self.product_items = []
            self.logger.info("Eventually put %d products into mongodb " % self.total_items)
        self.client.close()
        self.failure_file.close()

    def process_item(self, item, spider):
        spid = int(item['spid'])
        if self.item_filter.add(spid):
            raise DropItem("Duplicated product item: %d" % spid)
        else:
            if len(self.product_items) >= 2000:
                # 收集到2000个item, 则批量插入
                self.total_items += len(self.product_items)
                self.collection.insert_many(self.product_items)
                self.product_items = []
                self.logger.info("Put %d products into mongodb" % self.total_items)
            else:
                # 未收集到2000个item, 则继续收集
                if len(dict(item)) != PRODUCT_ITEM_FIELD_NUM:
                    # 没有获取到商品的所有字段信息, 则记录这些失败的商品
                    line = json.dumps(dict(item)) + "\n"
                    self.failure_file.write(line)
                else:
                    # 成功获取到商品的所有字段信息, 则收集item准备批量插入mongo
                    self.product_items.append(dict(item))
        return item


class JdLogPipeline(object):
    """将JdProductItem类型的item插入mongo库"""

    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.client = pymongo.MongoClient(mongo_uri)
        self.collection = self.client[mongo_db][mongo_collection]
        self.product_items = []
        self.total_items = 0
        self.item_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION'),
        )

    def close_spider(self, spider):
        if self.product_items:
            self.total_items += len(self.product_items)
            self.collection.insert_many(self.product_items)
            self.product_items = []
            self.logger.info("Eventually put %d products into mongodb " % self.total_items)
        self.client.close()

    def process_item(self, item, spider):
        spid = int(item['spid'])
        if self.item_filter.add(spid):
            raise DropItem("Duplicated product item: %d" % spid)
        else:
            if len(self.product_items) >= 100:
                # 收集到100个item, 则批量插入
                self.total_items += len(self.product_items)
                self.collection.insert_many(self.product_items)
                self.product_items = []
                self.logger.info("Put %d products into mongodb" % self.total_items)
            else:
                # 未收集到100个item, 则收集item准备批量插入mongo
                self.product_items.append(dict(item))
        return item


class JdCPSPipeline(object):
    """将JdCPSProductItem类型的item插入mongo库"""

    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.client = pymongo.MongoClient(mongo_uri)
        self.collection = self.client[mongo_db][mongo_collection]
        self.product_items = []
        self.total_items = 0
        self.item_filter = ScalableBloomFilter(mode=ScalableBloomFilter.LARGE_SET_GROWTH)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE'),
            mongo_collection=crawler.settings.get('MONGO_COLLECTION'),
        )

    def close_spider(self, spider):
        if self.product_items:
            self.total_items += len(self.product_items)
            self.collection.insert_many(self.product_items)
            self.product_items = []
            self.logger.info("Eventually put %d products into mongodb " % self.total_items)
        self.client.close()

    def process_item(self, item, spider):
        spid = int(item['spid'])

        if self.item_filter.add(spid):
            raise DropItem("Duplicated product item: %d" % spid)
        else:
            if len(self.product_items) >= 1000:
                # 收集到2000个item, 则批量插入
                self.total_items += len(self.product_items)
                self.collection.insert_many(self.product_items)
                self.product_items = []
                self.logger.info("Put %d products into mongodb" % self.total_items)
            else:
                # 收集item准备批量插入mongo
                self.product_items.append(dict(item))
        return item
