# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
import pymongo
from jingdong.items import *


class JingdongPipeline(object):
    """将JdProductItem类型的item插入mongo库"""

    def __init__(self, mongo_uri, mongo_db, mongo_collection):
        self.client = pymongo.MongoClient(mongo_uri)
        self.collection = self.client[mongo_db][mongo_collection]
        self.product_items = []
        self.total_items = 0
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
        # 收集到1000个item, 批量插入
        if len(self.product_items) >= 1000:
            self.total_items += len(self.product_items)
            self.collection.insert_many(self.product_items)
            self.product_items = []
            self.logger.info("Put %d products into mongodb" % self.total_items)
        # 收集item
        else:
            if len(dict(item)) != PRODUCT_ITEM_FIELD_NUM:
                self.logger.error("Product %d's fields error - %s" % item)
            else:
                self.product_items.append(dict(item))

        return item

