# -*- coding: utf-8 -*-

import mysql.connector
from itertools import islice





def get_keywords():
    """从MySQL中获取所有的淘宝热词"""
    config = {'user': 'root', 'password': 'myroot', 'host': '199.155.122.203', 'database': 'tts_spider_cps'}
    query = "SELECT keyword FROM t_spider_hot_keyword_temp"
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(query)

    keywords = []
    for keyword in cursor:
        keywords.append(keyword[0])

    conn.close()
    return keywords


def split_dict(my_dict, max_size_of_sub_dict):
    """将一个python字典分割成多个小字典, 且每个小字典的元素数量不多于max_size_of_sub_dict
    :param my_dict: 待分割的python字典
    :param max_size_of_sub_dict：小字典中的最大元素数
    """
    it = iter(my_dict)
    for i in xrange(0, len(my_dict), max_size_of_sub_dict):
        yield {key: my_dict[key] for key in islice(it, max_size_of_sub_dict)}