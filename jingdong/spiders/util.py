# -*- coding: utf-8 -*-

import mysql.connector


def get_categories():
    """从MySql中获取所有的京东三级类目"""
    config = {'user': 'predictwr', 'password': 'Wrk7predict32K8qpWR', 'host': '192.168.3.58',
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


# def get_keywords():
#     """从MySQL中获取所有的淘宝热词"""
#     config = {'user': 'root', 'password': 'myroot', 'host': '199.155.122.203', 'database': 'tts_spider_cps'}
#     query = "SELECT keyword FROM t_spider_hot_keyword_temp"
#     conn = mysql.connector.connect(**config)
#     cursor = conn.cursor()
#     cursor.execute(query)
#
#     keywords = []
#     for keyword in cursor:
#         keywords.append(keyword[0])
#
#     conn.close()
#     return keywords
