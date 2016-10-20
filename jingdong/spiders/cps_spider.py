# -*- coding: utf-8 -*-

import scrapy
from jingdong.items import JdCPSProductItem


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
            'jingdong.pipelines.JdCPSPipeline': 300
        },

        'MONGO_URI': '199.155.122.197:27017',
        'MONGO_DATABASE': 'jingdong',
        'MONGO_COLLECTION': 'cps_product_info_20161019',
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
        # "12473"  农用物资
        # "12259"  酒类
        # "737"  家用电器
        # "9847"  家具
        # "6144"  珠宝首饰
        # "1318"  运动户外
        # "9192"  医药保健
        # "652"  数码                   26400
        # "1315"  服饰内衣
        # "6994"  宠物生活
        # "6196"  厨具
        # "1320"  食品饮料
        # "4053"  教育音像
        # "6728"  汽车用品
        # "9855"  家装建材
        # "1316"  个护化妆
        # "670"  电脑、办公
        # "5025"  钟表
        # "11729"  鞋靴
        # "1620"  家居家装
        # "4051"  音乐
        # "9987"  手机
        # "1319"  母婴
        # "1672"  礼品箱包
        # "6233"  玩具乐器              60300 55137
        # "12218"  生鲜
        # "4052"  影视
        # "1713"  图书 -
        # "4938"  本地生活/旅游出行 -
        # "6322"  养生保健 -
        # "6323"  家用器械 -
        # "0"     全部类目 -

        # 去掉部分无用类目
        categories.remove('0')      # 全部类目
        categories.remove('1713')   # 图书
        categories.remove('4938')   # 本地生活/旅游出行
        categories.remove('6322')   # 养生保健
        categories.remove('6323')   # 家用器械

        # 构造按价格从高到低排序的url请求, (从返回页面中)解析出每个类目中所有商品的最高价
        for category in categories:
            meta = {
                'pageIndex': 1,
                'category1': category
            }
            url = make_url(**meta)
            yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_secondary_category)

    def parse_secondary_category(self, response):
        """解析parse函数中url链接所表示的页面, 从中提取出一级类目中所有商品的最高价, 并为该一级类目构造带有价格区间的多个请求
        :param response: parse函数中url链接所表示的页面
        """
        # 采集该一级类目下商品的经营类型, 及二级类目
        ad_owner_types = response.xpath('//*[@id="a_block"]/div[1]/div/label[*]/a/@data-value').extract()[1:]
        secondary_categories = response.xpath('//*[@id="two_categories"]/li[*]/label/a/@data-value').extract()

        # 从response中获得meta
        meta = response.meta.copy()
        meta.pop('download_timeout')
        meta.pop('download_latency')
        meta.pop('download_slot')
        meta.pop('depth')

        # 构造带有经营类型和二级类目的请求
        for ad_owner_type in ad_owner_types:
            for secondary_category in secondary_categories:
                meta['category'] = secondary_category
                meta['adownerType'] = ad_owner_type
                meta['property'] = 'pcPrice'
                meta['sort'] = 'desc'
                url = make_url(**meta)
                yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_highest_price)

    def parse_highest_price(self, response):
        """解析parse_secondary_category函数中url链接所表示的页面, 从中提取出二级类目中所有商品的最高价, 并为该二级类目构造带有价格区间的多个请求
        :param response: parse函数中url链接所表示的页面
        """
        try:
            # 解析出一个二级类目中所有商品的最高价
            pc_price = response.xpath('//*[@id="container"]/div[2]/div[2]/div[4]/table/tbody/tr[1]/td[2]/text()').extract()[0]
            pc_price = pc_price.replace(u'\r\n', '').replace(u' ', '').replace(u'￥', '').replace(u'PC：', '').replace(',', '')
            self.logger.info('category[%s,%s] highest price: %s, %s' % (response.meta['category1'], response.meta['category'], pc_price, response.url))

            # 当前价格区间
            from_price = 0
            to_price = int(float(pc_price) + 1)    # 加1是因为float转int会损失精度
            price_interval = to_price - from_price

            # 从response中获得meta
            meta = response.meta.copy()
            meta.pop('download_timeout')
            meta.pop('download_latency')
            meta.pop('download_slot')
            meta.pop('depth')

            # 为该二级类目构造带有价格区间的多个请求
            prices = [i for i in range(from_price, to_price, price_interval / 2)]
            prices.append(to_price)
            for i in range(len(prices) - 1):
                meta['fromPrice'] = str(prices[i] + 0.01)
                meta['toPrice'] = str(prices[i + 1] + 0.0)
                url = make_url(**meta)
                meta['prices'] = prices
                yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_list_page)
        except Exception, e:
            self.logger.error('page is empty, can not get highest price of category[%s,%s]. %s, %s' %
                              (response.meta['category1'], response.meta['category'], response.url, e))

    def parse_list_page(self, response):
        """解析parse函数中一个url所表示的页面, 并从selector中解析出商品信息
        :param response: parse函数中一个请求的响应.
        """
        # 从页面中获取页面总数page_num
        page_num = response.xpath('//*[@id="container"]/div[2]/div[2]/div[5]/ul[1]/li[1]/text()').extract()[0][1:-1]
        page_num = int(page_num)

        # 从response中获得meta
        meta = response.meta.copy()
        meta.pop('download_timeout')
        meta.pop('download_latency')
        meta.pop('download_slot')
        meta.pop('depth')
        meta.pop('prices')

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
            self.logger.error("prase first 100 pages: page_num(%d), price_inverval(%d), pc_rate(%s) %s" % (page_num, price_interval, response.meta['pcRate'], response.url))
            page_num = 100

        # # 页面总数不大于100的情况: 解析当前页面中的商品
        if page_num <= 100:
            selectors = response.xpath('//*[@id="container"]/div[2]/div[2]/div[4]/table/tbody/tr[*]')
            for index, selector in enumerate(selectors):
                product_item = JdCPSProductItem()
                try:
                    # 从selector中解析出商品信息
                    product_item['url'] = selector.xpath('td[1]/div[@class="dis_inline_k offset20 dis_ine_p_k"]/p[1]/a/@href').extract()[0]
                    product_item['spid'] = product_item['url'].split('/')[-1].split('.')[0]
                    product_item['imageUrl'] = selector.xpath('td[8]/a/@data-imgurl').extract()[0]
                    product_item['title'] = selector.xpath('td[8]/a/@data-title').extract()[0]
                    product_item['nick'] = selector.xpath('td[8]/a/@data-shopname').extract()[0]
                    product_item['startDate'] = selector.xpath('td[8]/a/@data-startdate').extract()[0]
                    product_item['endDate'] = selector.xpath('td[8]/a/@data-enddate').extract()[0]
                    product_item['website'] = 'jd.com'
                    product_item['isCps'] = True
                    product_item['cid'] = response.meta['category1']
                    yield product_item
                except Exception as e:
                    self.logger.error('parse empty page, %s, %s' % (e, response.url))
                    pass

            # 构造下一页请求并解析
            if response.meta['pageIndex'] < page_num:
                meta['pageIndex'] += 1
                url = make_url(**meta)
                yield scrapy.Request(url=url, headers=self.headers, meta=meta, callback=self.parse_list_page)
