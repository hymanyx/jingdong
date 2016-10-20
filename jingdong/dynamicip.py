# -*- coding: utf-8 -*-
import json
import logging
from kazoo.client import KazooClient


class DynamicIP(object):
    def __init__(self, hosts, watch_node):
        self.proxies = dict()
        self.watch_node = watch_node
        self.zk_client = KazooClient(hosts=hosts)
        self.zk_client.start()
        self.logger = logging.getLogger(__name__)

    def watcher(self, proxy_ids):
        current_proxy_ids = set(self.proxies.keys())
        newcome_proxy_ids = set(proxy_ids)

        # 删除失效的代理
        expried_proxy_ids = current_proxy_ids - newcome_proxy_ids
        for expried_proxy_id in expried_proxy_ids:
            expried_proxy = self.proxies.pop(expried_proxy_id)
            # self.logger.info('expried proxy: %s' % expried_proxy)

        # 新增代理
        new_proxy_ids = newcome_proxy_ids - current_proxy_ids
        for new_proxy_id in new_proxy_ids:
            try:
                ip, stat = self.zk_client.get(self.watch_node + '/' + new_proxy_id)
                data, stat = self.zk_client.get('/adsl_proxy/ip' + '/' + ip)
                data = json.loads(data)
                self.proxies[new_proxy_id] = data['host']
                self.logger.info('new proxy: %s' % data['host'])
            except Exception as e:
                pass

    def get_proxy(self):
        if len(self.proxies):
            max_proxy_id = max(self.proxies.keys())
            return self.proxies[max_proxy_id]
        else:
            return None

    def run(self):
        self.zk_client.ChildrenWatch(self.watch_node, self.watcher)
