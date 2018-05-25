#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2017/11/29 14:39
# @Author  : seankang


import os
import re
import time
import logging
import datetime
from contextlib import closing
from itertools import chain
from urlparse import urlparse
from logging.handlers import RotatingFileHandler
from multiprocessing.dummy import Pool as ThreadPool
from collections import namedtuple
from collections import defaultdict

import MySQLdb
import requests
from bs4 import BeautifulSoup

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
auth = ('xxx', 'xxx')

# 需要排除的 master ip
EXCLUDE = []

logger = logging.getLogger(__name__)

now_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# mysql params
HOST = '192.168.1.10'
PORT = 4051
USER = 'xxx'
PASSWORD = 'xxx'
DATABASE = 'report'

# tidb params
THOST = '192.168.1.11'
TPORT = 4000
TUSER = 'xxx'
TPASSWORD = 'xxx'
TDATABASE = 'report'

days_ago = (datetime.datetime.now() - datetime.timedelta(
    days=60)).date().strftime("%Y-%m-%d %H:%M:%S")
SQL_DELETE_60_DAYS = ("delete from bid_base_info_detail "
                      "where date < '{0}';".format(days_ago))

SQL_SAVE_BID_INFO = ("insert into bid_base_info_detail("
                     "date,"
                     "masterip,"
                     "bid,"
                     "objsize_min,"
                     "objsize_max,"
                     "allocapacity,"
                     "node_usedcnt,"
                     "node_maxcnt,"
                     "nodeperc,"
                     "objsize_usedcapa,"
                     "objsize_maxcapa,"
                     "objperc,"
                     "aveperc,"
                     "maxperc) "
                     "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                     "on duplicate key update "
                     "objsize_min=VALUES(objsize_min),"
                     "objsize_max=VALUES(objsize_max),"
                     "allocapacity=VALUES(allocapacity),"
                     "node_usedcnt=VALUES(node_usedcnt),"
                     "node_maxcnt=VALUES(node_maxcnt),"
                     "nodeperc=VALUES(nodeperc),"
                     "objsize_usedcapa=VALUES(objsize_usedcapa),"
                     "objsize_maxcapa=VALUES(objsize_maxcapa),"
                     "objperc=VALUES(objperc),"
                     "aveperc=VALUES(aveperc),"
                     "maxperc=VALUES(maxperc);")


def setup_log():
    """
    设置日志,最多2个日志文件，每个文件最大10M
    """
    log_file = 'crawler.log'
    log_file = os.path.join(BASE_DIR, log_file)
    fh = RotatingFileHandler(log_file, maxBytes=10485760, backupCount=2)
    fmt = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
    date_fmt = '%Y-%m-%d %H:%M:%S'
    formatter = logging.Formatter(fmt, date_fmt)
    fh.setFormatter(formatter)
    logging.getLogger().addHandler(fh)
    logging.getLogger().setLevel(logging.INFO)


def get_oss_url(url):
    start = time.time()
    o = urlparse(url)
    url_root = 'http://' + o.hostname
    oss_path = []
    try:
        r = requests.get(url, timeout=60, auth=auth)
        # 手动指定response编码,requests可能会猜错
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find('table')
        if not table:
            logger.warn('table is None: %s' % url)
            return oss_path
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            set_path = cols[0].find('a').attrs.get('href')
            business_group = cols[2].text
            if business_group in ['ABC', 'DEF']:
                oss_path.append(url_root + set_path.encode('utf-8'))
    except Exception:
        logger.exception('failed')
    end = time.time()
    logger.info('get oss url elapsed time: %s' % str(round(end - start, 2)))
    return oss_path


def get_master_url(url):
    start = time.time()
    o = urlparse(url)
    base_url = 'http://' + o.hostname
    ip_regx = re.compile(r'(?:\d{1,3}\.){3}\d{1,3}')
    db_ip = ip_regx.findall(url)
    num = 500
    params = ('/cgi-bin/list.cgi'
              '?act=list&start=0&num=%s&ver=3.0&port=9020' % num)
    ip_addresses = defaultdict(list)
    urls = []
    if len(db_ip) > 0:
        db_ip = db_ip[0].encode('utf-8')
    try:
        r = requests.get(url, auth=auth)
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, "html.parser")
        table = soup.find('table')
        if not table:
            logger.warn('table is None: %s' % url)
            return []
        table_body = table.find('tbody')
        rows = table_body.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if cols[1].text != '-':
                msg = 'master ip: {0}'.format(cols[1].text.encode('utf-8'))
                logger.debug(msg)
                ip_addresses[db_ip].append(cols[1].text.strip())
        for db_ip, master_ip in ip_addresses.items():
            for mip in set(master_ip):
                m_url = base_url + params + '&dbip=%s&masterip=%s' % (
                    db_ip, mip)
                m = namedtuple('master', ['url', 'ip'])
                urls.append(m(m_url.encode('utf-8'), mip.encode('utf-8')))
    except Exception:
        logger.exception('failed')
    end = time.time()
    logger.info('get master url elapsed time: %s' % str(round(end - start, 2)))
    return urls


def generate_master_info():
    regions = ['ab', 'cd', 'de']
    site = 'http://{0}mem.xxx.com/'
    house = 'cgi-bin/mem_show_house.cgi'
    api_set = [site.format(r) + house for r in regions]
    urls = []
    try:
        pool = ThreadPool(len(api_set))
        oss_urls = pool.map(get_oss_url, api_set)
        oss_urls_list = []
        for u in chain.from_iterable(oss_urls):
            logger.debug('oss url: {0}'.format(u))
            oss_urls_list.append(u)
        pool = ThreadPool(len(oss_urls_list))
        master_info = pool.map(get_master_url, oss_urls_list)
        for info in chain.from_iterable(master_info):
            urls.append(info)
    except Exception:
        logger.exception('generate master info failed')
    return urls


class GetBidInfo(object):
    @staticmethod
    def parse_bid_info(url_and_ip):
        url, master_ip = url_and_ip
        if master_ip in EXCLUDE:
            logger.info('ignore master: %s' % master_ip)
            return []
        start = time.time()
        try:
            r = requests.get(url, timeout=60, auth=auth)
        except Exception:
            logger.exception('get %s failed' % url)
            return []
        if r.status_code != 200:
            msg = 'get {0} failed, status_code: {1}'.format(url,
                                                            r.status_code)
            logger.error(msg)
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        tbody = soup.find(id="bid_list_tbody")
        if not tbody:
            logger.info('tbody is None: %s' % url)
            return []
        if not tbody.find_all("tr"):
            logger.info('not find tr in tbody: %s' % url)
            return []
        bid_info = GetBidInfo.parse_tbody(master_ip, tbody)
        elapsed_time = round(time.time() - start, 2)
        msg = 'parse %s elapsed time: %s' % (master_ip, str(elapsed_time))
        logger.info(msg)
        return bid_info

    @staticmethod
    def parse_tbody(master_ip, tbody):
        """
        解析tbody，返回insert values的数据
        """
        bid_info = []
        try:
            for tr in tbody.find_all("tr"):
                td = list(tr.find_all("td"))
                # 只有前7列需要解析
                td = td[:8]
                bid = td[0].contents[0].contents[0]
                obj_size = td[3].contents[0]
                if ":" in obj_size:
                    obj_size_min = obj_size.split(":")[0]
                    obj_size_max = obj_size.split(":")[1]
                else:
                    obj_size_min = obj_size
                    obj_size_max = obj_size
                allocated_size = int(td[4].contents[0]) * 1000
                users = td[5].contents[0]
                users_max = td[5].attrs.get("title").split(" ")[0].split("=")[
                    1]
                users_percent = td[5].attrs.get("title").split(" ")[1]
                users_percent = users_percent.replace("(", "").replace("%)",
                                                                       "")
                data_size = td[6].contents[0].strip(' M')
                data_size_max = td[6].attrs.get("title")
                data_size_max = data_size_max.split(" ")[0].split("=")[1]
                data_percent = td[6].attrs.get("title").split(" ")[2]
                data_percent = data_percent.replace("(", "").replace("%)", "")
                used_percent = td[7].contents[0].contents[0].strip('%')
                max_percent = td[7].contents[2].contents[0].strip('%')
                # 组合为tuple是为了符合executemany参数要求
                bid_info.append((now_time,
                                 master_ip,
                                 bid,
                                 obj_size_min,
                                 obj_size_max,
                                 allocated_size,
                                 users,
                                 users_max,
                                 users_percent,
                                 data_size,
                                 data_size_max,
                                 data_percent,
                                 used_percent,
                                 max_percent))
        except Exception:
            logger.exception('parse tbody failed')
        return bid_info

    @staticmethod
    def worker(url_and_ip):
        start = time.time()
        url, master_ip = url_and_ip
        # 爬取页面数据并解析
        bid_info = GetBidInfo.parse_bid_info(url_and_ip)
        # 保存到数据库
        try:
            t1 = time.time()
            GetBidInfo.insert_to_database(bid_info)
            t2 = time.time()
            logger.info('%s insert elapsed time: %s' %
                        (master_ip, str(round(t2 - t1, 2))))
            t1 = time.time()
            GetBidInfo.insert_to_tidb_database(bid_info)
            t2 = time.time()
            logger.info('%s insert tidb elapsed time: %s' %
                        (master_ip, str(round(t2 - t1, 2))))
        except Exception:
            logger.exception('insert data to database exception:\n')
        end = time.time()
        logger.info('%s worker elapsed time: %s' %
                    (master_ip, str(round(end - start, 2))))

    @staticmethod
    def run():
        """
        程序入口
        """
        start = time.time()
        setup_log()
        # 每个url启动一个线程爬取页面数据并解析
        try:
            urls = generate_master_info()
            logger.info('total urls: %s' % len(urls))
            pool = ThreadPool(len(urls) + 1)
            pool.map(GetBidInfo.worker, urls)
            t1 = time.time()
            GetBidInfo.delete_old_data()
            t2 = time.time()
            logger.info('delete elapsed time: %s' % str(round(t2 - t1, 2)))
        except Exception:
            logger.exception('worker exception:\n')
        end = time.time()
        logger.info('elapsed time: %s' % str(round(end - start, 2)))

    @staticmethod
    def insert_to_database(bid_info):

        connection = MySQLdb.connect(host=HOST, port=PORT, user=USER,
                                     passwd=PASSWORD, db=DATABASE)
        try:
            with closing(connection.cursor()) as cursor:
                cursor.executemany(SQL_SAVE_BID_INFO, bid_info)
                cursor.execute("commit;")
        finally:
            connection.close()
            logging.debug('save bid info complete')

    @staticmethod
    def delete_old_data():

        connection = MySQLdb.connect(host=HOST, port=PORT, user=USER,
                                     passwd=PASSWORD, db=DATABASE)
        try:
            with closing(connection.cursor()) as cursor:
                cursor.execute(SQL_DELETE_60_DAYS)
                cursor.execute("commit;")
        finally:
            connection.close()
            logging.debug('delete bid info complete')

    @staticmethod
    def insert_to_tidb_database(bid_info):

        connection = MySQLdb.connect(host=THOST, port=TPORT, user=TUSER,
                                     passwd=TPASSWORD, db=TDATABASE)
        try:
            with closing(connection.cursor()) as cursor:
                cursor.executemany(SQL_SAVE_BID_INFO, bid_info)
                cursor.execute("commit;")
        finally:
            connection.close()
            logging.debug('save bid info complete')

    @staticmethod
    def delete_tidb_old_data():

        connection = MySQLdb.connect(host=THOST, port=TPORT, user=TUSER,
                                     passwd=TPASSWORD, db=TDATABASE)
        try:
            with closing(connection.cursor()) as cursor:
                cursor.execute(SQL_DELETE_60_DAYS)
                cursor.execute("commit;")
        finally:
            connection.close()
            logging.debug('delete bid info complete')


if __name__ == '__main__':
    GetBidInfo.run()
