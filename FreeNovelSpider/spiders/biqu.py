# -*- coding: utf-8 -*-
from datetime import datetime
import json
import time
import pymysql
import scrapy
from scrapy.utils.project import get_project_settings
from elasticsearch import Elasticsearch
from scrapy_redis.spiders import RedisSpider
# http://www.xbiquge.la/

def setargs(author, target, label, conn):
    cursor = conn.cursor()
    # 现在时间
    now_time = datetime.now()
    now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
    sql = "select id from author where name='%s';"
    if not cursor.execute(sql % author):
        sql_author = "insert into author(name,addtime) values('%s','%s');"
        cursor.execute(sql_author % (author, now_time))

    for tag in target:
        # 判断没有了添加
        sql = "select id from novel_tag where target='%s';"
        if not cursor.execute(sql % tag):
            sql_target = "insert into novel_tag(target,addtime) values('%s','%s');"
            cursor.execute(sql_target % (tag, now_time))
    sql = "select id from novel_type where type='%s';"
    if not cursor.execute(sql % label):
        sql_label = "insert into novel_type(type,addtime) values('%s','%s');"
        cursor.execute(sql_label % (label, now_time))
    conn.commit()


class BiquSpider(RedisSpider):
    name = 'biqu'
    allowed_domains = ['www.biquga.com']
    # start_urls = ['http://www.biquga.com/']
    # https://www.biquga.com/34_34057/ 小说详情页
    redis_key = 'biquspider:start_urls'
    # 将配置文件读到内存中，是一个字典
    settings = get_project_settings()
    host = settings['DB_HOST']
    port = settings['DB_PORT']
    user = settings['DB_USER']
    password = settings['DB_PASSWORD']
    dbname = settings['DB_NAME']
    dbcharset = settings['DB_CHARSET']
    # es_host = settings['ES_HOST']

    conn = ''
    es = Elasticsearch()

    def parse(self, response):
        parse_url = response.url
        # 解析当前url
        bookId = parse_url.split('_')[1]
        conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, password=self.password, db=self.dbname, charset=self.dbcharset)
        self.conn = conn
        cursor = conn.cursor()
        # 解析小说
        novel_name = response.xpath('//div[@id="info"]/h1/text()').extract_first()
        novel_author = response.xpath('//div[@id="info"]/p[1]/text()').extract_first().split('：')[1]
        novel_img = 'https://www.biquga.com' + response.xpath('//div[@id="fmimg"]/img/@src').extract_first()
        novel_info = response.xpath('//div[@id="intro"]/p').extract_first()
        novel_lastupt = response.xpath('//div[@id="info"]/p[3]/text()').extract_first().split('：')[1]
        novel_type = response.xpath('//div[@id="info"]/p[2]/text()').extract_first().split('：')[1]
        type_dict = {'修真小说': '5', '玄幻小说': '4', '都市小说': '3', '历史小说': '6', '网游小说': '8', '科幻小说': '8', '其他小说': '4'}
        try:
            label = type_dict[novel_type]
        except:
            label = '4'
        updated = time.mktime(time.strptime(novel_lastupt, "%Y/%m/%d"))
        # 判断小说是否存在数据库
        sql_find = "select id from novels name='%s' and bookId='%s';"
        cursor.execute(sql_find % (novel_name, bookId))
        fin = cursor.fetchone()
        if not fin:
            # 采集
            setargs(novel_author, [], '都市', conn)    # 将作者添加作者表
            sql = "insert into novels(name,cover,summary,label,state,words,created,authorId,target,score,bookId,addtime) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"
            cursor.execute(sql % (
                novel_name, novel_img, novel_info, label,
            ))

        else:
            #更新
            pass
        # 最后更新时间

        # 现在时间
        now_time = datetime.datetime.now()
        now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
