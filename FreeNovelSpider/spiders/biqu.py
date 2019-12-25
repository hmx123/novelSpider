# -*- coding: utf-8 -*-
import re
from datetime import datetime
import json
import time
import pymysql
import scrapy
from scrapy.utils.project import get_project_settings
from elasticsearch import Elasticsearch
from scrapy_redis.spiders import RedisSpider
# https://www.xinxs.la/

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
    allowed_domains = ['www.xinxs.la']
    # start_urls = ['http://www.biquga.com/']
    # https://www.biquga.com/34_34057/ 小说详情页
    redis_key = 'xinxsspider:start_urls'
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
        bookId = parse_url.split('_')[1].strip('/')
        conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, password=self.password, db=self.dbname, charset=self.dbcharset)
        self.conn = conn
        cursor = conn.cursor()
        # 解析小说
        novel_name = response.xpath('//div[@id="info"]/h1/text()').extract_first()
        novel_author = response.xpath('//div[@id="info"]/p[1]/text()').extract_first().split('：')[1]
        novel_img = 'https://www.xinxs.la' + response.xpath('//div[@id="fmimg"]/img/@src').extract_first()
        novel_info = response.xpath('//div[@id="intro"]/text()').extract_first().strip()
        novel_lastupt = response.xpath('//div[@id="info"]/p[3]/text()').extract_first().split('：')[1]
        novel_type = response.xpath('//meta[@property="og:novel:category"]/@content').extract_first()
        chapter_list = response.xpath('//div[@id="list"]/dl/dd/a/@href')
        chapters = []
        # 判断是否有 斜杠 否则pass
        for chapter in chapter_list:
            chapter_str = chapter.extract()
            if '/' not in chapter_str:
                continue
            chapters.append(chapter_str)
        type_dict = {'武侠仙侠': '5', '玄幻奇幻': '4', '都市言情': '9', '历史军事': '6', '网游竞技': '21', '科幻灵异': '8', '女频频道': '9'}
        try:
            label = type_dict[novel_type]
        except:
            label = '4'
        try:
            updated = int(time.mktime(time.strptime(novel_lastupt, "%d/%m/%Y %H:%M:%S %p")))
        except:
            updated = int(time.mktime(time.strptime(novel_lastupt, "%m/%d/%Y %H:%M:%S %p")))
        # 判断小说是否存在数据库
        sql_find = "select id from novels where name='%s' and bookId='%s' and novel_web=4;"
        cursor.execute(sql_find % (novel_name, bookId))
        fin = cursor.fetchone()
        if not fin:
            # 采集
            words = 0
            now_time = datetime.now()
            now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
            setargs(novel_author, [], '都市', conn)  # 将作者添加作者表 words created
            sql = "select id from author where name='%s';"
            cursor.execute(sql % novel_author)
            try:
                authorId = cursor.fetchone()[0]  # 作者id
            except Exception as e:
                authorId = 170
                print(e)
            sql = "insert into novels(name,cover,summary,label,state,words,created,updated,authorId,target,score,bookId,addtime,novel_web,updatetime) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"
            cursor.execute(sql % (
                novel_name, novel_img, novel_info, label, '1', words, '0', updated, authorId, '', '9.5', bookId,
                now_time, '4', now_time
            ))
            conn.commit()
            # 获取小说id
            sql = "select id from novels where bookId='%s' and name='%s';"
            cursor.execute(sql % (bookId, novel_name))
            novelId = cursor.fetchone()[0]
            try:
                # 将小说名字添加到elasticsearch索引
                self.es.index(index="novel-index", id=novelId, body={"title": novel_name, "timestamp": datetime.now()})
            except Exception as e:
                print('%s-----------%s' % (now_time, e))
            chapter_count = 0  # 记录章节总数
            for chapter in chapters:
                chapter_count += 1
                # 拼接章节url http://www.xbiquge.la/19/19523/10080224.html
                url = 'https://www.xinxs.la' + chapter
                yield scrapy.Request(url=url, callback=self.parse_chaper,
                                     meta={'novelId': novelId, 'bookId': bookId, 'chapterId': chapter_count})
        else:
            # 更新
            novelId = fin[0]
            now_time = datetime.now()
            now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
            chapter_count = 0  # 记录章节总数
            for chapter in chapters:
                chapter_count += 1
                # 判断章节是否存在
                sql_find = "select id from chapters where novelId='%s' and chapterId='%s';"
                chapter_url = chapter
                chapterId = chapter_count
                # 判断数据库是否存在 novelId chapterId
                if not cursor.execute(sql_find % (novelId, chapterId)):
                    # 更新小说表小说更新时间
                    sql_update = "update novels set updated='%s',updatetime='%s' where id='%s';"
                    cursor.execute(sql_update % (updated, now_time, novelId))
                    conn.commit()
                    # 拼接章节url http://www.xbiquge.la/19/19523/10080224.html
                    url = 'https://www.xinxs.la' + chapter_url
                    yield scrapy.Request(url=url, callback=self.parse_chaper,
                                         meta={'novelId': novelId, 'bookId': bookId, 'chapterId': chapter_count})
        sql = "update novels set chaptercount= '%s' where id='%s';"
        cursor.execute(sql % (chapter_count, novelId))
        conn.commit()

    def parse_chaper(self, response):
        '''请求章节数据'''
        # https://www.xinxs.la/3_3143/1839901.html
        cursor = self.conn.cursor()
        chapterId = response.meta['chapterId']
        novelId = response.meta['novelId']
        bookId = response.meta['bookId']
        # 获取章节名字
        chapter_name = response.xpath('//div[@class="bookname"]/h1/text()').extract_first()
        content = response.xpath('//div[@id="content"]').extract_first()

        # 去掉div  去掉空格  正则匹配p标签 去掉
        content = content.replace('<div id="content">', '').replace('</div>', '')
        reg = 'Ps:书友们(.*)'
        content = re.sub(reg, '', content)
        content = content.replace('<script type="text/javascript" src="/js/chaptererror.js"></script>', '')
        content = content.replace('　', '').replace('<br><br>', '\t\r').replace('<br>', '\t\r').replace(' ', '').strip()
        words = len(content)
        # 将内容插入章节
        sql = "insert into chapters(name,created,updated,words,novelId,chapterId) values('%s','%s','%s','%s','%s','%s');"
        sql_find = "select id from chapters where novelId='%s' and chapterId='%s';"
        # 判断数据库是否存在 novelId chapterId
        if not cursor.execute(sql_find % (novelId, chapterId)):
            # 章节内容
            cursor.execute(sql % (pymysql.escape_string(chapter_name), '0', '0', words, novelId, chapterId))
            self.conn.commit()
            # 获取章节id  将内容插入内容表
            # 获取章节的id
            cursor.execute(sql_find % (novelId, chapterId))
            chapterid = cursor.fetchone()[0]
            # 将数据保存数据库
            sql = "insert into contents(title,content,created,updated,words,chapterId,novelId) values('%s','%s','%s','%s','%s','%s','%s');"
            cursor.execute(sql % (
                pymysql.escape_string(chapter_name), pymysql.escape_string(content), '0', '0', words, chapterid, novelId
            ))
            self.conn.commit()
