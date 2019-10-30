# -*- coding: utf-8 -*-

import re
from datetime import datetime

import time
import pymysql
import scrapy
from scrapy.utils.project import get_project_settings
from elasticsearch import Elasticsearch
from scrapy_redis.spiders import RedisSpider


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


class XBiquSpider(RedisSpider):
    name = 'qb5tw'
    allowed_domains = ['www.qb5.tw']
    # start_urls = ['http://www.biquga.com/']
    # http://www.xbiquge.la/42/42515/  小说详情页
    redis_key = 'qb5tw:start_urls'
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
        # https://www.qb5.tw/shu/112022.html
        parse_url = response.url
        # 解析当前url
        bookId = parse_url.split('/')[-1].split('.')[0]
        conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, password=self.password, db=self.dbname, charset=self.dbcharset)
        self.conn = conn
        cursor = conn.cursor()
        # 解析小说
        novel_name = response.xpath('//div[@id="info"]/h1/text()').extract_first()
        novel_author = response.xpath('//div[@class="options"]/a[1]/text()').extract_first().split('：')[1]
        novel_img = response.xpath('//div[@class="img_in"]/img/@src').extract_first()
        novel_infos = response.xpath('//div[@id="intro"]/text()')
        novel_info = '\t\r'.join(x.extract().strip() for x in novel_infos)
        novel_lastupt = response.xpath('//div[@class="update"]/text()').getall()[1].strip().strip('（）')
        novel_type = response.xpath('//div[@class="nav-mbx"]/a[2]/text()').extract_first()
        chapters = response.xpath('//dl[@class="zjlist"]/dd/a/@href')
        word_count = response.xpath('//div[@class="options"]/span[1]/text()').extract_first()
        # 正则匹配到数字
        pattern = re.compile(r'\d+')   # 查找数字
        novel_word_count = '%s0000' % pattern.findall(word_count)[0]
        type_dict = {'仙侠修真': '5', '玄幻魔法': '4', '都市言情': '3', '历史军事': '6', '网游竞技': '14', '科幻灵异': '8', '恐怖惊悚': '7', '其他类型': '4'}
        try:
            label = type_dict[novel_type]
        except:
            label = '4'
        updated = time.mktime(time.strptime(novel_lastupt, "%Y-%m-%d %H:%M"))
        # 判断小说是否存在数据库
        sql_find = "select id from novels where name='%s' and bookId='%s' and novel_web=3;"
        cursor.execute(sql_find % (novel_name, bookId))
        fin = cursor.fetchone()
        if not fin:
            # 采集
            words = novel_word_count
            now_time = datetime.now()
            now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
            setargs(novel_author, [], '都市', conn)    # 将作者添加作者表 words created
            sql = "select id from author where name='%s';"
            cursor.execute(sql % novel_author)
            try:
                authorId = cursor.fetchone()[0]  # 作者id
            except Exception as e:
                authorId = 170
                print(e)
            sql = "insert into novels(name,cover,summary,label,state,words,created,updated,authorId,target,score,bookId,addtime,novel_web,updatetime) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"
            cursor.execute(sql % (
                novel_name, novel_img, novel_info, label, '1', words, '0', updated, authorId, '', '9.5',  bookId, now_time, '3', now_time
            ))
            conn.commit()
            # 获取小说id
            sql = "select id from novels where bookId='%s' and name='%s';"
            cursor.execute(sql % (bookId, novel_name))
            novelId = cursor.fetchone()[0]
            # 将小说名字添加到elasticsearch索引
            self.es.index(index="novel-index", id=novelId, body={"title": novel_name, "timestamp": datetime.now()})
            chapter_count = 0  # 记录章节总数
            for chapter in chapters:
                chapter_count += 1
                # 拼接章节url http://www.xbiquge.la/19/19523/10080224.html
                url = 'https://www.qb5.tw' + chapter.extract()
                yield scrapy.Request(url=url, callback=self.parse_chaper, meta={'novelId': novelId, 'bookId': bookId, 'chapterId': chapter_count})
        else:
            #更新
            novelId = fin[0]
            now_time = datetime.now()
            now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
            chapter_count = 0   # 记录章节总数
            for chapter in chapters:
                chapter_count += 1
                # 判断章节是否存在
                sql_find = "select id from chapters where novelId='%s' and chapterId='%s';"
                chapter_url = chapter.extract()
                chapterId  = chapter_count
                # 判断数据库是否存在 novelId chapterId
                if not cursor.execute(sql_find % (novelId, chapterId)):
                    # 更新小说表小说更新时间
                    sql_update = "update novels set updated='%s',updatetime='%s' where id='%s';"
                    cursor.execute(sql_update % (updated, now_time, novelId))
                    conn.commit()
                    # 拼接章节url https://www.qb5.tw/shu/40714/13641063.html
                    url = 'https://www.qb5.tw' + chapter_url
                    yield scrapy.Request(url=url, callback=self.parse_chaper, meta={'novelId': novelId, 'bookId': bookId, 'chapterId': chapter_count})
        sql = "update novels set chaptercount= '%s' where id='%s';"
        cursor.execute(sql % (chapter_count, novelId))
        conn.commit()

    def parse_chaper(self, response):
        '''请求章节数据'''
        # https://www.qb5.tw/shu/40714/13834628.html
        cursor = self.conn.cursor()
        chapterId = response.meta['chapterId']
        novelId = response.meta['novelId']
        bookId = response.meta['bookId']
        # 获取章节名字
        chapter_name = response.xpath('//div[@id="main"]/h1/text()').extract_first()
        contents = response.xpath('//div[@id="content"]/text()')[1:-1]
        content = '\t\r'.join(x.extract().strip() for x in contents).replace('\t\r\t\r', '\t\r')

        words = len(content)
        # 将内容插入章节
        sql = "insert into chapters(name,created,updated,words,novelId,chapterId) values('%s','%s','%s','%s','%s','%s');"
        sql_find = "select id from chapters where novelId='%s' and chapterId='%s';"
        # 判断数据库是否存在 novelId chapterId
        if not cursor.execute(sql_find % (bookId, chapterId)):
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

