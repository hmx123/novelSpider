# -*- coding: utf-8 -*-
from datetime import datetime
import json

import pymysql
import scrapy
from scrapy.utils.project import get_project_settings
from elasticsearch import Elasticsearch



# from FreeNovelSpider.spiders.util import chin_to_num


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



class NovelSpider(scrapy.Spider):
    name = 'novel'
    allowed_domains = ['reader.browser.duokan.com']
    # 玄幻书籍 categoryId=3
    start_urls = ['https://reader.browser.duokan.com/api/v2/book/list2?len=10&page=1&sex=1&bookStatus=0&categoryId=10&wordCountsInterval=0&hotChoice=0']
    # 都市
    # https://reader.browser.duokan.com/api/v2/book/list2?len=10&page=1&sex=1&bookStatus=0&categoryId=7&wordCountsInterval=0&hotChoice=0
    # 仙侠
    #https://reader.browser.duokan.com/api/v2/book/list2?len=3&page=1&sex=1&bookStatus=0&categoryId=6&wordCountsInterval=0&hotChoice=0
    # 历史
    # https://reader.browser.duokan.com/api/v2/book/list2?len=3&page=1&sex=1&bookStatus=0&categoryId=8&wordCountsInterval=0&hotChoice=0
    # 灵异
    #https://reader.browser.duokan.com/api/v2/book/list2?len=10&page=1&sex=1&bookStatus=0&categoryId=10&wordCountsInterval=0&hotChoice=0
    # 将配置文件读到内存中，是一个字典
    settings = get_project_settings()
    host = settings['DB_HOST']
    port = settings['DB_PORT']
    user = settings['DB_USER']
    password = settings['DB_PASSWORD']
    dbname = settings['DB_NAME']
    dbcharset = settings['DB_CHARSET']
    # es_host = settings['ES_HOST']

    conn = pymysql.Connect(host=host, port=port, user=user, password=password, db=dbname, charset=dbcharset)
    es = Elasticsearch()
    # es.indices.create(index='novel-index', ignore=400)

    def parse(self, response):
        results = json.loads(response.text)
        cursor = self.conn.cursor()
        if results['status'] == 0 and results['data']:
            datalist = results['data']['list']
            for book in datalist:
                bookId = book['bookId']  # 获取小说id 回调请求章节数据
                name = book['name']
                cover = book['imgUrl']
                summary = book['description']
                label = book['secondCategoryName']  # 小说分类 回调设置到分类表 后获取
                state = book['bookStatus']
                if state == '连载':
                    state = 1
                elif state == '完结':
                    state = 2
                else:
                    state = 3
                #enabled = 1   是否可读
                words = book['wordCount']
                created = book['updateTime']
                # updated = 最后更新时间  获取最后一章节后 设置
                author = book['author']    # 作者 回调设置到作者表 后获取
                target = book['tags']   # 标签 设置标签表中回调 后获取
                score = book['score']
                # 判断小说是否存在
                sql = "select id from novels where bookId='%s';"
                if not cursor.execute(sql % bookId):
                    setargs(author, target, label, self.conn)
                    # 保存数据库
                    # 从数据库中获取关联id
                    sql = "select id from author where name='%s';"
                    cursor.execute(sql % author)
                    authorId = cursor.fetchone()[0]  # 作者id
                    tag_list = []
                    for tag in target:
                        sql = "select id from novel_tag where target='%s';"
                        cursor.execute(sql % tag)
                        tagId = cursor.fetchone()[0]
                        tag_list.append(tagId)
                    tag_str = ','.join(str(x) for x in tag_list)   # 标签id字符串
                    sql = "select id from novel_type where type='%s';"
                    cursor.execute(sql % label)
                    typeId = cursor.fetchone()[0]    # 分类id
                    sql = "insert into novels(name,cover,summary,label,state,words,created,authorId,target,score,bookId) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"
                    cursor.execute(sql % (
                        name, cover, summary, typeId, state, words, created, authorId, tag_str, score, bookId
                    ))
                    self.conn.commit()

                    # 中间表关联小说id 和 标签 id
                    # 获取小说id
                    sql = "select id from novels where bookId='%s';"
                    cursor.execute(sql % bookId)
                    novelId = cursor.fetchone()[0]
                    sql = "insert into middle_tag_nov(tagId,novelId) values('%s','%s');"
                    for tagid in tag_list:
                        cursor.execute(sql % (tagid, novelId))
                    self.conn.commit()
                    # 将小说名字添加到elasticsearch索引
                    self.es.index(index="novel-index", id=novelId, body={"title": name, "timestamp": datetime.now()})
                    # 拼接章节url
                    url = 'https://reader.browser.duokan.com/api/v2/chapter/list/%s' % bookId
                    yield scrapy.Request(url=url, callback=self.parse_chaper, meta={'novelId': novelId, 'bookId': bookId})

    def parse_chaper(self, response):
        '''请求章节数据'''
        novelId = response.meta['novelId']
        bookId = response.meta['bookId']
        results = json.loads(response.text)
        cursor = self.conn.cursor()
        if results['status'] == 0 and results['data']:
            datalist = results['data']['list']
            sql = "insert into chapters(name,created,updated,novelId,chapterId) values('%s','%s','%s','%s','%s');"
            sql_find = "select id from chapters where novelId='%s' and chapterId='%s';"
            i = ''
            for chapter in datalist:
                name = chapter['chapterName']
                chapterId = chapter['chapterId']
                # 判断数据库是否存在 novelId chapterId
                if not cursor.execute(sql_find % (novelId, chapterId)):
                    # 根据章节名称获取章节num
                    #number = chin_to_num(name)
                    created = int(str(chapter['updateTime'])[0:10])
                    i = created
                    cursor.execute(sql % (name, created, created, novelId, chapterId))
                    self.conn.commit()
                    # 获取章节的id
                    cursor.execute(sql_find % (novelId, chapterId))
                    chapterid = cursor.fetchone()[0]
                    # 回调获取章节详情内容
                    url = 'https://reader.browser.duokan.com/api/v2/chapter/content/%s/?chapterId=%s&volumeId=1' % (bookId, chapterId)
                    yield scrapy.Request(url=url, callback=self.parse_content, meta={'chapterid': chapterid, 'novelId': novelId})

            # 把最后的更新时间添加到书籍
            sql = "update novels set updated='%s' where id='%s'"
            cursor.execute(sql % (i, novelId))
            self.conn.commit()


    def parse_content(self, response):
        '''请求内容数据'''
        chapterid = response.meta['chapterid']
        novelId = response.meta['novelId']
        results = json.loads(response.text)
        if results['status'] == 0:
            cursor = self.conn.cursor()
            data = results['data']
            title = data['title']
            contentList = data['contentList']
            content = '\t\r'.join(x for x in contentList)
            words = len(content)
            # 数据库获取该章节更新时间
            sql = "select updated from chapters where id='%s';"
            cursor.execute(sql % chapterid)
            updated = cursor.fetchone()[0]
            # 将数据保存数据库
            sql = "insert into contents(title,content,created,updated,words,chapterId,novelId) values('%s','%s','%s','%s','%s','%s','%s');"
            cursor.execute(sql % (
                title, content, updated, updated, words, chapterid, novelId
            ))
            self.conn.commit()
