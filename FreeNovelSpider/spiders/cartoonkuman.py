# -*- coding: utf-8 -*-
import os
from datetime import datetime
import json

from PIL import Image

import pymysql
import requests
import scrapy
from scrapy.utils.project import get_project_settings
from elasticsearch import Elasticsearch
from scrapy_redis.spiders import RedisSpider


base_path = '/root/pyProject/novelServer/static/cartoon'


class BiquSpider(RedisSpider):
    name = 'kuman'
    allowed_domains = ['www.kuman.com', 'mhpic.manhualang.com', 'www.isamanhua.com', 'mhpic.isamanhua.com']
    # http://client.api.kuman.com/kuman-cartoon/webCartoon/detail?cartoonId=107512 漫画详情页
    redis_key = 'kumanspider:start_urls'
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
    cursor = ''
    es = Elasticsearch()

    def parse(self, response):
        conn = pymysql.Connect(host=self.host, port=self.port, user=self.user, password=self.password, db=self.dbname,
                               charset=self.dbcharset)
        self.conn = conn
        cursor = conn.cursor()
        self.cursor = cursor
        parse_url = response.url
        cartoonId = parse_url.split('=')[-1]
        res = json.loads(response.body)
        if res['code'] == 200:
            # 现在时间
            now_time = datetime.now()
            now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
            data = res['data']
            cartoonName = data['cartoonName']
            cartoonAuthor = data['cartoonAuthorListName']
            cover = data['coverList'][0]
            status = data['cartoonStatus']
            lastChapterTime = data['lastChapterTime']
            # 时间戳转时间字符串、
            dateArray = datetime.fromtimestamp(lastChapterTime)
            updatetime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
            desc = data['desc']
            subscriberCount = data['subscriberCount']  # 追漫人数
            hotCount = data['hotCount'][:-4]    # 漫画热度 万
            chapterList = data['chapterList']
            chaptercount = len(chapterList)
            type_list = data['cartoonKumanType']
            # 分类转换为id
            type_dict = {'新作': '1', '热血': '2', '恋爱': '3', '穿越': '4', '都市': '5', '玄幻': '6', '搞笑': '7',
                         '恐怖': '8', '生活': '9', '科幻': '10', '战争': '11', '古风': '12', '霸总': '13', '真人': '14',
                         '后宫': '15', '动作': '16', '其他': '17'}
            type_ = []
            for typ in type_list:
                typeId = type_dict.get(typ)
                if typeId:
                    type_.append(typeId)
            type_str = ','.join(x for x in type_)
            # 判断数据库是否存在
            sql_fin = "select id from cartoon where cartoonId='%s' and webId='%s';"
            fin = cursor.execute(sql_fin % (cartoonId, '1'))
            # 漫画不存在
            if not fin:
                filename = cover.split('/')[-1]
                # 存入数据库
                sql = "insert into cartoon(name,author,statu,label,hotcount,subcount,info,chaptercount,updatetime,addtime,cover,webId,cartoonId) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"
                cursor.execute(sql % (cartoonName, cartoonAuthor, status, type_str, hotCount, subscriberCount,
                                      desc, chaptercount, updatetime, now_time, filename, 1, cartoonId
                                      ))
                conn.commit()
                cursor.execute(sql_fin % (cartoonId, '1'))
                cartoonID = cursor.fetchone()[0]
                # typeId 和cartoonId匹配
                sql = "insert into cartoonid_typeid(typeId,cartoonId) values('%s','%s');"
                for typeId in type_:
                    cursor.execute(sql % (typeId, cartoonID))
                conn.commit()
                # 新建文件夹
                os.mkdir('%s/%s' % (base_path, str(cartoonID)))
                # 下载封面
                path = '%s/%s/%s' % (base_path, cartoonID, filename)
                self.download_img(path, cover)

                try:
                    # 将小说名字添加到elasticsearch索引
                    self.es.index(index="cartoon-index", id=cartoonID,
                                  body={"title": cartoonName, "timestamp": datetime.now()})
                except Exception as e:
                    print('%s-----------%s' % (now_time, e))
                chapter_count = 0  # 记录章节数
                for chapter in chapterList:
                    chapter_count += 1
                    # 拼接章节url http://mhpic.manhualang.com/comic/J/%E7%BB%9D%E4%B8%96%E5%94%90%E9%97%A8/%E5%BC%95GQV/1.jpg-kmmh.high.webp
                    chapterName = chapter['chapterName']
                    chapterImage = chapter['chapterImage']['high']
                    chapterDomain = chapter['chapterDomain']
                    startNum = chapter['startNum']
                    createDate = chapter['createDate']
                    # 时间戳转时间字符串、
                    dateArray = datetime.fromtimestamp(createDate)
                    updatetime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
                    endNum = chapter['endNum']
                    # 章节插入数据库
                    sql = "insert into cartoon_chapter(cid,name,startnum,endnum,isbuy,price,updatetime,chapterId) values('%s','%s','%s','%s','%s','%s','%s','%s');"
                    self.cursor.execute(
                        sql % (cartoonID, chapterName, startNum, endNum, '0', '0', updatetime, chapter_count))
                    self.conn.commit()
                    # 创建章节文件夹
                    os.mkdir('%s/%s/%s' % (base_path, cartoonID, chapter_count))
                    for x in range(int(startNum), int(endNum) + 1):
                        # 下载图片
                        href = chapterImage.replace('$$', str(x))
                        filename = href.split('/')[-1]
                        path = '%s/%s/%s/%s' % (base_path, cartoonID, chapter_count, filename)
                        url = 'http://%s%s' % (chapterDomain, href)
                        yield scrapy.Request(url=url, callback=self.download_img_yield, meta={'path': path})
            # 漫画存在
            else:
                cartoonID = cursor.fetchone()[0]
                chapter_count = 0
                for chapter in chapterList:
                    chapter_count += 1
                    # 查找章节数据是否插入过
                    sql = "select id from cartoon_chapter where cid='%s' and chapterId='%s';"
                    fin = self.cursor.execute(sql % (cartoonID, chapter_count))
                    if not fin:
                        # 拼接章节url http://mhpic.manhualang.com/comic/J/%E7%BB%9D%E4%B8%96%E5%94%90%E9%97%A8/%E5%BC%95GQV/1.jpg-kmmh.high.webp
                        chapterName = chapter['chapterName']
                        chapterImage = chapter['chapterImage']['high']
                        chapterDomain = chapter['chapterDomain']
                        startNum = chapter['startNum']
                        createDate = chapter['createDate']
                        # 时间戳转时间字符串、
                        dateArray = datetime.fromtimestamp(createDate)
                        updatetime = dateArray.strftime("%Y-%m-%d %H:%M:%S")
                        endNum = chapter['endNum']
                        # 章节插入数据库
                        sql = "insert into cartoon_chapter(cid,name,startnum,endnum,isbuy,price,updatetime,chapterId) values('%s','%s','%s','%s','%s','%s','%s','%s');"
                        self.cursor.execute(
                            sql % (cartoonID, chapterName, startNum, endNum, '0', '0', updatetime, chapter_count))
                        # 更新漫画更新日期和章节总数
                        sql_up = "update cartoon set updatetime='%s', chaptercount='%s';"
                        self.cursor.execute(sql_up % (updatetime, chapter_count))
                        self.conn.commit()
                        # 创建章节文件夹
                        os.mkdir('%s/%s/%s' % (base_path, cartoonID, chapter_count))
                        for x in range(int(startNum), int(endNum) + 1):
                            # 下载图片
                            href = chapterImage.replace('$$', str(x))
                            filename = href.split('/')[-1]
                            path = '%s/%s/%s/%s' % (base_path, cartoonID, chapter_count, filename)
                            url = 'http://%s%s' % (chapterDomain, href)
                            yield scrapy.Request(url=url, callback=self.download_img_yield, meta={'path': path})


    def download_img(self, path, url):
        response = requests.get(url=url, verify=False).content
        with open(path, 'wb') as f:
            f.write(response)

    def download_img_yield(self, response):
        path = response.meta['path']
        with open(path, 'wb') as f:
            f.write(response.body)
        # 添加水印
        im = Image.open(path)
        h = im.height
        # 获取一个Image对象，参数分别是RGB模式。宽150，高30，
        # mark = Image.new('RGBA',(250,80),color)
        mark = Image.open("mark.jpg")
        layer = Image.new('RGBA', im.size, (0, 0, 0, 0))
        layer.paste(mark, (0, h - 80))
        out = Image.composite(layer, im, layer)
        out.save(path)
