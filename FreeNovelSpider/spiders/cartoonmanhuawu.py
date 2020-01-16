# -*- coding: utf-8 -*-
import base64
import os
import random
import re
from datetime import datetime
from lxml import etree
import json

from PIL import Image

import pymysql
import requests
import scrapy
from scrapy.utils.project import get_project_settings
from elasticsearch import Elasticsearch
from scrapy_redis.spiders import RedisSpider
from PIL import ImageFile
ImageFile.LOAD_TRUNCATED_IMAGES = True

base_path = '/mnt/pyProject/novelServer/static/cartoon'
# base_path = r'D:\usr\PycharmProjects\FreeNovelSpider\FreeNovelSpider\spiders'


class BiquSpider(RedisSpider):
    name = 'manhuawu'
    allowed_domains = ['kuman5.com/', 'si1.go2yd.com', 'pic.rmb.bdstatic.com']
    # http://client.api.kuman.com/kuman-cartoon/webCartoon/detail?cartoonId=107512 漫画详情页
    redis_key = 'manhuawuspider:start_urls'
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
        cartoonId = parse_url.split('/')[-2]
        name = response.xpath('//div[@class="info"]/h1/text()').extract_first()
        author = response.xpath('//div[@class="info"]/p[@class="subtitle"]/text()').extract_first()
        author = author.split('：')[-1]
        cover = response.xpath('//div[@class="banner_detail_form"]/div[@class="cover"]/img/@src').extract_first()
        # '已完结' '连载中'
        statu = response.xpath('//div[@class="info"]/p[@class="tip"]/span[@class="block"]/span/text()').extract_first()
        if statu == '已完结':
            statu = 1
        else:
            statu = 2
        # 冒险热血(2,6,16) 武侠格斗(16,2,1) 科幻魔幻(10,16,8) 侦探推理(5,17,8) 耽美爱情(3,5) 生活漫画(9,14,7)
        label_str = response.xpath('//div[@class="info"]/p[@class="tip"]/span[@class="block ticai"]/text()').extract_first()
        label_str = label_str.split('：')[-1]
        label_dict = {
            '冒险热血': '2,16', '武侠格斗': '16,2', '科幻魔幻': '10,16', '侦探推理': '5,17', '耽美爱情': '3,5', '生活漫画': '9,14,7', '玄幻科幻': '6,10'
        }
        if label_str in label_dict:
            label = label_dict[label_str]
            label_list = label.split(',')
            rand_num = random.randint(1, 18)
            while rand_num in label_list:
                rand_num = random.randint(1, 18)
            label += ',%s' % rand_num
        else:
            label = '17'
        updatetime = response.xpath('//div[@class="info"]/p[@class="tip"]/span[@class="block"][2]/text()').extract_first()
        updatetime = updatetime.split('：')[-1]
        info = response.xpath('//div[@class="info"]/p[@class="content"]/text()').extract_first().strip().split('：')[-1]
        chapter_url = 'http://www.kuman5.com' + response.xpath('//div[@class="bottom"]/a[3]/@href').extract_first()
        # 获取chapter详情
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36'
        }
        responsess = requests.get(url=chapter_url, headers=headers).text
        html = etree.HTML(responsess)
        chapter_url_list = html.xpath('//div[@id="chapterlistload"]/ul/li/a/@href')[::-1]
        chapter_name_list = html.xpath('//div[@id="chapterlistload"]/ul/li/a/text()')[::-1]

        # 判断数据库是否存在
        sql_fin = "select id from cartoon where cartoonId='%s' and webId='%s';"
        fin = cursor.execute(sql_fin % (cartoonId, '2'))
        chaptercount = len(chapter_url_list)
        # 漫画不存在
        if not fin:
            now_time = datetime.now()
            now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
            hotCount = random.randint(100, 999)
            subscriberCount = random.randint(100, 999)
            filename = '%s.jpg' % cover.split('/')[-1]
            # 存入数据库
            sql = "insert into cartoon(name,author,statu,label,hotcount,subcount,info,chaptercount,updatetime,addtime,cover,webId,cartoonId) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');"
            cursor.execute(sql % (name, author, statu, label, hotCount, subscriberCount,
                                  info, chaptercount, updatetime, now_time, filename, 2, cartoonId
                                  ))
            conn.commit()
            cursor.execute(sql_fin % (cartoonId, '2'))
            cartoonID = cursor.fetchone()[0]
            # typeId 和cartoonId匹配
            sql = "insert into cartoonid_typeid(typeId,cartoonId) values('%s','%s');"
            type_ = label.split(',')
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
                              body={"title": name, "timestamp": datetime.now()})
            except Exception as e:
                print('%s-----------%s' % (now_time, e))
            chapter_count = 0  # 记录章节数
            for x in range(chaptercount):
                chapter_url = chapter_url_list[x]
                chapter_name = chapter_name_list[x]
                chapter_count += 1
                url = 'http://www.kuman5.com' + chapter_url
                # 回调请求章节数据
                yield scrapy.Request(url=url, callback=self.chpater, meta={'chapter_name': chapter_name,
                                                                                   'cartoonID': cartoonID,
                                                                                   'chapter_count': chapter_count}, dont_filter=True)

        # 漫画存在
        else:
            cartoonID = cursor.fetchone()[0]
            chapter_count = 0
            for x in range(chaptercount):
                chapter_count += 1
                # 查找章节数据是否插入过
                sql = "select id from cartoon_chapter where cid='%s' and chapterId='%s';"
                fin = self.cursor.execute(sql % (cartoonID, chapter_count))
                if not fin:
                    # 更新漫画 章节数 更新时间
                    sql = "update cartoon set chaptercount='%s',updatetime='%s' where id='%s';"
                    self.cursor.execute(sql % (chaptercount, updatetime, cartoonID))
                    self.conn.commit()
                    # 拼接章节url http://mhpic.manhualang.com/comic/J/%E7%BB%9D%E4%B8%96%E5%94%90%E9%97%A8/%E5%BC%95GQV/1.jpg-kmmh.high.webp
                    chapter_url = chapter_url_list[x]
                    chapter_name = chapter_name_list[x]
                    url = 'http://www.kuman5.com' + chapter_url
                    # 回调请求章节数据
                    yield scrapy.Request(url=url, callback=self.chpater, meta={'chapter_name': chapter_name,
                                                                                       'cartoonID': cartoonID,
                                                                                       'chapter_count': chapter_count}, dont_filter=True)

    def download_img(self, path, url):
        response = requests.get(url=url, verify=False).content
        with open(path, 'wb') as f:
            f.write(response)

    def chpater(self, response):
        chapter_name = response.meta['chapter_name']
        cartoonID = response.meta['cartoonID']
        chapter_count = response.meta['chapter_count']
        # 正则匹配加密图片链接
        reg = "km5_img_url='(.*?)'"
        matchObj = re.search(reg, response.text)
        base_str = matchObj.group(1)
        new_str = base64.b64decode(base_str).decode("utf-8")
        new_str = new_str.replace('\\r', '')
        new_str = new_str.replace('\\', '')
        image_list = eval(new_str)
        endNum = len(image_list)
        # 章节插入数据库
        sql = "insert into cartoon_chapter(cid,name,startnum,endnum,isbuy,price,updatetime,chapterId) values('%s','%s','%s','%s','%s','%s','%s','%s');"
        self.cursor.execute(
            sql % (cartoonID, chapter_name, '1', endNum, '0', '0', '2009-09-23 08:00:08', chapter_count))
        self.conn.commit()
        # 创建章节文件夹
        # 判断文件夹是否存在
        chapter_path = '%s/%s/%s' % (base_path, cartoonID, chapter_count)
        if not os.path.exists(chapter_path):
            os.mkdir(chapter_path)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.80 Safari/537.36',
            'Referer': ''
        }
        for image in image_list:
            # 下载图片
            filename, img_url = image.split('|')
            filename = '%s.jpg' % filename
            path = '%s/%s/%s/%s' % (base_path, cartoonID, chapter_count, filename)

            yield scrapy.Request(url=img_url, headers=headers, callback=self.download_img_yield, meta={'path': path},
                                 dont_filter=True)

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
