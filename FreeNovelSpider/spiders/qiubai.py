# -*- coding: utf-8 -*-
import scrapy
import pymysql
import os


class QiubaiSpider(scrapy.Spider):
    name = 'qiubai'
    allowed_domains = ['img.moban5.net', 'www.qiushibaike.com']
    start_urls = ['http://www.qiushibaike.com/']
    conn = ''
    cursor = ''

    def parse(self, response):
        base_uri = os.getcwd()
        self.conn = pymysql.Connect(host='192.168.1.91', port=3306, user='admin', password='admin', db='moban5',
                               charset='utf8')
        self.cursor = self.conn.cursor()
        sql = "select id,morepic from www_moban5_cn_ecms_game;"
        self.cursor.execute(sql)
        results = self.cursor.fetchall()
        # http://img.moban5.net/eyu/d/file/moban5/2017011319/5875fc7eb514b.jpg
        for result in results:
            bid = result[0]
            pic_str = result[1]
            if not pic_str:
                continue
            pic_list = pic_str.split('::::::')
            for x in pic_list:
                if x:
                    pic = x.strip('\n')
                    yield scrapy.Request(url=pic, callback=self.download_img, meta={'bid': bid, 'base_uri': base_uri})
            # 更新数据库路径
            sql = "update www_moban5_cn_ecms_game set morepic='%s' where id='%s';"
            new_pic_str = pic_str.replace('http://img.moban5.net/eyu', '')
            self.cursor.execute(sql % (new_pic_str, bid))
            self.conn.commit()

    def download_img(self, response):
        # 路径
        bid = response.meta['bid']
        base_uri = response.meta['base_uri']

        request_url = response.url
        path_list = request_url.split('/')
        img_name = path_list[-1]
        new_path = path_list[4:-1]
        ba_path = '/'.join(x for x in new_path)
        path = '%s/%s' % (base_uri, ba_path)
        img_pa = '%s/%s' % (ba_path, img_name)
        # 判断文件夹是否存在
        if not os.path.exists(path):
            os.makedirs(path)
        img_path = '%s/%s' % (path, img_name)
        with open(img_path, 'wb') as f:
            f.write(response.body)

