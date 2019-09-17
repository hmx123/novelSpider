# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from scrapy.utils.project import get_project_settings

class FreenovelspiderPipeline(object):
    def open_spider(self, spider):
        # 连接数据库
        # self.conn = pymysql.Connect(host='127.0.0.1', port=3306, user='root', password='123456', db='movie', charset='utf8')

        # 将配置文件读到内存中，是一个字典
        settings = get_project_settings()
        host = settings['DB_HOST']
        port = settings['DB_PORT']
        user = settings['DB_USER']
        password = settings['DB_PASSWORD']
        dbname = settings['DB_NAME']
        dbcharset = settings['DB_CHARSET']

        self.conn = pymysql.Connect(host=host, port=port, user=user, password=password, db=dbname, charset=dbcharset)

    def process_item(self, item, spider):
        type = 1
        # 执行sql语句
        self.cursor = self.conn.cursor()
        # 去数据库查找分类
        sql = 'select id from novel_brief where name="%s";'
        find = self.cursor.execute(sql % item['novel_name'])
        if find:
            brief_id = self.cursor.fetchone()[0]
        else:
            brief_id = 0

        # 写入数据库中
        sql = 'insert into novel_detail(type_id, brief_id, title, content, name) values("%s", "%s", "%s", "%s", "%s")' % (
            type, brief_id, pymysql.escape_string(item['title']), pymysql.escape_string(item['content']),
            pymysql.escape_string(item['novel_name']))
        try:
            self.cursor.execute(sql)
            print('#' * 10)
            self.conn.commit()
        except Exception as e:
            print('*' * 10)
            print(e)
            self.conn.rollback()

        return item

    def close_spider(self, spider):
        self.cursor.close()
        self.conn.close()
