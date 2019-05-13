# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymongo


class QiushibaikePipeline(object):

    def __init__(self):
        self.connection = pymongo.MongoClient('localhost', 27017)
        self.db = self.connection.scrapy
        self.collection = self.db.qiushibaike

    def process_item(self, item, spider):
        if not self.connection or not item:
            return
        self.collection.save(item)

    def __del__(self):
        if self.connection:
            self.connection.close()


