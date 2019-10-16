# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class StackoverflowPythonItem(scrapy.Item):
    _id = scrapy.Field()
    questions = scrapy.Field()
    votes = scrapy.Field()
    answers = scrapy.Field()
    views = scrapy.Field()
    links = scrapy.Field()

