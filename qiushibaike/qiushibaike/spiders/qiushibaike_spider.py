# -*- coding: utf-8 -*-
import random

import scrapy

from qiushibaike.items import QiushibaikeItem


class QiushiSpider(scrapy.Spider):
    # 这里定义一个唯一的名称，用来标识糗事的爬虫，在项目中不能和别的爬虫名称一样，等会会用到这个名称
    name = "qiushibaike"


    def start_requests(self):
        urls = [
            'https://www.qiushibaike.com/text/page/1/',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        content_left_div = response.xpath('//*[@id="content-left"]')
        content_list_div = content_left_div.xpath('./div')

        for content_div in content_list_div:
            item = QiushibaikeItem()
            item['author'] = content_div.xpath('./div/a[2]/h2/text()').get()
            item['content'] = content_div.xpath('./a/div/span/text()').getall()
            item['_id'] = content_div.attrib['id']
            yield item

        next_page = response.xpath('//*[@id="content-left"]/ul/li[last()]/a').attrib['href']

        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)