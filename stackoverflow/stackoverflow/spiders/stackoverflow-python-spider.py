import scrapy
from stackoverflow.items import StackoverflowPythonItem

class StackoverflowPythonSpider(scrapy.Spider):

    name = "stackoverflow-python"

    def start_requests(self):
        urls = []
        _url = 'https://stackoverflow.com/questions/tagged/python?tab=votes&page={}&pagesize=15'

        for page in range(1, 84322):
            urls.append(_url.format(page))

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        question_list = response.xpath('//*[@id="questions"]')

        for question in question_list.xpath('./div'):
            item = StackoverflowPythonItem()
            item['_id'] = question.attrib['id']
            item['questions'] = question.xpath('div[2]/h3/a/text()').extract()
            item['votes'] = question.xpath(
                    'div[1]/div[1]/div[1]/div[1]/span/strong/text()').extract()
            item['answers'] = question.xpath(
                    'div[1]/div[1]/div[2]/strong/text()').extract()
            item['views'] = question.xpath('div[1]/div[2]/@title').extract()
            item['links'] = question.xpath('div[2]/h3/a/@href').extract()
            yield item




