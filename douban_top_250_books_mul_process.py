import requests
from bs4 import BeautifulSoup
import xlwt
import multiprocessing
import time
import sys

def request_douban(url):
    try:
        response = requests.get(url,headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})
        if response.status_code == 200:
            return response.text
    except requests.RequestException:
        return None

def main(url):
    sys.setrecursionlimit(1000000)
    data = []
    html = request_douban(url)
    # soup = BeautifulSoup(html, 'lxml')
    soup = BeautifulSoup(html, 'html.parser')
    list = soup.find(class_='grid_view').find_all('li')
    for item in list:
        item_name = item.find(class_='title').string
        item_img = item.find('a').find('img').get('src')
        item_index = item.find(class_='').string
        item_score = item.find(class_='rating_num').string
        item_author = item.find('p').text
        item_intr = ''
        if (item.find(class_='inq') != None):
            item_intr = item.find(class_='inq').string
        print('爬取电影：' + item_index + ' | ' + item_name + ' | ' + item_score + ' | ' + item_intr)
        item = {
            'item_index': item_index,
            'item_name': item_name,
            'item_score': item_score,
            'item_intr': item_intr,
            'item_img': item_img,
            'item_author': item_author
        }
        data.append(item)
    return data
    
if __name__ == '__main__':
    startTime = time.time()
    data = []
    urls = []
    pool = multiprocessing.Pool(multiprocessing.cpu_count()-1)
    for i in range(0, 10):
        url = 'https://movie.douban.com/top250?start=' + str(i * 25) + '&filter='
        urls.append(url)
    pool.map(main, urls)
    for pageItem in pool.map(main, urls):
        data.extend(pageItem)
    book = xlwt.Workbook(encoding='utf-8', style_compression=0)
    sheet = book.add_sheet('豆瓣电影Top250-test', cell_overwrite_ok=True)
    sheet.write(0, 0, '名称')
    sheet.write(0, 1, '图片')
    sheet.write(0, 2, '排名')
    sheet.write(0, 3, '评分')
    sheet.write(0, 4, '作者')
    sheet.write(0, 5, '简介')
    for index,item in enumerate(data):
        sheet.write(index+1, 0, item['item_name'])
        sheet.write(index+1, 1, item['item_img'])
        sheet.write(index+1, 2, item['item_index'])
        sheet.write(index+1, 3, item['item_score'])
        sheet.write(index+1, 4, item['item_author'])
        sheet.write(index+1, 5, item['item_intr'])
    book.save(u'豆瓣最受欢迎的250部电影-mul.xlsx')

    endTime = time.time()
    dtime = endTime - startTime
    print("程序运行时间：%s s" % dtime)  # 4.036666631698608 s