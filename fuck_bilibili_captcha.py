import time
import requests
from PIL import Image
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import re
from io import BytesIO

driver = webdriver.Chrome('/usr/lib/chromium-browser/chromedriver')
WAIT = WebDriverWait(driver, 10)
url = 'https://passport.bilibili.com/login'


def mergy_Image(image_file, location_list):
    """
    将原始图片进行合成
    :param image_file: 图片文件
    :param location_list: 图片位置
    :return: 合成新的图片
    """

    # 存放上下部分的各个小块
    upper_half_list = []
    down_half_list = []

    image = Image.open(image_file)

    # 通过 y 的位置来判断是上半部分还是下半部分,然后切割
    for location in location_list:
        if location['y'] == -58:
            # 间距为10，y：58-116
            im = image.crop((abs(location['x']), 58, abs(location['x'])+10, 116))
            upper_half_list.append(im)
        if location['y'] == 0:
            # 间距为10，y：0-58
            im = image.crop((abs(location['x']), 0, abs(location['x']) + 10, 58))
            down_half_list.append(im)

    # 创建一张大小一样的图片
    new_image = Image.new('RGB', (260, 116))

    # 粘贴好上半部分 y坐标是从上到下（0-116）
    offset = 0
    for im in upper_half_list:
        new_image.paste(im, (offset, 0))
        offset += 10

    # 粘贴好下半部分
    offset = 0
    for im in down_half_list:
        new_image.paste(im, (offset, 58))
        offset += 10

    return new_image


def get_distance(bg_Image, fullbg_Image):

    # 阈值
    threshold = 200

    print(bg_Image.size[0])
    print(bg_Image.size[1])


    for i in range(60, bg_Image.size[0]):
        for j in range(bg_Image.size[1]):
            bg_pix = bg_Image.getpixel((i, j))
            fullbg_pix = fullbg_Image.getpixel((i, j))
            r = abs(bg_pix[0] - fullbg_pix[0])
            g = abs(bg_pix[1] - fullbg_pix[1])
            b = abs(bg_pix[2] - fullbg_pix[2])

            if r + g + b > threshold:
               return i




def get_path(distance):
        result = []
        current = 0
        mid = distance * 4 / 5
        t = 0.2
        v = 0
        while current < (distance - 10):
            if current < mid:
                a = 2
            else:
                a = -3
            v0 = v
            v = v0 + a * t
            s = v0 * t + 0.5 * a * t * t
            current += s
            result.append(round(s))
        return result


def start_drag(driver, distance):

    # 被妖怪吃掉了
    # knob =  WAIT.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#gc-box > div > div.gt_slider > div.gt_slider_knob.gt_show")))
    # ActionChains(driver).click_and_hold(knob).perform()
    # ActionChains(driver).move_by_offset(xoffset=distance, yoffset=0.1).perform()
    # time.sleep(0.5)
    # ActionChains(driver).release(knob).perform()

    # 被妖怪吃掉了
    # ActionChains(driver).drag_and_drop_by_offset(knob, distance-10, 0).perform()

    knob = WAIT.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#gc-box > div > div.gt_slider > div.gt_slider_knob.gt_show")))
    result = get_path(distance)
    ActionChains(driver).click_and_hold(knob).perform()

    for x in result:
        ActionChains(driver).move_by_offset(xoffset=x, yoffset=0).perform()

    time.sleep(0.5)
    ActionChains(driver).release(knob).perform()


def recognize_code(driver):
    """
    识别滑动验证码
    :param driver: selenium驱动
    :return:
    """

    bs = BeautifulSoup(driver.page_source,'lxml')
    # 找到背景图片和缺口图片的div
    bg_div = bs.find_all(class_='gt_cut_bg_slice')
    fullbg_div = bs.find_all(class_='gt_cut_fullbg_slice')

    # 获取缺口背景图片url
    bg_url = re.findall('background-image:\surl\("(.*?)"\)',bg_div[0].get('style'))
    # 获取背景图片url
    fullbg_url = re.findall('background-image:\surl\("(.*?)"\)',fullbg_div[0].get('style'))

    # 存放每个合成缺口背景图片的位置
    bg_location_list = []
    # 存放每个合成背景图片的位置
    fullbg_location_list = []

    for bg in bg_div:
        location = {}
        location['x'] = int(re.findall('background-position:\s(.*?)px\s(.*?)px;', bg.get('style'))[0][0])
        location['y'] = int(re.findall('background-position:\s(.*?)px\s(.*?)px;', bg.get('style'))[0][1])
        bg_location_list.append(location)

    for fullbg in fullbg_div:
        location = {}
        location['x'] = int(re.findall('background-position:\s(.*?)px\s(.*?)px;', fullbg.get('style'))[0][0])
        location['y'] = int(re.findall('background-position:\s(.*?)px\s(.*?)px;', fullbg.get('style'))[0][1])
        fullbg_location_list.append(location)

    print(bg_location_list)
    print(fullbg_location_list)

    # 将图片格式存为 jpg 格式
    bg_url = bg_url[0].replace('webp', 'jpg')
    fullbg_url = fullbg_url[0].replace('webp', 'jpg')
    # print(bg_url)
    # print(fullbg_url)

    # 下载图片
    bg_image = requests.get(bg_url).content
    fullbg_image = requests.get(fullbg_url).content
    print('完成图片下载')

    # 写入图片
    bg_image_file = BytesIO(bg_image)
    fullbg_image_file = BytesIO(fullbg_image)

    # 合成图片
    bg_Image = mergy_Image(bg_image_file, bg_location_list)
    fullbg_Image = mergy_Image(fullbg_image_file, fullbg_location_list)
    # bg_Image.show()
    # fullbg_Image.show()

    # 计算缺口偏移距离
    distance = get_distance(bg_Image, fullbg_Image)
    print('得到距离：%s' % str(distance))

    start_drag(driver, distance)




if __name__ == '__main__':

    # 获取滑块按钮
    driver.get(url)
    slider = WAIT.until(EC.element_to_be_clickable(
        (By.CSS_SELECTOR, "#gc-box > div > div.gt_slider > div.gt_slider_knob.gt_show")))

    recognize_code(driver)


    # driver.close()

