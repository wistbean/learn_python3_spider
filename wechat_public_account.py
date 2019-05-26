#-*- coding:UTF-8 -*-
import json
import time
import pdfkit

import requests

base_url = 'https://mp.weixin.qq.com/mp/profile_ext'


# 这些信息不能抄我的，要用你自己的才有效
headers = {
    'Connection': 'keep - alive',
    'Accept': '* / *',
    'User-Agent': '写你自己的',
    'Referer': '写你自己的',
    'Accept-Encoding': 'br, gzip, deflate'
}

cookies = {
    'devicetype': 'iOS12.2',
    'lang': 'zh_CN',
    'pass_ticket': '写你自己的',
    'version': '1700042b',
    'wap_sid2': '写你自己的',
    'wxuin': '3340537333'
}



def get_params(offset):
    params = {
        'action': 'getmsg',
        '__biz': '写你自己的',
        'f': 'json',
        'offset': '{}'.format(offset),
        'count': '10',
        'is_ok': '1',
        'scene': '126',
        'uin': '777',
        'key': '777',
        'pass_ticket': '写你自己的',
        'appmsg_token': '写你自己的',
        'x5': '0',
        'f': 'json',
    }

    return params


def get_list_data(offset):
    res = requests.get(base_url, headers=headers, params=get_params(offset), cookies=cookies)
    data = json.loads(res.text)
    can_msg_continue = data['can_msg_continue']
    next_offset = data['next_offset']

    general_msg_list = data['general_msg_list']
    list_data = json.loads(general_msg_list)['list']

    for data in list_data:
        try:
            if data['app_msg_ext_info']['copyright_stat'] == 11:
                msg_info = data['app_msg_ext_info']
                title = msg_info['title']
                content_url = msg_info['content_url']
                # 自己定义存储路径
                pdfkit.from_url(content_url, '/home/wistbean/wechat_article/'+title+'.pdf')
                print('获取到原创文章：%s ： %s' % (title, content_url))
        except:
            print('不是图文')

    if can_msg_continue == 1:
        time.sleep(1)
        get_list_data(next_offset)


if __name__ == '__main__':
    get_list_data(0)