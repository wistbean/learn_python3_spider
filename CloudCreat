# coding: utf-8

from wordcloud import WordCloud
import cv2
import jieba
import matplotlib.pyplot as plt

with open('test.txt', 'r') as f:
    text = f.read()

cut_text = " ".join(jieba.cut(text))

color_mask = cv2.imread('back.jpeg')

cloud = WordCloud(
    # 设置字体，不指定就会出现乱码
    font_path="/Users/caichenyang/Desktop/爬虫尝试/Baoli.ttc",
    # font_path=path.join(d,'simsun.ttc'),
    # 设置背景色
    background_color='white',
    # 词云形状
    mask=color_mask,
    # 允许最大词汇
    max_words=120,
    # 最大号字体
    max_font_size=2000
)

wCloud = cloud.generate(cut_text)
wCloud.to_file('cloud.jpg')
plt.imshow(wCloud, interpolation='bilinear')
plt.axis('off')
plt.show()

