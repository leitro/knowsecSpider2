#!/usr/bin/env python3
'''
    Version: 0.3
    Author:  gitsocket
    Mail:    kingthunder2004@aliyun.com
    Phone:   18756091592
'''
import threading
import sqlite3
import hashlib
import os
from progressbar import *


#生成数据库中保存的所有HTML页面的MD5值列表
class testSameDB(threading.Thread):
    def __init__(self, cursor, md5, progress):
        threading.Thread.__init__(self)
        self.c = cursor
        self.count = 0
        self.md5 = md5  # list的append是线程安全的，可以这么使用
        self.progress = progress
        self.start()

    def run(self):
        while True:
            self.c.execute('select content from spider limit %s,%s'
                           % (self.count, self.count + 10000))
            self.count += 10000
            contents = self.c.fetchall()
            if len(contents) == 0:
                break
            for c in contents:
                res = hashlib.md5(c[0].encode('utf8'))
                self.md5.append(res.hexdigest())
                self.progress[0] = self.count


#生成本地目录中保存的所有HTML页面的MD5值列表
class testSameFile(threading.Thread):
    def __init__(self, files, md5, progress):
        threading.Thread.__init__(self)
        self.count = 0
        self.files = files
        self.md5 = md5  # list的append是线程安全的，可以这么使用
        self.progress = progress
        self.start()

    def run(self):
        while len(self.files):
            fileName = self.files.pop()
            with open(fileName, 'rb') as f:
                data = f.read()
                res = hashlib.md5(data)
                self.md5.append(res.hexdigest())
                self.progress[0] += 1


#自测主程序，测试爬取的HTML页面有无重复
def test(key, dbFile):
    md5 = []
    progress = [0]
    if key:  # 指定了关键词，要在数据库中自测
        db = sqlite3.connect(dbFile, check_same_thread=False)
        c = db.cursor()
        c.execute('select count(*) from spider')
        totalNum = c.fetchall()[0][0]
        t = testSameDB(c, md5, progress)
        t.join()
    else:  # 没有指定关键词，在本地目录下自测
        files = os.listdir('.')
        files.remove(dbFile)
        totalNum = len(files)
        threads = [testSameFile(files, md5, progress)
                   for i in range(100)]
        for t in threads:
            t.join()
    pBar = ProgressBar(widgets=[Percentage(), Bar()],
                       maxval=totalNum).start()
    while progress[0] < totalNum:
        pBar.update(progress[0] + 1)
    pBar.finish()
    if len(md5) == totalNum:
        print('爬取的HTML页面没有重复，程序按要求“正确”执行。')
    else:
        print('爬取了“重复”的HTML页面，请修改代码！')

if __name__ == '__main__':
    test('科学', 'spider.db')
