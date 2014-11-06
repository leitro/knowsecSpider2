#!/usr/bin/env python3
'''
    Version: 0.4
    Author:  littlethunder
    Mail:    kingthunder2004@aliyun.com
'''
import urllib.request
import urllib.parse
import time
import sys
import getopt
import threading
import os
import hashlib
import queue
import sqlite3
import functools
import curses
import logging
import lxml.html


class SameFileError(Exception): pass
class NoneTypeError(Exception): pass

#初始化数据库，如果不存在该文件则创建并新增一个表格
def _initDB(dbFile):
    exist = False
    ls = os.listdir('.')
    if dbFile in ls:
        exist = True
    db = sqlite3.connect(dbFile, check_same_thread=False)
    c = db.cursor()
    if not exist:
        try:
            c.execute('create table spider(id integer primary key,\
                    url text,key text,content text)')
            db.commit()
        except sqlite3.OperationalError:
            logging.cratical(dbFile+' 创建表格错误')
    return db, c


#向数据库表格中插入链接、关键词、网页全文
def _insert(url, key, content):
    try:
        content = content.decode('gbk')
    except UnicodeDecodeError:
        content = content.decode('utf8', 'ignore')
        logging.debug(url+' 链接是UTF-8编码')
    content = urllib.parse.quote(content)
    try:
        c.execute('insert into spider(url,key,content) values(\
                "'+url+'","'+key+'","'+content+'")')
        db.commit()
    except sqlite3.OperationalError:
        logging.critical('插入 '+url+' 数据错误')


#请求一个链接，返回HTTP类型、主机Host名、页面的二进制数据
def _requestData(url):
    headers = {
        'Connection': 'keep-alive',
        'Accept': 'text/html,application/xhtml+xml,\
              application/xml;q=0.9,image/webp,*/*;q=0.8',
        'User-Agent': 'Mozilla/5.0 (X11; Linux i686)\
              AppleWebKit/537.36 (KHTML, like Gecko)\
              Chrome/35.0.1916.153 Safari/537.36',
        'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6',
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        res = urllib.request.urlopen(req, timeout=5).read()
    except:
        logging.error('打开'+url+'链接失败')
        return req.type, req.host, None
    return req.type, req.host, res


#处理文本内容不一样但名字一样的情况
def _dealSameFileName(name):
    try:
        files=os.listdir('.')
    except:
        logging.error('无法读取本目录下的文件')
        exit()
    count=1
    while True:
        if name in files:
            name='.'.join([name,str(count)])
            count+=1
        else:
            return name

#显示进度信息
class showProgress(threading.Thread):
    def __init__(self, QLinks, deep, event):
        threading.Thread.__init__(self)
        self.QLinks = QLinks
        self.deep = deep
        self.event = event
        self.start()

    def run(self):
        if deep == 0:
            print('level 1 :', 1, '/', 1)
            return
        screen = curses.initscr()  # 初始化终端界面输出窗口
        maxFile = [0] * (self.deep+1)
        while True:
            links = list(self.QLinks.__dict__['queue'])
            #队列中每个URL此时的深度值
            deeps = [x[1] for x in links]
            '''keys中元素是[deep值,次数]，
            deep=0为最里子层，deep=n-1为父层'''
            keys = [[x, 0] for x in range(self.deep+1)]
            n = len(keys)
            for d in deeps:
                keys[d][1] += 1
            screen.clear()  # 清屏，等待输出
            count = 0
            for d in range(1, n+1):
                count += 1
                if keys[n-d][1] > maxFile[d-1]:
                    maxFile[d-1] = keys[n-d][1]
                screen.addstr(count, 0, 'level ' + str(d) + ' : ' +
                              str(keys[n-d][1])+' / '+str(maxFile[d-1]))
            screen.refresh()  # 使生效
            time.sleep(0.2)
            total = functools.reduce(lambda x, y: x + y,
                                     [i[1] for i in keys])
            totalMax = functools.reduce(lambda x, y: x + y, maxFile)
            if self.event.is_set():
                curses.endwin()
                logging.info('Done at '+time.ctime())
                break


class spider(threading.Thread):
    def __init__(self, QLinks, key, rlock):
        threading.Thread.__init__(self)
        self.queue = QLinks
        self.keyList = key
        self.rlock = rlock
        self.link = None
        self.deep = None
        self.key = None
        self.setDaemon(True)  # 父线程结束后，子线程也相应结束
        self.start()

    def run(self):
        while True:
            try:
                self.link, self.deep = self.queue.get(timeout=2)
                self.key = self.keyList[0]
            except queue.Empty:
                continue
            if self.deep > 0:
                self.deep -= 1
                links = self.getLinks()
                if links:
                    for i in links:
                        global urls
                        if i not in urls:
                            urls.add(i)
                            self.queue.put((i, self.deep))
                self.queue.put((self.link, 0))
            else:
                if not self.key:
                    self.download2File()
                else:
                    self.download2DB()
                logging.info(self.link+'  ['+str(self.deep)+']')
            self.queue.task_done()

    #没有设定关键词的时候，下载到本地目录下
    def download2File(self):
        name = urllib.parse.quote(self.link)
        name = name.replace('/', '_')
        name = _dealSameFileName(name)
        try:
            data = _requestData(self.link)[2]
            md5 = hashlib.md5(data).hexdigest()
            global fileMD5
            if md5 in fileMD5:
                raise SameFileError
            else:
                fileMD5.add(md5)
                with open(name, 'wb') as f:
                    f.write(data)
        except SameFileError:
            logging.info(self.link+' 出现相同的内容，已丢弃')

    #设定关键词的时候，把查询到关键词的网页保存到数据库中
    def download2DB(self):
        data = _requestData(self.link)[2]
        if not data:
            return
        try:
            html = data.decode('gbk')
        except UnicodeDecodeError:
            html = data.decode('utf8', 'ignore')
        if key in html:  # 在页面中找到关键字则放到数据库中
            self.rlock.acquire()
            _insert(self.link, self.key, data)
            self.rlock.release()

    #找出一个URL页面中所有不重复的且正确的子URL
    def getLinks(self):
        try:
            resType, resHost, resData = _requestData(self.link)
            if not resData:
                raise NoneTypeError
        except NoneTypeError:
            return None
        try:
            data = resData.decode('gbk')
        except UnicodeDecodeError:
            data = resData.decode('utf8', 'ignore')
        host = resType+'://'+resHost
        doc = lxml.html.document_fromstring(data)
        tags = ['a', 'iframe', 'frame']
        doc.make_links_absolute(host)
        links = doc.iterlinks()
        trueLinks = []
        for l in links:
            if l[0].tag in tags:
                trueLinks.append(l[2])
        return trueLinks  # 要确保是绝对路径


class threadPool:
    def __init__(self, num, event):
        self.num = num
        self.event = event
        self.threads = []
        self.queue = queue.Queue()
        self.key = [None]
        self.createThread()

    def createThread(self):
        for i in range(self.num):
            self.threads.append(spider(self.queue, self.key, rlock))

    def putJob(self, job, key=None):  # job是(link,deep)的一个tuple
        self.queue.put(job)
        self.key[0] = key

    def getQueue(self):
        return self.queue

    def wait(self):
        self.queue.join()
        self.event.set()  # 通知显示模块程序结束，关闭进度显示


#主控制程序
def mainHandler(threadNum, link, deep, key, test):
    event = threading.Event()
    event.clear()
    pool = threadPool(threadNum, event)
    showProgress(pool.getQueue(), deep, event)
    pool.putJob((link, deep), key)
    pool.wait()
    if test:  # 需要自测模块运行
        import test
        test.test(key, dbFile)


#用法说明
def _usage():
    print('''spider v0.4 --littlethunder
用法：python3 spider.py -u [URL] -d [Deep] -f [Log File]\
 -l [Level] --thread [Thread Number] --dbfile \
[Database File Name] --key [Key Word]

-h  帮助
-u  [URL] （必选参数）指定爬虫开始地址，必须以http或https开头
-d  [Deep] 指定爬虫深度，默认为1，即为当前页
-f  [Log File] 日志记录文件，默认本地目录下spider.log
-l  [Level] 日志记录详细程度，数字越大越详细，1-5取值，默认1
--thread  [Thread Number] 指定线程池大小，默认10
--dbfile  [Database File Name] 指定存放数据结果的数据库，\
默认spider.db，如果没有指定--key参数则失效
--key  [Key Word] 页面内的关键词，如果指定该项则会把数据\
保存在数据库中，否则默认下载网页到本地目录
testself  程序自测，可选参数''')

if __name__ == '__main__':
    rlock = threading.RLock()
    url = None  # 默认参数开始
    deep = '1'
    logFile = 'spider.log'
    level = '4'
    threadNum = '10'
    dbFile = 'spider.db'
    key = None  # 默认参数结束

    optlist, args = getopt.getopt(
        sys.argv[1:],
        'u:d:f:l:h',
        ['thread=', 'dbfile=', 'key='])
    for k, v in optlist:
        if k == '-u':
            url = v
        elif k == '-d':
            deep = v
        elif k == '-f':
            logFile = v
        elif k == '-l':
            level = v
        elif k == '--thread':
            threadNum = v
        elif k == '--dbfile':
            dbFile = v
        elif k == '--key':
            key = v
        elif k == '-h':
            _usage()
            exit()

    deep = int(deep)
    level = int(level)
    threadNum = int(threadNum)
    if level < 1 or level > 5 or deep < 1 or threadNum < 1 or not url:
        _usage()
        exit()
    if key:
        db, c = _initDB(dbFile)
    logLevel = {  # 日志级别，1最不详细，5最详细
        1: logging.CRITICAL,
        2: logging.ERROR,
        3: logging.WARNING,
        4: logging.INFO,
        5: logging.DEBUG,
    }
    logging.basicConfig(filename=logFile, level=logLevel[level])
    deep -= 1
    urls = set()  # 防止抓取重复URL
    fileMD5 = set()  # 保存文件的MD5值防止url不同但内容相同的文件重复下载
    mainHandler(threadNum, url, deep, key, 'test`self' in args)
