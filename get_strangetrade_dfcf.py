#-*- coding:utf-8 -*-
import BeautifulSoup as bfs
import re,os
import pandas as pd
import datetime
from tornado import httpclient, gen, ioloop, queues
import pymysql
import time

class AsySpider(object):
    def __init__(self, urls, concurrency,connect_timeout=120, request_timeout=180):
        urls.reverse()
        self.urls = urls
        self.concurrency = concurrency
        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self._q = queues.Queue()
        self._fetching = set()
        self._fetched = set()
        self.header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                    'like Gecko) Chrome/55.0.2883.87 Safari/537.36',
                       'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                       'Accept-Encoding':'gzip, deflate, sdch'}

    def handle_page(self, url, html):
        pass

    @gen.coroutine
    def get_page(self, url):
        try:
            request = httpclient.HTTPRequest(url=url,headers=self.header,\
                connect_timeout=self.connect_timeout, request_timeout=self.request_timeout)
            response = yield httpclient.AsyncHTTPClient().fetch(request=request)
        except Exception as e:
            print('Exception: %s %s' % (e, url))
            raise gen.Return('')
        raise gen.Return(response.body)

    @gen.coroutine
    def _run(self):
        yield gen.sleep(5.0)
        @gen.coroutine
        def fetch_url():
            current_url = yield self._q.get()
            try:
                if current_url in self._fetching:
                    return
                self._fetching.add(current_url)
                html = yield self.get_page(current_url)
                self._fetched.add(current_url)
                self.handle_page(current_url, html)
                for i in range(self.concurrency):
                    if self.urls:
                        yield self._q.put(self.urls.pop())
            finally:
                self._q.task_done()

        @gen.coroutine
        def worker():
            while True:
                yield fetch_url()
        self._q.put(self.urls.pop())

        for _ in xrange(self.concurrency):
            worker()
        yield self._q.join(timeout=datetime.timedelta(seconds=300000))
        assert self._fetching == self._fetched

    def run(self):
        io_loop = ioloop.IOLoop.current()
        io_loop.run_sync(self._run)

############################################################################
class LongHu(AsySpider):
    '''
    获取东方财富网上的股票龙虎榜明细数据
    '''
    def __init__(self,urls,concurrency,connect_timeout,request_timeout,only_code_date = False):
        AsySpider.__init__(self,urls,concurrency,connect_timeout,request_timeout)
        assert(isinstance(urls,list))
        self.only_code_date = only_code_date
        self.data = []
        self.regex = re.compile('[0-9]')
        self.regex_a = re.compile('span|option')
        self.regex_b = re.compile('/|,|\.')
        self.regex_c = re.compile(u';|类型：|：')
        self.code = ''
        self.date = None

    def handle_page(self,url,html):
        tmp = re.split(self.regex_b,url)
        self.code = tmp[-2]
        if self.only_code_date:
            self.parser_lhb_date(html)
        else:
            td = tmp[-3].replace('-','')
            try:
                self.date = int(td)
            except:
                return
            print self.code,self.date
            self.parser_lhb_detail(html)

    def parser_lhb_detail(self,html):
        try:
            html = html.decode('gb2312').encode('utf-8')
            soup = bfs.BeautifulSoup(html,fromEncoding = 'utf-8')
            lhb_type = soup.findAll('div', {'class':'left con-br'})
            lhb_table = soup.findAll('tbody')
            len_lhb_type = len(lhb_type)
            if len_lhb_type == 0:
                return
            if lhb_table[0].string == '\n' or lhb_table[0].string is None:
                del(lhb_table[0])
            for i in xrange(len_lhb_type):
                plhb_type = self.regex_c.split(lhb_type[i].string)[-1]
                self.data += self.lhb_dt(2*i,lhb_table[2*i],plhb_type)
                self.data += self.lhb_dt(2*i+1,lhb_table[2*i+1],plhb_type)
        except Exception as e:
            print e.message

    def parser_lhb_date(self, html):
        try:
            html = html.decode('gb2312').encode('utf-8')
            soup = bfs.BeautifulSoup(html, fromEncoding='utf-8')
            content = soup.findAll('ul', {'class': 'date-list'})
            if len(content) == 0:
                return
            content = bfs.BeautifulSoup(re.sub(self.regex_a, 'a', str(content[0])))
            span = content.findAll('a')
            n = len(span)
            if n == 0:
                return
            self.data += self.lhb_cd(n, span)
        except Exception as e:
            print e.message

    def lhb_cd(self,n, span):
        '''
        获取东财个股龙虎榜出现的时间和日期
        '''
        tmp = []
        for i in xrange(n):
            sp = self.regex.findall(span[i].string)
            if len(sp) == 0:
                continue
            tmp.append(('%s%s%s%s-%s%s-%s%s' % tuple(sp), self.code))
        return tmp

    def lhb_dt(self,idx, tbody, plhb_type):
        '''
        获取东财个股龙虎榜
        '''
        tmp = tbody.findAll('tr')
        res = []
        trade_type = 1 if idx % 2 == 0 else -1
        n = len(tmp)
        for i in xrange(n):
            t1 = tmp[i]
            td = t1.findAll('td')
            if td[0].string is None:
                break
            ord = int(td[0].string)
            where_name = td[1].findAll('a', href=re.compile('.html'))[0].string
            if where_name.find(u'机构') != -1:
                where_name = u'机构专用'

            if len(self.regex.findall(td[2].string)) != 0:
                buy_amount = float(td[2].string)
                buy_ratio = float(td[3].string.split('%')[0])
            else:
                buy_amount = 0
                buy_ratio = 0
            if len(self.regex.findall(td[4].string)) != 0:
                sell_amount = float(td[4].string)
                sell_ratio = float(td[5].string.split('%')[0])
            else:
                sell_amount = 0
                sell_ratio = 0
            net_buy_amount = float(td[6].string)
            ukey = int('%d%.6s' % (10, self.code)) if self.code.startswith('6') else int('%d%.6s' % (11, self.code))
            res.append((self.date, ukey, plhb_type, trade_type, ord, where_name, \
                        buy_amount, buy_ratio, sell_amount, sell_ratio, net_buy_amount))
        return res

    def get_data(self,dtype = 'cd'):
        '''
        dtype: 'cd',返回包含code和date的DataFrame
               'lhb',返回龙虎榜数据的DataFrame
        '''
        if len(self.data) == 0:
            return None
        if dtype == 'cd':#code and date
            columns = ['DataDate','ukey']
            return pd.DataFrame(self.data,columns = columns)
        elif dtype == 'dt':#lhb detail
            columns = ['DataDate','ukey','lhb_type','trade_type','order','where_name',\
            'buy_amount','buy_ratio','sell_amount','sell_ratio','net_buy_amount']
            return pd.DataFrame(self.data,columns = columns)
        else:
            return None

def main(save_file,code):
    '''
    url = ['http://data.eastmoney.com/stock/lhb,2016-01-26,002624.html']

    url = ['http://data.eastmoney.com/stock/lhb/600158.html',\
            'http://data.eastmoney.com/stock/lhb/600283.html']
    '''
    today = datetime.datetime.now().date()
    lhb_data = pd.DataFrame()
    if os.path.exists(save_file):
        lhb_data = pd.read_csv(save_file)
        start_date = lhb_data['DataDate'].max()
        start_date = pd.to_datetime(start_date,format='%Y%m%d').date()
        start_date = start_date + datetime.timedelta(days=1)
        while start_date <= today:
            date_str = start_date.strftime('%Y-%m-%d')
            print date_str
            urls = map(lambda x:'http://data.eastmoney.com/stock/lhb,%s,%s.html' % (date_str,x), code)
            lhb_dt = LongHu(urls, 20, 120, 180, only_code_date = False)
            lhb_dt.run()
            tmp = lhb_dt.get_data(dtype='dt')
            if tmp is None:
                start_date = start_date + datetime.timedelta(days=1)
                continue
            with open(save_file,'a') as f:
                tmp['lhb_type'] = tmp.lhb_type.str.encode('utf8')
                tmp['where_name'] = tmp.where_name.str.encode('utf8')
                #tmp.to_csv(f,header = None,index = None,encoding = 'gbk')
                tmp.to_csv(f,header = None,index = None)
            #lhb_data = lhb_data.append(tmp)
            start_date = start_date + datetime.timedelta(days=1)
    else:
        urls = map(lambda x:'http://data.eastmoney.com/stock/lhb/%s.html'%x,code)
        n = len(urls)
        for i in xrange(n):
            lhb_cd = LongHu([urls[i]], 20, 120, 180, only_code_date = True)
            lhb_cd.run()
            code_dt = lhb_cd.get_data(dtype='cd')
            if code_dt is None:
                continue
            code_dt = code_dt[code_dt['DataDate']>='2013-01-01']
            if len(code_dt) == 0:
                print urls[i]
                continue
            urls2 = map(lambda x: 'http://data.eastmoney.com/stock/lhb,%s,%s.html' % (x[0], x[1]),
                        code_dt.values.tolist())
            lhb_dt = LongHu(urls2, 200, 120, 180, only_code_date = False)
            lhb_dt.run()
            lhb_data = lhb_data.append(lhb_dt.get_data(dtype='dt'))
        #lhb_data.to_csv(save_file,index = None,encoding = 'gbk')
        lhb_data['lhb_type'] = lhb_data.lhb_type.str.encode('utf8')
        lhb_data['where_name'] = lhb_data.where_name.str.encode('utf8')
        lhb_data.to_csv(save_file,index = None)
if __name__ == '__main__':

    save_file = '/auto/chinaqr/data/lhb/lhb.csv'
    today = datetime.datetime.now()
    db = pymysql.connect('********', '********', '********', 'wind', charset="utf8")
    code = pd.read_sql('''SELECT A.`S_INFO_CODE` FROM wind.ASHAREDESCRIPTION A;''',con=db)
    db.close()
    code['is_stock'] = map(lambda x:1 if x[0] in '603' else 0,code.iloc[:,0])
    code = code[code['is_stock']==1]['S_INFO_CODE'].values.tolist()
    main(save_file=save_file,code = code)

