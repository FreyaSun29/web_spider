import re
import aiohttp
import asyncio
import pandas as pd

fw = open('guba.txt', 'w')

#爬取帖子名称、链接等数据
#获取网页
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

#解析网页
async def parser(html):
    p1 = r'最后更新</span>.+?<div\sclass="pager".+?#D7E5FF;">'
    p2 = r'<div\sclass=.+?a1">(.+?)<.+?a2">(.+?)<.+?a3"><a\shref="(.+?)".+?title="(.+?)".+?a4"><a\shref="(.+?)".+?<font>(.+?)</font>'
    obj1 = re.compile(p1, re.S)
    obj2 = re.compile(p2, re.S)
    match1 = obj1.search(html)
    match2 = obj2.findall(match1.group())
    for line in match2:
        if line[4].rfind('gray') == -1:
            match3 = re.findall('<font>(.+?)</font>', line[4], re.S)
        else:
            match3 = re.findall('<span.+?>(.+)',line[4],re.S)
        fw.write('{:s}\t{:s}\t{:s}\t{:s}\t{:s}\n'.format(line[0],line[1],line[2],line[3],match3[0]))

# 处理网页
async def download(url):
    async with aiohttp.ClientSession() as session:
        html = await fetch(session, url)
        await parser(html)

urls = ['http://guba.eastmoney.com/list,601633_'+str(i)+'.html' for i in range(1,1800)]

# 利用asyncio模块进行异步IO处理
loop = asyncio.get_event_loop()
tasks = [asyncio.ensure_future(download(url)) for url in urls]
tasks = asyncio.gather(*tasks)
loop.run_until_complete(tasks)

#进入每一个帖子爬取时间
#获取网页
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.text()

#解析网页
async def parser1(html):
    p1 = r'<div\sclass=.+?发表于\s(.+?)\s东方.+?</div>'
    obj1 = re.compile(p1, re.S)
    match1 = obj1.findall(html)
    if len(match1) > 0:
        fw.write('{:s}\n'.format(match1[0]))
    else:
        fw.write('Null\n')

async def parser2(html):
    p2 = r'</a><span>(.+?)年(.+?)月(.+?)日(.+?)</span>'
    obj2 = re.compile(p2, re.S)
    match2 = obj2.findall(html)
    if len(match2) > 0:
        tt = match2[0]
        t = tt[0] + '-' + tt[1] + '-' + tt[2] + tt[3] + ':00'
        fw.write('{:s}\n'.format(t))
    else:
        fw.write('Null\n')

# 处理网页
async def download(lk):
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=64,verify_ssl=False)) as session:
        if lk.rfind('eastmoney') == -1:
            url = 'http://guba.eastmoney.com' + lk
            html = await fetch(session, url)
            await parser1(html)
        else:
            url = 'http:' + lk
            html = await fetch(session, url)
            await parser2(html)

data = pd.read_table('guba.txt', sep='\t',header=None)
data.columns = ['num_read', 'num_comment', 'url_post', 'title', 'url_poster', 'poster']
links = data.iloc[:,2].values

# 利用asyncio模块进行异步IO处理
loop = asyncio.get_event_loop()
tasks = [asyncio.ensure_future(download(lk)) for lk in links]
tasks = asyncio.gather(*tasks)
loop.run_until_complete(tasks)