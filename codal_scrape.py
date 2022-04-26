import asyncio
import aiohttp
import nest_asyncio
import time
from urllib.request import urlopen
import urllib.parse
import urllib.request
import json
from pathlib import Path
import os.path
from os import path
import socket

###############################################################################
nest_asyncio.apply()
start = time.time()

main_dir = str(Path.home() / "Downloads")
symbols = ['شستا', 'فولاد', 'فملی', 'وتجارت']
pages_urls = []
for symbol in symbols:
    symbol_converted = urllib.parse.quote(symbol.encode('utf8'))
    url = f'https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=3&Childs=false&CompanyState=0&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Isic=571919&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber=1&Publisher=false&Symbol={symbol_converted}&TracingNo=-1&search=true'
    with urlopen(url) as response:
        body = response.read()
    items = json.loads(body)
    page_count = items['Page']
    total_files = items['Total']
    ticker = items['Letters'][0]['Symbol']
    print(f'{symbol}, total pages: {page_count}, total files: {total_files}')
    
    for page in range(page_count):
        url = f'https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=3&Childs=false&CompanyState=0&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Isic=571919&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber={page+1}&Publisher=false&Symbol={symbol}&TracingNo=-1&search=true'
        pages_urls.append(url)
###############################################################################
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.read()
    
async def main():
    
    tasks = []
    async with aiohttp.ClientSession() as session:
        for page_url in pages_urls:
            tasks.append(fetch(session, page_url))
        pages = await asyncio.gather(*tasks)
        
        count = 0
        broken_links = []
        for page in pages:
            itemss = json.loads(page)
            ticker = itemss['Letters'][0]['Symbol']

            for i in range(len(itemss['Letters'])):
                excel_link = itemss['Letters'][i]['ExcelUrl']
                title = itemss['Letters'][i]['Title'].replace('/', '_')
                ticker = itemss['Letters'][i]['Symbol']
                publish_data = itemss['Letters'][i]['PublishDateTime'].replace('/', '_').replace(':', '_')
                socket.setdefaulttimeout(5)
                os.chdir(main_dir)
                # create ticker directory
                if path.exists(f'{ticker}') == False:
                    os.mkdir(f'{ticker}')
                else:
                    pass
                # change to ticker directory
                try:
                    ticker_dir = f'{main_dir}\{ticker}'
                    os.chdir(ticker_dir)
                except:
                    print('there is no {ticker} directory')    
                     
                try:
                    if path.exists(f'{ticker}_{publish_data}_{title}.xls') == False:
                        urllib.request.urlretrieve(excel_link, filename = f'{ticker}_{publish_data}_{title}.xls')
                        count += 1
                        print(f'{count}: {ticker}_{publish_data}_{title}')
                    else:
                        pass
                except:
                    print(f'broken link, skipped {ticker}_{publish_data}_{title}')
                    broken_links.append(f'{ticker}_{publish_data}_{title}')
                    pass

        os.chdir(main_dir)
        print(f'back to project directory: {os.getcwd()}')
        print(f'number of broken links: {len(broken_links)}')
        for broken in broken_links:
            print(f' broken link # {broken_links.index(broken)+1}: {broken}')
            
if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    t = time.perf_counter()
    loop.run_until_complete(main())
    t2 = time.perf_counter() - t
    print(f'total time taken: {t2:0.2f}  seconds')
