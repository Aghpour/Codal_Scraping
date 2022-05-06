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
# put tickers here
symbols = ['شستا', 'فولاد', 'فملی', 'وتجارت', 'کگهر']
pages_urls = []
files_count_init = []
files_count_final = []
# create pages urls
for symbol in symbols:
    # create tickers directory
    os.chdir(main_dir)
    if path.exists(f'{symbol}') == False:
        os.mkdir(f'{symbol}')
    else:
        pass
    # change to ticker directory
    try:
        ticker_dir = f'{main_dir}\{symbol}'
        os.chdir(ticker_dir)
        dir_len0 = len(os.listdir(ticker_dir)) # get files count
        files_count_init.append(dir_len0)
    except:
        print(f'there is no {ticker_dir} directory')
    # making urls (putting symbols)
    symbol_converted = urllib.parse.quote(symbol.encode('utf8'))
    url = f'https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=3&Childs=false&CompanyState=0&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Isic=571919&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber=1&Publisher=false&Symbol={symbol_converted}&TracingNo=-1&search=true'
    with urlopen(url) as response:
        body = response.read()
    items = json.loads(body)
    # get total pages and links
    page_count = items['Page']
    total_files = items['Total']
    ticker = items['Letters'][0]['Symbol']
    print(f'{symbol}, total pages: {page_count}, total files: {total_files}')
    # making urls (putting page numbers)
    for page in range(page_count):
        url = f'https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category=3&Childs=false&CompanyState=0&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Isic=571919&Length=-1&LetterType=-1&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber={page+1}&Publisher=false&Symbol={symbol}&TracingNo=-1&search=true'
        pages_urls.append(url)
###############################################################################
async def fetch(session, url):
    async with session.get(url) as response:
        return await response.read()
    
async def main():
    # create tasks
    tasks = []
    async with aiohttp.ClientSession() as session:
        for page_url in pages_urls:
            tasks.append(fetch(session, page_url))
        pages = await asyncio.gather(*tasks)
        
        count = 0
        broken_links = []
        # load pages
        for page in pages:
            itemss = json.loads(page)
            ticker = itemss['Letters'][0]['Symbol']
            #tickers.append(ticker)
            # find links in a page
            for i in range(len(itemss['Letters'])):
                excel_link = itemss['Letters'][i]['ExcelUrl']
                title = itemss['Letters'][i]['Title'].replace('/', '_')
                ticker = itemss['Letters'][i]['Symbol']
                publish_data = itemss['Letters'][i]['PublishDateTime'].replace('/', '_').replace(':', '_')
                socket.setdefaulttimeout(5)
                os.chdir(main_dir)
                # change to ticker directory
                try:
                    ticker_dir = f'{main_dir}\{ticker}'
                    os.chdir(ticker_dir)
                except:
                    print(f'there is no {ticker_dir} directory')    
                # download excel file     
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

        # get files count
        for ticker in symbols:
            ticker_dir = f'{main_dir}\{ticker}'
            dir_len1 = len(os.listdir(ticker_dir))
            files_count_final.append(dir_len1)
        # number of new downloaded files 
        for i, ticker in zip(range(len(symbols)), symbols):
            new_file = files_count_final[i] - files_count_init[i]
            print(f'{ticker} new downloaded files: {new_file}')
        
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
