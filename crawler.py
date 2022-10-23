# Standard libraries
from time import sleep, time, perf_counter_ns
from urllib.parse import urljoin, urlparse
from pathlib import PurePosixPath as Path
from collections import deque
from queue import Queue
from multiprocessing import Process, Queue as ProcessQueue
from threading import Thread, active_count
import logging

# 3rd party libraries
from bs4 import BeautifulSoup as BS
from pymongo import MongoClient
from bs4 import Tag
from tldextract import extract as TLDExtract
import requests

# Aplication libraries

# ----------------------------------------------------------------------------
# Functionalities:      
#
# 1 - Have the necessary structures and mechanisms for the crawler evolution
#     - Error tolerant - Maintain operation despite errors (urllib, requests
#       and BS errors)
#     - Scalable - Multithreads / Processes
# 2 v Save all diferente .pt domains
#     v Avoid other root domains or
#     v Acept pt.domain.com
# 3 v Save all errors found
# 4 v Use euristics to classify websites as e-commerce
# 5 v Save one html file for each domain to distinguish e-commerce from not
# 6 - Use as much bloking avoidance techniques:
#     - Dynamically adjust the request rate to maximium allowed by the server
#       - HTTP Error "429 - Too Many Requests" with the bellow header
#       - HTTP Header "Retry-After"
#       - HTTP Header "x-ratelimit-limit" - Number of requests in 60 seconds
#     v Use as much autentic headers as possible
#     - Crawl based on low trafic hours morning (2-8am of users/server) to
#       avoid slow website
#     - Crawl based on robots.txt
#     
# Issues to correct:
#    - Find ways to Speed up the crawl (Ideas)
#      - Use async acess to database - speed up thread creation
#      - Diferent architecture to separate asyncronouse operations (requests)
#        and processing
#        - Max threads possible request and whait for response to saturate bandwith
#        - Other treads to process, access memory and disk
#      - Use multiple processes instead of threads to use all CPU cores
#-----------------------------------------------------------------------------

# Constants
TRIES = 5
MAX_THREADS = 103
HEADERS = {'Accept':'text/html,application/xhtml+xml,application/xml;'
           'q=0.9,image/avif,image/webp,image/apng,*/*;'
           'q=0.8,application/signed-exchange;v=b3;q=0.9',
           'Accept-Encoding':'gzip, deflate',
           'Accept-Language':'pt-PT,pt;q=0.9,en-US;q=0.8,en;q=0.7',
           'Device-Memory':'8',
           'Downlink':'100',
           'Sec-Ch-Ua':'"Chromium";v="104", " Not A;Brand";v="99"'
           '"Google Chrome";v="104"',
           'Sec-Ch-Ua-Arch':'"x86"',
           'Sec-Ch-Ua-Full-Version':'"104.0.5112.81"',
           'Sec-Ch-Ua-Platform':'"Windows"',
           'Sec-Ch-Ua-Platform-Version':'"10.0.0"',
           'Connection':'keep-alive',
           'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
           'AppleWebKit/537.36 (KHTML, like Gecko)'
           'Chrome/104.0.0.0 Safari/537.36',
           'Viewport-Width':'853'}
EURISTICS = ['add to cart','add to bag','add to basket','adicionar à cesta',
             'adicionar ao cesto','adicionar ao carrinho','adicionar',
             'juntar ao carrinho','carrinho','comprar']
SEEDS = ['https://melhores-sites.pt/melhores-sites-portugal.html',
         'https://pt.trustpilot.com/categories',
         'https://portal-sites.net/']
#MONGOKEY_ATLAS = ('mongodb+srv://LuisFigueira:'
#            'Telecaster13@cluster0.rnnt0.mongodb.net/'
#           'MAXUT?retryWrites=true&w=majority')
MONGOKEY = 'mongodb://localhost:27017/'
ALLOWED_FILES = ['.html',
                 '']
ALLOWED_DOMAINS = ['pt',
                   'net.pt',
                   'gov.pt',
                   'org.pt',
                   'edu.pt',
                   'int.pt',
                   'publ.pt',
                   'com.pt',
                   'nome.pt',]

# Variables
visited_urls = set()    # 
visited_sites = set()  # Query se existe antes de inserir (100xmais rapido)

# Deques
follow_deque = deque()
responses_queue = ProcessQueue()
getLinks_queue = ProcessQueue()
headers_queue = ProcessQueue()
classify_queue = ProcessQueue()
errors_queue = ProcessQueue()

# Services initializations
session = requests.Session()
session.headers.update(HEADERS)
adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_THREADS,
                                        pool_maxsize=MAX_THREADS)
session.mount('http://',adapter)
session.mount('https://',adapter)

# Será que é preciso? Ou apenas no final??
client = MongoClient(MONGOKEY)
db = client.MAXUT

# Functions
def info():
    while True:
        sleep(1)
        stop = time()
        print((f'{len(visited_urls)} webpages visited - '
              f'{active_count()} active threads - '
              f'{follow_deque.qsize()} urls to be followed - '
               f'{int(len(visited)//(stop-start))} crawled pages per second'))
        

def threadLauncher(TRIES, session, follow_deque, responses_queue, errors_queue):
    while True:
        if not follow_deque.empty() and threading.active_count < MAX_THREADS:
            url = follow_deque.popleft()
            threading.Thread(target=requestThread,args=(TRIES, session, url, responses_queue, error_queue)).start()
            print(f'THREAD do {url} lançada')
            

def responsesThread(TRIES, session, url, responses_queue, errors_queue):
    for i in range(TRIES):
        try:
            response = session.get(url,allow_redirects=True)
            break
        except Exception as e:
            print(f'erro: {e}')
            if i < TRIES-1:
                error = {}
                error['error'] = str(e)
                error['url'] = url
                errors_queue.put(error)
                sleep(3)
                continue
            else:
                return
    print(f'resposta do {url} recebida')
    responses_queue.put(response)
    return None


def getLinks(getLinks_queue, follow_deque, visited_urls, visited_sites):
    while True:
        if not getLinks_queue.empty():
            response = getLinks_queue.get()
            url = response.url
            visited_urls.add(url)
            domain = TLDExtract(url).fqdn
            if domain not in visited_sites:
                visited_sites.add(domain)
            if response.status_code >= 400:
                continue
            soup = BS(response.text,'html.parser')
            body = soup.body
            if body is None:
                continue
            tags = body(href=True)
            if tags is not None:
                links = [ urljoin(url,tag['href']) if urlparse(tag['href']).netloc == ''
                          # If url doesn't have netloc assume is relative path so join to url
                          else  tag['href']
                          # if url has url treat it as a url
                          for tag in tags if
                              # if HAS netloc AND it's sufix is allowed AND is a path to an allowed file type
                          ( ( ( urlparse(tag['href']).netloc != ''
                                and Path(urlparse(tag['href']).path).suffix in ALLOWED_FILES
                                and ( TLDExtract(tag['href']).suffix in ALLOWED_DOMAINS
                                    or TLDExtract(tag['href']).subdomain in ALLOWED_DOMAINS ) )
                              or
                              # If NOT have netloc (is relative path)
                              # AND is an allowed file
                              # AND this site is in the allowed domains
                              (urlparse(tag['href']).netloc == ''
                               and Path(urlparse(tag['href']).path).suffix in ALLOWED_FILES
                               )
                              or TLDExtract(tag['href']).subdomain == 'portal-sites')
                               # Commented seems avoid impossible situation (that the page has not allowed domain)
                               #and ( TLDExtract(url).suffix in ALLOWED_DOMAINS
                               #     or TLDExtract(url).subdomain in ALLOWED_DOMAINS) ) )
                            and
                            # If it wasn't followed or is not scheduled for following
                            ( tag['href'] not in visited_urls
                             or
                              urljoin(url,tag['href']) not in visited ) ) ]
                follow_deque.extend(links)
            

def errorStorage(queue):
    client = MongoClient(MONGOKEY)
    db = client.MAXUT
    errors = db['errors']
    while True:
        try: 
            if not queue.empty():
                error = {}
                obj = queue.get()
                error['error'] = obj['error']
                error['url'] = obj['url']
                matches = queue.count_documents(error)
                if matches == 0:
                    queue.insert_one(error)
        except Exception as e:
            queue.put(e)
            continue
        
        
def headersStorage(queue, errors_queue):
    client = MongoClient(MONGOKEY)
    db = client.MAXUT
    headers_DB = db['headers']
    while True:
        try:
            if not queue.empty():
                headers = queue.get()
                for header in headers:
                    if header not in ['Date','Expires']:
                        obj = {}
                        obj['header'] = header
                        obj['value'] = headers[header]
                        matches = headers_DB.count_documents(obj)
                        if matches == 0:
                            headers_DB.insert_one(obj)
        except Exception as e:
            errors_queue.put(e)
            continue


def taggify(soup):
    for tag in soup:
        if isinstance(tag,Tag):
            yield '<{}>{}</{}>'.format(tag.name,
                                        ''.join(taggify(tag)),tag.name)


def classify(queue, errors_queue):
    '''Find different html structures, classify them as e-commerce or not and store on Database'''
    client = MongoClient(MONGOKEY)
    db = client.MAXUT
    ML_objects = db['e-com']
    while True:
        if not queue.empty():
            response = queue.get()
            url = response.url
            soup = BS(response.text,'html.parser')
            body = soup.body
            if body is None:
                continue
            structure = ''.join(taggify(soup))
            classification = False
            for string in soup.stripped_strings:
                if string in EURISTICS:
                    classification = False
            # Store html structure, url and classification
            url_infos = {}
            url_infos['structure'] = structure
            url_infos['url'] = url
            url_infos['classification'] = classification
            try:
                matches = ML_objects.count_documents({'structure':url_infos['structure']})
                if matches == 0:
                    ML_objects.insert_one(url_infos)
            except Exception as e:
                errors_queue.put(e)


def finalizer(follow_deque, responses_queue, visited_urls, visited_sites):
    '''Transfers visited urls and sites sets to the database after checking for empty deques
        and queues, I.E. end of program'''
    flag = False
    while True:
        sleep(10)
        if len(follow_deque) == 0 and responses_queue.qsize() == 0:
            if flag == False:
                flag = True
                continue
            else:
                # DEVELOP:
                client = MongoClient(MONGOKEY)
                db = client.MAXUT
                sites = db['sites']
                urls = db['urls']
                sites
                # Save sets in the database
                # Turn processes off
                # Print Crawl Info
                # Exit program
        elif flag == True:
            flag = False

# Algorithm
start = time()
try:
    if __name__ == '__main__':
        p = Process(target=errorStorage,args=(errors_queue,))
        p.start()
        p.join()
        p = Process(target=headersStorage,args=(queue, errors_queue,))
        p.start()
        p.join()
        Process(target=classify,args=(queue, errors_queue,))
        p.start()
        p.join()
        Process(target=getLinks,args=(getLinks_queue, follow_deque, visited_urls, visited_sites,))
        p.start()
        p.join()
        Process(target=responsesThread,args=(TRIES, session, url, responses_queue, errors_queue,))
        p.start()
        p.join()
        Process(target=threadLauncher, args=(TRIES, session, follow_deque, responses_queue, errors_queue,))
        p.start()
        p.join()
except Exception as e:
    errors_queue.put(e)
follow_deque.extend(SEEDS)
finalizer(follow_deque, responses_queue, visited_urls, visited_sites)