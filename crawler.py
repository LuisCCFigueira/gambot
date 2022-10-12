# Standard libraries
from urllib.parse import urljoin, urlparse
from pathlib import PurePosixPath as Path
from queue import Queue
#import multiprocessing
import threading
import logging
from time import sleep, time
# 3rd party libraries
from bs4 import BeautifulSoup as BS
from pymongo import MongoClient
from bs4 import Tag
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
#     - Acept pt.domain.com
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
#    - Is visiting duplicate webpages - Not cheking the queue for duplicates
#    - Using a queue must be slowing the crawl for heaving lots of threads acessing it
#    - Use tldextract to allow third level pt. subdomains
#    - Find ways to Speed up the crawl (Ideas)
#      - Use local database
#      - Use async acess to database - speed up thread creation
#      - Diferent architecture to separate asyncronouse operations (requests)
#        and processing
#        - Max threads possible request and whait for response to saturate bandwith
#        - Other treads to process, access memory and disk
#      - Use multiple processes instead of threads to use all CPU cores
#-----------------------------------------------------------------------------

# Constants
TRIES = 5
MAX_THREADS = 100
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
MONGOKEY_ATLAS = ('mongodb+srv://LuisFigueira:'
            'Telecaster13@cluster0.rnnt0.mongodb.net/'
            'MAXUT?retryWrites=true&w=majority')
MONGOKEY = 'mongodb://localhost:27017/'
ALLOWED_DOMAINS = ['.pt']
ALLOWED_FILES = ['.html','']

# Variables
visited = set()

# Objects
errorQueue = Queue()
headersQueue = Queue() 
websitesQueue = Queue()
ecomQueue = Queue()
follow = Queue()

session = requests.Session()
session.headers.update(HEADERS)

adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_THREADS,
                                        pool_maxsize=MAX_THREADS)
session.mount('http://',adapter)
session.mount('https://',adapter)

client = MongoClient(MONGOKEY)
db = client.MAXUT

# Functions
def info():
    while True:
        sleep(5)
        stop = time()
        print((f'{len(visited)} webpages visited - '
              f'{threading.active_count()} active threads - '
              f'{follow.qsize()} urls to be followed - '
               f'{len(visited)//(stop-start)} crawled pages per second'))


def errorStorage(queue):
    errors = db['errors']
    while True:
        try: 
            if not queue.empty():
                error = {}
                obj = queue.get()
                error['error'] = obj['error']
                error['url'] = obj['url']
                matches = errors.count_documents(error)
                if matches == 0:
                    errors.insert_one(error)
        except Exception as e:
            errorQueue.put(e)
            continue


def headersStorage(queue):
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
            errorQueue.put(e)
            continue
    

def websitesStorage(queue):
    websites = db['websites']
    while True:
        try:
            if not queue.empty():
                website = {}
                website['url'] = queue.get()
                matches = websites.count_documents(website)
                if matches == 0:
                    websites.insert_one(website)
        except Exception as e:
            errorQueue.put(e)
            continue 


def ecomStorage(queue):  
    ML_objects = db['e-com']
    while True:
        try:
            if not queue.empty():
                ML_object = queue.get()
                matches = ML_objects.count_documents({'structure':ML_object['structure']})
                if matches == 0:
                    ML_objects.insert_one(ML_object)
        except Exception as e:
            errorQueue.put(e)
            continue 


def threadManager(queue):
    while True:
        try:
            if not queue.empty() and threading.active_count() < MAX_THREADS:
                url = queue.get()
                threading.Thread(target=crawlUrl, args=(url,)).start()
        except Exception as e:
            errorQueue.put(e)
            continue
            

def taggify(soup):
    for tag in soup:
        if isinstance(tag,Tag):
            yield '<{}>{}</{}>'.format(tag.name,
                                        ''.join(taggify(tag)),tag.name)


def getLinks(soup,url):
    '''Returns a list with all valid urls based on the filterrig logic rules or 
       returns an empty list in case no links satisfy rules or no links exist 
     '''
    tags = soup.body(href=True) 
    if tags is not None:
        links = [ urljoin(url,tag['href']) if urlparse(tag['href']).netloc == ''
                  # If url doesn't have netloc assume is relative path so join to url
                  else  tag['href']
                  # if url has url treat it as a url
                  for tag in tags if
                      # if HAS netloc AND it's sufix is allowed AND is a path to an allowed file type
                  ( ( ( urlparse(tag['href']).netloc != ''
                        and Path(urlparse(tag['href']).netloc).suffix in ALLOWED_DOMAINS  
                        and Path(urlparse(tag['href']).path).suffix in ALLOWED_FILES )
                      or
                      # If NOT have netloc (is relative path)
                      # AND is an allowed file
                      # AND this site is in the allowed domains
                      (urlparse(tag['href']).netloc == ''
                       and Path(urlparse(tag['href']).path).suffix in ALLOWED_FILES
                       and Path(urlparse(url).netloc).suffix in ALLOWED_DOMAINS) )
                    and
                    ( (tag['href'] not in visited and tag['href'] not in follow.queue) 
                     or
                      (urljoin(url,tag['href']) not in visited
                       and urljoin(url,tag['href']) not in follow.queue) ) )]
    else:
        return []
    return links


def classify(soup):
    '''Clasify a webpage as product (True) or not (False)'''
    for string in soup.stripped_strings:
        if string in EURISTICS:
            return True
    return False
    

def crawlUrl(url):
    # Get webpage
    for i in range(TRIES):
        try:
            response = session.get(url,allow_redirects=True)
            break
        except Exception as e:
            if i < TRIES:
                error = {}
                error['error'] = str(e)
                error['url'] = url
                errorQueue.put(error)
                sleep(3)
                continue
            else:
                return
    
    # Add webpage to visited
    visited.add(url)
    # Add website netloc to existent domains
    netloc = urlparse(url).netloc
    websitesQueue.put(netloc)
    
    # Store website response headers if different than previous
    headers = dict(response.headers.items())
    headersQueue.put(headers)
    
    # Exit if http error status code
    if response.status_code >= 400:
        return
    
    soup = BS(response.text,'html.parser')
    
    # Get HTML body structure
    structure = ''.join(taggify(soup))
    
    # Classify webpage as e-commerce product page or not
    webpage_classification = classify(soup)
    
    # Store html structure, url and classification
    ML_object = {}
    ML_object['structure'] = structure
    ML_object['url'] = url
    ML_object['classification'] = webpage_classification
    ecomQueue.put(ML_object)
    
    # Get all links that can be followed
    links = getLinks(soup,url)
    
    # Insert links in queue
    for link in links:
        follow.put(link)
    return

# Algorithm
start = time()
try:
    threading.Thread(target=info,args=()).start()
    threading.Thread(target=errorStorage,args=(errorQueue,)).start()
    threading.Thread(target=headersStorage,args=(headersQueue,)).start()
    threading.Thread(target=websitesStorage,args=(websitesQueue,)).start()
    threading.Thread(target=ecomStorage,args=(ecomQueue,)).start()
    threading.Thread(target=threadManager,args=(follow,)).start()
except Exception as e:
    errorQueue.put(e)
for seed in SEEDS:
    follow.put(seed)