# Standard libraries
from time import sleep, time, perf_counter_ns
from urllib.parse import urljoin, urlparse
from pathlib import PurePosixPath as Path
from multiprocessing import Value, Process, Queue
from threading import Thread, active_count

# 3rd party libraries
from bs4 import BeautifulSoup as BS
from pymongo import MongoClient
from bs4 import Tag
from tldextract import extract as TLDExtract
import requests
import psutil

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
MAX_THREADS = 500
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
crawling = True

# Queues
responses_queue = Queue()
getLinks_queue = Queue()
classify_queue = Queue()
print_queue = Queue()
follow = Queue()

# Services initializations
session = requests.Session()
session.headers.update(HEADERS)
adapter = requests.adapters.HTTPAdapter(pool_connections=MAX_THREADS,
                                        pool_maxsize=MAX_THREADS)
session.mount('http://',adapter)
session.mount('https://',adapter)

client = MongoClient(MONGOKEY)
db = client.MAXUT
names = db.list_collection_names()
for name in names:
    db.drop_collection(name)


# Functions
def info(visited_urls,follow):
    global crawling
    while crawling == True:
        print(f'[info] - crawling = {crawling}')
        try:
            sleep(5)
            stop = time()
            print((f'\n----- STATS  -----\n{visited_urls.value} Total responses processed - '
                f'{active_count()} active threads - '
                f'{follow.qsize()} urls to be followed - '
                f'{(visited_urls.value)//(stop-start)} responses processed per second\n\n'))
        except Exception as e:
            continue
    else:
        return
       

# Lança a aprox 30 por segundo
def threadLauncher(TRIES, session, follow, responses_queue):
    global crawling
    while crawling == True:
        try:
            if not follow.empty() and active_count() < MAX_THREADS:
                start = perf_counter_ns() 
                url = follow.get()          
                Thread(target=requestThread,args=(session, url, responses_queue)).start()
        except Exception:
            continue
    else:
        return


def requestThread(session, url, responses_queue):
    try:
        response = session.get(url,allow_redirects=True)
    except Exception as e:
        return
    responses_queue.put(response)


# Não atrasa o crawl
def responsesThread(responses_queue, getLinks_queue, classify_queue):
    global crawling
    while crawling == True:
        if not responses_queue.empty():
            response = responses_queue.get()
            getLinks_queue.put(response)
            classify_queue.put(response)
    else:
        return
            
            
# Up to 250+ times faster than loop implementation
def sacaLinks(text,lista):
    parts = text.partition('href')
    if parts[1] == '':
        return lista
    link = parts[2].split('"',2)
    lista.append(link[1])
    if len(link) == 3 and link[2] != '':
        sacaLinks(link[2],lista)
        
    
def getLinks(getLinks_queue, follow, visited_urls_value, getLinksFlag):
    visited_urls = set() 
    visited_sites = set()
    while getLinksFlag.value == 0:
        try:
            if not getLinks_queue.empty():
                response = getLinks_queue.get()
                url = response.url
                visited_urls.add(url)
                visited_urls_value.value = len(visited_urls)
                domain = TLDExtract(url).fqdn
                if domain not in visited_sites:
                    visited_sites.add(domain)
                if response.status_code >= 400:
                    continue
                body = response.text.partition('<body>')[2]
                tags = []
                sacaLinks(body,tags)
                links = [ urljoin(url,tag) if urlparse(tag).netloc == ''
                          # If url doesn't have netloc assume is relative path so join to url
                          else  tag
                          # if url has url treat it as a url
                          for tag in tags if
                              # if HAS netloc AND it's sufix is allowed AND is a path to an allowed file type
                          ( ( ( urlparse(tag).netloc != ''
                                and Path(urlparse(tag).path).suffix in ALLOWED_FILES
                                and ( TLDExtract(tag).suffix in ALLOWED_DOMAINS
                                    or TLDExtract(tag).subdomain in ALLOWED_DOMAINS ) )
                              or
                              # If NOT have netloc (is relative path)
                              # AND is an allowed file
                              # AND this site is in the allowed domains
                              (urlparse(tag).netloc == ''
                               and Path(urlparse(tag).path).suffix in ALLOWED_FILES
                               )
                              or TLDExtract(tag).subdomain == 'portal-sites')
                               # Commented seems avoid impossible situation (that the page has not allowed domain)
                               #and ( TLDExtract(url).suffix in ALLOWED_DOMAINS
                               #     or TLDExtract(url).subdomain in ALLOWED_DOMAINS) ) )
                            and
                            # If it wasn't followed or is not scheduled for following
                            ( tag not in visited_urls
                             or
                              urljoin(url,tag) not in visited_urls ) ) ]
                for link in links:
                    follow.put(link)
        except Exception:
            continue
    else:
        print(f'[getLinks] - No processo de saida')
        client = MongoClient(MONGOKEY)
        db = client.MAXUT
        visited = db['visited_sites']
        for url in visited_sites:
            visited.insert_one({'url':url})
        getLinksFlag.value = 0
        return
        

def htmlStructure(html):
    string = ''
    while True:
        parts = html.partition('</')
        if parts[1] == '':
            return string
        split = parts[2].split('>',1)
        string += split[0]
        html = split[1]


def classify(queue, flag):
    '''Find different html structures, classify them as e-commerce or not and store on Database'''
    ML_objects = []
    structures = set()
    while flag.value == 0:
        try:
            if not queue.empty():
                response = queue.get()
                url = response.url
                body = response.text.partition('<main')
                if body[1] == '':
                    body = response.text.partition('<body>')[2]
                else:
                    body = body[2].partition('</main>')[0]
                structure = htmlStructure(body)
                if structure not in structures:
                    structures.add(structure)
                    ML_Object = {}
                    ML_Object['url'] = url
                    ML_Object['html'] = response.text
                    ML_objects.append(ML_Object)
        except Exception:
            continue
    else:
        print(f'[classify] - Processo de saida')
        client = MongoClient(MONGOKEY)
        db = client.MAXUT
        ecom = db['e-com']
        ecom.insert_many(ML_objects)
        flag.value = 0
        return
    

getLinks
def finalizer(follow_queue, responses_queue, getLinks_queue, classify_queue,getLinksFlag, classifyFlag, visited_urls):
    '''Transfers visited urls and sites sets to the database after checking for empty deques
        and queues, I.E. end of program'''
    flag = False
    global crawling
    while True:
        try:
            sleep(10)
            if (follow_queue.qsize() == 0 
            and responses_queue.qsize() == 0 
            and getLinks_queue.qsize() == 0
            and classify_queue.qsize() == 0):
                if flag == False:
                    flag = True
                    continue
                else:
                    stop = perf_counter_ns() - 7200000000000
                    print(f'--------- CRAWL END ---------')
                    print(f'Duration: {((stop-start)//1000000000)/(60/(60//24))} days')
                    print(f'Urls crawled: {visited_urls.value}')
                    print(f'Urls per second: {visited_urls.value/(stop-start//1000000000)}')
                    print(f'Downloading data to Database... Whait for it to end')
                    crawling = False
                    getLinksFlag.value = 1
                    classifyFlag.value = 1
                    while True:
                        sleep(1)
                        if getLinksFlag.value == 0 and classifyFlag.value == 0:
                            return
            elif flag == True:
                flag = False
        except Exception as e:
            print(f'[finalizer] - Error: {e}')
            continue

# Algorithm
start = time()
try:
    if __name__ == '__main__':
        visited_urls = Value('i',0)
        getLinksFlag = Value('i',0)
        classifyFlag = Value('i',0)
        processes = []
        processes.append(Process(target=classify,args=(classify_queue, classifyFlag),name='classify'))
        processes.append(Process(target=getLinks,args=(getLinks_queue, follow, visited_urls, getLinksFlag),name='getLinks'))
        Thread(target=threadLauncher,args=(TRIES, session, follow, responses_queue,)).start()
        Thread(target=info,args=(visited_urls,follow,)).start()
        Thread(target=responsesThread,args=(responses_queue, getLinks_queue, classify_queue,)).start()
        for p in processes:
            p.start()
            if p.name == 'getLinks':
                pid = p.pid
                psu = psutil.Process(pid)
                psu.cpu_affinity([3])
                print(f'New CPU affinity for getLinks:{psu.cpu_affinity()}')
            if p.name == 'classify':
                pid = p.pid
                psu = psutil.Process(pid)
                psu.cpu_affinity([2])
                print(f'New CPU affinity for classify:{psu.cpu_affinity()}')
        for seed in SEEDS:
            follow.put(seed)
except Exception as e:
    print(f'CRITICAL ERROR - Processes or Threaads not launched: {e}')

finalizer(follow, responses_queue, getLinks_queue, classify_queue, getLinksFlag, classifyFlag, visited_urls)
