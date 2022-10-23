# gambot
The web crawler whose goal is to find and store all e-commerce sites products and its key:value pairs

## Minor Version 0.1.0
- Goal: Get most .pt sites and its different html structures to use for Machine Learning Learning and application as fast as possibe
- Multithreaded and Multiprocessing to maximize speed

### Patch 0.0.2 (Machine Learning Dataset crawler - 2nd try)
- Added native MongoDB database capability
- Fixed logic that crawled urls more than once
- Don't store >400 code responses
- Fixed crawling links on the head tag
- Added crawling info logs
- Added robust TLD handling using tldextract library
- Fixed url request loop-try-except logic incorrection

### Patch 0.0.1
- Added url to the error object to be stored
- Removed block=True argument in queue.put() because was not tested yet
- Removed log file creation

# Major Version 0.0.0
- Goal: Get data to use in learning Machine Learning
- First "stable" version
- Multithreaded crawler whose threads are created from links stored on a queue by each thread
- Crawled webpages restricted to tdl .pt
- Main purposes:
  - Get a list of all reachable .pt domains (country level registry doesn't makes them available)
  - Get data to apply Machine learning while studying it to achieve gambot's goal
  - Get data about all the headers and erros that can happen while crawling
