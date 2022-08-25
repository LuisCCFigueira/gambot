# gambot
The web crawler whose goal is to find and store all e-commerce sites products and its key:value pairs

### Patch 0.0.1
- Added url to the error object to be stored
- Removed block=True argument in queue.put() because was not tested yet
- Removed log file creation

## Version 0.0.0
- First "stable" version
- Multithreaded crawler whose threads are created from links stored on a queue by each thread
- Crawled webpages restricted to tdl .pt
- Main purposes:
  - Get a list of all reachable .pt domains (country level registry doesn't makes them available)
  - Get data to apply Machine learning while studying it to achieve gambot's goal
  - Get data about all the headers and erros that can happen while crawling
