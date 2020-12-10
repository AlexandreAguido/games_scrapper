import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

def main():
    method = sys.argv[1]
    print(method)
    if not method or method not in ('discover', 'update'): return
    process = CrawlerProcess(get_project_settings())
    process.crawl('kabum', method=method)
    process.start()

if __name__ == '__main__':
    main()