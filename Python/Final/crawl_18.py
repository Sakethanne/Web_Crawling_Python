import logging
import csv
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

class Crawler:
    def __init__(self, base_url, urls=[], max_pages=50000, max_depth=16):
        self.base_url = base_url
        self.session = requests.Session()  # Use session for connection pooling
        self.visited_urls = set()
        self.urls_to_visit = [(url, 1) for url in set(urls)]  # Start with depth of 1
        self.max_pages = max_pages
        self.fetched_pages = 0
        self.max_depth = max_depth
        self.site_name = urlparse(base_url).netloc.split('.')[1]
        self.init_csv_files()

    def init_csv_files(self):
        # Initialize CSV files
        with open(f'fetch_{self.site_name}.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['URL', 'Status'])
        with open(f'visit_{self.site_name}.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['URL', 'Size (Bytes)', '# of Outlinks', 'Content-Type'])
        with open(f'urls_{self.site_name}.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['URL', 'Indicator'])

    def download_url(self, url, depth):
        if self.fetched_pages >= self.max_pages or depth > self.max_depth:
            return '', 0, 0, '', True
        try:
            response = requests.get(url)
            content_type = response.headers.get('Content-Type', '')
            if any(ct in content_type for ct in ['html', 'pdf', 'msword', 'image']):
                self.fetched_pages += 1
                return response.text, response.status_code, len(response.content), content_type, False
            else:
                return '', 0, 0, content_type, True
        except Exception as e:
            logging.exception(f'Error downloading {url}: {e}')
            return '', 0, 0, '', True

    def crawl(self, url, depth):
        if url in self.visited_urls:
            return []
        self.visited_urls.add(url)
        html, status_code, size, content_type, skip = self.download_url(url, depth)
        
        # Skip logging and processing for status codes 0 and 999
        if status_code == 0 or status_code == 999:
            return []
        
        with open(f'fetch_{self.site_name}.csv', 'a', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow([url, status_code])
        
        if not skip:
            outlinks = list(self.get_linked_urls(url, html))
            for outlink in outlinks:
                indicator = 'OK' if urlparse(outlink).netloc == urlparse(self.base_url).netloc else 'N_OK'
                with open(f'urls_{self.site_name}.csv', 'a', newline='', encoding='utf-8') as urls_file:
                    urls_writer = csv.writer(urls_file)
                    urls_writer.writerow([outlink, indicator])
            with open(f'visit_{self.site_name}.csv', 'a', newline='', encoding='utf-8') as file:
                writer = csv.writer(file)
                writer.writerow([url, size, len(outlinks), content_type])
            return outlinks
        return []

    def get_linked_urls(self, url, html):
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            path = link.get('href')
            if path and path.startswith('/'):
                path = urljoin(url, path)
            elif not path or not path.startswith('http'):
                continue
            yield path

    def run(self):
        with ThreadPoolExecutor(max_workers=400) as executor:  # Increased number of workers
            futures = {executor.submit(self.crawl, url, depth): (url, depth) for url, depth in self.urls_to_visit}
            while futures:
                # Process futures as they complete
                done, _ = as_completed(futures), futures.pop
                for future in done:
                    url, depth = futures.pop(future)
                    if self.fetched_pages >= self.max_pages:
                        break
                    if depth < self.max_depth:
                        outlinks = future.result()
                        for outlink in set(outlinks) - self.visited_urls:
                            if self.fetched_pages < self.max_pages:
                                futures[executor.submit(self.crawl, outlink, depth + 1)] = (outlink, depth + 1)


if __name__ == '__main__':
    Crawler(base_url='https://www.nytimes.com/', urls=['https://www.nytimes.com/']).run()