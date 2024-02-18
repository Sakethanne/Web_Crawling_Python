import logging
import csv
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.INFO)

class Crawler:
    def __init__(self, base_url, urls=[], max_records=20000, max_depth=16):
        self.base_url = base_url
        self.session = requests.Session()  # Use session for connection pooling
        self.visited_urls = set()
        self.urls_to_visit = [(url, 1) for url in set(urls)]  # Start with depth of 1
        self.max_records = max_records
        self.max_depth = max_depth
        self.site_name = urlparse(base_url).netloc.split('.')[1]
        self.init_csv_files()
        self.fetched_pages = 0
        self.non_200_count = 0
        self.max_non_200 = 1811

    def init_csv_files(self):
        # Initialize CSV files
        with open(f'fetch_{self.site_name}.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['URL', 'Status'])
        with open(f'visit_{self.site_name}.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['URL', 'Size', 'Out Links Found', 'Content Type'])
        with open(f'urls_{self.site_name}.csv', 'w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(['URL', 'Status'])

    def download_url(self, url, depth):
        try:
            response = requests.get(url)
            content_type = response.headers.get('Content-Type', '')
            if any(ct in content_type for ct in ['html', 'pdf', 'msword', 'image']):
                return response.text, response.status_code, len(response.content), content_type, False
            else:
                return '', response.status_code, 0, content_type, True
        except Exception as e:
            logging.exception(f'Error downloading {url}: {e}')
            return '', 0, 0, '', True

    def crawl(self, url, depth):
        if self.fetched_pages >= self.max_records or depth > self.max_depth:
            return []
        if url in self.visited_urls:
            return []
        self.visited_urls.add(url)
        html, status_code, size, content_type, skip = self.download_url(url, depth)
    
        if status_code != 200:
            if self.non_200_count >= self.max_non_200:
                logging.info('Maximum limit of non-200 status code URLs reached. Stopping further processing.')
                return []  # Stop processing non-200 status code URLs if limit reached
            self.non_200_count += 1
            logging.info(f'Number of unsuccessful URLs: {self.non_200_count}')

        with open(f'fetch_{self.site_name}.csv', 'a', newline='', encoding='utf-8') as file:
            if status_code != 200:
                if self.non_200_count <= self.max_non_200:
                    writer = csv.writer(file)
                    writer.writerow([url, status_code])
            else:
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
        with ThreadPoolExecutor(max_workers=400) as executor:
            futures = {executor.submit(self.crawl, url, depth): (url, depth) for url, depth in self.urls_to_visit}
            while futures and self.fetched_pages < self.max_records:
                done, _ = as_completed(futures), futures.pop
                for future in done:
                    url, depth = futures.pop(future)
                    if self.fetched_pages >= self.max_records:
                        break
                    if depth < self.max_depth:
                        outlinks = future.result()
                        for outlink in set(outlinks) - self.visited_urls:
                            if self.fetched_pages < self.max_records:
                                futures[executor.submit(self.crawl, outlink, depth + 1)] = (outlink, depth + 1)

if __name__ == '__main__':
    crawler = Crawler(base_url='https://www.nytimes.com/', urls=['https://www.nytimes.com/'])
    crawler.run()