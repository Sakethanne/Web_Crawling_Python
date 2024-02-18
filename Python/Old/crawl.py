import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import csv
import queue
import threading
import mimetypes
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global variable to store the crawled count
crawled_count = 0
total_urls_to_crawl = 200

# Function to fetch URLs and their status codes
# Function to fetch URLs and their status codes
def fetch_urls(url_list, news_site_name, lock):
    global crawled_count
    with lock:
        if crawled_count >= total_urls_to_crawl:
            return
    fetched_urls = []
    total_urls = len(url_list)
    for i, url in enumerate(url_list):
        if crawled_count >= total_urls_to_crawl:
            break
        try:
            response = requests.get(url)
            status_code = response.status_code
            fetched_urls.append([url, status_code])
        except Exception as e:
            fetched_urls.append([url, str(e)])
            logger.error(f"Error fetching URL: {url}, {e}")
        print(f"Fetching {i + 1}/{total_urls} - URL: {url}")  # Progress indicator
        with lock:
            crawled_count += 1

    with open(f'fetch_{news_site_name}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['URL', 'Status'])
        writer.writerows(fetched_urls)

# Function to visit URLs, save successfully downloaded files, and record metadata
def visit_urls(url_list, news_site_name, lock):
    with lock:
        global crawled_count
        if crawled_count >= total_urls_to_crawl:
            return
    visited_urls = []
    total_urls = len(url_list)
    for i, url in enumerate(url_list):
        try:
            response = requests.get(url)
            content_type = response.headers.get('Content-Type', 'Unknown')
            if any(content_type.startswith(mime) for mime in ['text/html', 'application/msword', 'application/pdf']):
                soup = BeautifulSoup(response.content, 'html.parser')
                outlinks = soup.find_all('a', href=True)
                outlinks_count = len(outlinks)
                visited_urls.append([url, len(response.content), outlinks_count, content_type])
            else:
                # Record metadata for images
                ext = mimetypes.guess_extension(content_type.split(';')[0])
                if ext in {'.jpeg', '.jpg', '.png', '.gif'}:
                    img_info = response.headers
                    visited_urls.append([url, img_info.get('Content-Length', 0), 0, content_type])
                else:
                    visited_urls.append([url, len(response.content), 0, content_type])
        except Exception as e:
            visited_urls.append([url, str(e), 0, 'Unknown'])
            logger.error(f"Error visiting URL: {url}, {e}")

        print(f"Visiting {i + 1}/{total_urls} - URL: {url}")  # Progress indicator

        with lock:
            crawled_count += 1
            if crawled_count >= total_urls_to_crawl:
                break

    with lock:
        with open(f'visit_{news_site_name}.csv', 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['URL', 'Size', 'Outlinks', 'Content-Type'])
            writer.writerows(visited_urls)


# Function to extract domain from URL
def get_domain(url):
    parsed_url = urlparse(url)
    return parsed_url.netloc


# Function to extract base URL
def get_base_url(url):
    parsed_url = urlparse(url)
    return parsed_url.scheme + '://' + parsed_url.netloc


# Function to crawl URLs
def crawl(q, domain, visited, all_urls, lock, max_depth=16):
    global crawled_count
    while True:
        with lock:
            if crawled_count >= total_urls_to_crawl:
                break
        try:
            url, depth = q.get(timeout=1)
            if url in visited or depth > max_depth:
                continue
            visited.add(url)
            print(f"Crawling {crawled_count + 1} - Current URL: {url}")
            all_urls.add(url)
            with lock:
                crawled_count += 1
                print(f"Progress: {crawled_count} / {total_urls_to_crawl}")  # Progress indicator
                if crawled_count >= total_urls_to_crawl:
                    break
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                for link in soup.find_all('a', href=True):
                    next_url = urljoin(url, link['href'])
                    if get_domain(next_url) == domain:
                        q.put((next_url, depth + 1))
        except queue.Empty:
            break
        except Exception as e:
            print(f"Error crawling {url}: {e}")
            logger.error(f"Error crawling URL: {url}, {e}")
            continue  # Continue crawling even if there's an error

    print("Total URLs processed from queue:", crawled_count)


# Function to categorize URLs
def categorize_urls(urls, news_site_name, base_url):
    categorized_urls = []
    for url in urls:
        if url.startswith(base_url):
            categorized_urls.append([url, 'OK'])
        else:
            categorized_urls.append([url, 'N_OK'])

    with open(f'urls_{news_site_name}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['URL', 'Category'])
        writer.writerows(categorized_urls)


def main(url, news_site_name):
    domain = get_domain(url)
    base_url = get_base_url(url)
    q = queue.Queue()
    q.put((url, 1))  # Include initial URL and depth
    all_urls = set()
    visited = set()
    lock = threading.Lock()

    # Create and start 50 threads
    threads = []
    for _ in range(50):
        t = threading.Thread(target=crawl, args=(q, domain, visited, all_urls, lock))
        t.start()
        threads.append(t)

    # Wait for all threads to finish
    for t in threads:
        t.join()

    categorize_urls(all_urls, news_site_name, base_url)
    fetch_urls(all_urls, news_site_name, lock)
    visit_urls(all_urls, news_site_name, lock)
    print("Crawling Completed!")


# Example usage:
if __name__ == "__main__":
    url = 'https://www.nytimes.com'
    news_site_name = 'NYTimes'
    main(url, news_site_name)