import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import csv
import queue
import mimetypes

# Function to fetch URLs and their status codes
def fetch_urls(url_list, news_site_name):
    fetched_urls = []
    total_urls = len(url_list)
    for i, url in enumerate(url_list):
        try:
            response = requests.get(url)
            status_code = response.status_code
            fetched_urls.append([url, status_code])
        except Exception as e:
            fetched_urls.append([url, str(e)])
        print(f"Fetching {i + 1}/{total_urls} - URL: {url}")  # Progress indicator

        if len(fetched_urls) >= 10000:  # Limit to 10,000 records
            break

    with open(f'fetch_{news_site_name}.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['URL', 'Status'])
        writer.writerows(fetched_urls)


# Function to visit URLs, save successfully downloaded files, and record metadata
def visit_urls(url_list, news_site_name):
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

        print(f"Visiting {i + 1}/{total_urls} - URL: {url}")  # Progress indicator

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
def crawl(q, domain, visited=set(), all_urls=set(), limit=10000, max_depth=16):
    crawled_count = 0
    while crawled_count < limit:
        try:
            url, depth = q.get(timeout=1)
            if url in visited or depth > max_depth:
                continue
            visited.add(url)
            print(f"Crawling {crawled_count + 1}/{limit} - Queue Size: {q.qsize()} - Current URL: {url}")
            all_urls.add(url)
            crawled_count += 1
            response = requests.get(url)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                for link in soup.find_all('a', href=True):
                    next_url = urljoin(url, link['href'])
                    if get_domain(next_url) == domain:
                        q.put((next_url, depth + 1))
                    else:
                        all_urls.add(next_url)
        except queue.Empty:
            break
        except Exception as e:
            print(f"Error crawling {url}: {e}")
            continue  # Continue crawling even if there's an error


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
    crawl(q, domain, visited, all_urls)
    categorize_urls(all_urls, news_site_name, base_url)
    fetch_urls(all_urls, news_site_name)
    visit_urls(all_urls, news_site_name)
    print("Crawling Completed!")


# Example usage:
if __name__ == "__main__":
    url = 'https://www.nytimes.com'
    news_site_name = 'NYTimes'
    main(url, news_site_name)