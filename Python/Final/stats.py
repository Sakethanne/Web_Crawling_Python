import csv

# Constants
FETCH_FILE = 'fetch_nytimes.csv'
URLS_FILE = 'urls_nytimes.csv'
VISIT_FILE = 'visit_nytimes.csv'
CRAWL_REPORT_FILE = 'CrawlReport_nytimes_1.txt'

# Function to collate statistics
def collate_statistics():
    # Initialize counters
    fetch_attempted = 0
    fetch_succeeded = 0
    fetch_failed = 0
    total_urls_extracted = 0
    unique_urls_extracted = set()
    unique_news_website_urls = set()
    unique_external_urls = set()
    status_codes = {}
    file_sizes = {}
    content_types = set()

    # Read fetch statistics from fetch file
    with open(FETCH_FILE, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            fetch_attempted += 1
            status_code = int(row[1])
            if 200 <= status_code < 300:
                fetch_succeeded += 1
            else:
                fetch_failed += 1
            status_codes[status_code] = status_codes.get(status_code, 0) + 1

    # Read URLs statistics from URLs file
    with open(URLS_FILE, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            total_urls_extracted += 1
            url = row[0]
            unique_urls_extracted.add(url)
            if row[1] == 'OK':
                unique_news_website_urls.add(url)
            else:
                unique_external_urls.add(url)

    # Read visit statistics from visit file
    with open(VISIT_FILE, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for row in reader:
            content_type = row[3]
            file_size = int(row[1])
            content_types.add(content_type)
            file_sizes[file_size] = file_sizes.get(file_size, 0) + 1

    # Generate formatted output
    output = f"""Fetch statistics:
# fetches attempted: {fetch_attempted}
# fetches succeeded: {fetch_succeeded}
# fetches failed or aborted: {fetch_failed}

Outgoing URLs: statistics about URLs extracted from visited HTML pages
Total URLs extracted: {total_urls_extracted}
# unique URLs extracted: {len(unique_urls_extracted)}
# unique URLs within your news website: {len(unique_news_website_urls)}
# unique URLs outside the news website: {len(unique_external_urls)}

Status codes:
{format_status_codes(status_codes)}

File sizes:
{format_file_sizes(file_sizes)}

Content Type:
{', '.join(content_types)}
"""

    # Write output to file
    with open(CRAWL_REPORT_FILE, 'w', encoding='utf-8') as file:
        file.write(output)

# Helper function to format status codes
def format_status_codes(status_codes):
    return '\n'.join(f"{status_code}: {count}" for status_code, count in status_codes.items())

# Helper function to format file sizes
# Helper function to format file sizes
def format_file_sizes(file_sizes):
    ranges = [(0, 1023), (1024, 1048575), (1048576, 2147483647)]  # Adjusted upper bound for the last range
    labels = ['< 1KB', '1KB - 1MB', '> 1MB']
    output = []
    for label, (lower, upper) in zip(labels, ranges):
        count = sum(file_sizes.get(size, 0) for size in range(lower, upper + 1))
        output.append(f"{label}: {count}")
    return '\n'.join(output)


# Main function
if __name__ == "__main__":
    collate_statistics()
