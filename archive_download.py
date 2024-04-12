

import dataclasses
import threading
import requests
import argparse
import pandas
import shutil
import time
import json
import sys
import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin
from dataclasses import dataclass
from bs4 import BeautifulSoup
from lxml import html
from tqdm import tqdm
from enum import Enum

@dataclass
class UrlData:
    name : str
    url : str
    ext : str

class ProgressStatus(Enum):
    MISSING = "missing"
    IN_PROGRESS = "in_progress"
    DONE = "done"

@dataclass
class CSVData:
    name : str
    url : str
    ext : str
    progress : ProgressStatus
    final_file : str

csv_data = []
csv_filename = ""
csv_lock = threading.Lock()
print_lock = threading.Lock()
progress_lock = threading.Lock()

def url_to_filename(url):
    # Remove protocol and replace slashes with underscores
    filename = re.sub(r'^https?://', '', url.replace('/', '_'))
    # Remove other invalid characters
    filename = re.sub(r'[^\w\-.]', '_', filename)
    # Remove leading and trailing whitespace
    filename = filename.strip()
    return filename

def save_csv():
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["name", "url", "ext", "progress", "final_file"])
        for data in csv_data:
            writer.writerow([data.name, data.url, data.ext, data.progress.value, data.final_file])

def claim_missing_csv():
    with csv_lock:
        for data in csv_data:
            # If progress is "missing", update it to "in_progress" and return the data
            if data.progress == ProgressStatus.MISSING:
                data.progress = ProgressStatus.IN_PROGRESS
                return dataclasses.replace(data) # NOTE: returns a copy
    return None

def finalize_csv_data(final_data):
    with csv_lock:
        for data in csv_data:
            # If final_file exists and progress is not already "done"
            if data.url == final_data.url:
                if not final_data.final_file:
                    print("File missing for " + final_data.name)
                    return
                if not os.path.exists(args.out_dir):      
                    print("File \"{file}\" doesnt exists".format(file=final_data.name))
                    return
                data.progress = ProgressStatus.DONE
                data.final_file = final_data.final_file
                save_csv()
                return
            


def ScrapeUrls(page_to_scrape, to_skip):
    scraped_data = []

    print("Downloading {url}".format(url=page_to_scrape))
    page_raw = requests.get(page_to_scrape)
    soup = BeautifulSoup(page_raw.text, 'html.parser')
        

    print("Scraping {url}".format(url=page_to_scrape))
    feed_objs = soup.find_all("tr")
    index = 0

    scrape_progress = tqdm(desc="Scraping: ", total=len(feed_objs), unit='items')
    for feed_obj in feed_objs:
        index += 1
        scrape_progress.update(1)
        if index > to_skip:
            url = feed_obj.find("a")

            img_name = url.text
            img_url = url.attrs['href']

            # Check if the href attribute is a relative URL
            if not img_url.startswith('http'):
                # If it's a relative URL, join it with the base URL of the page
                img_url = urljoin(page_to_scrape, img_url)

            dot_pos = img_url.rfind('.')
            img_ext = img_url[dot_pos + 1:]
            img_name = img_name[:img_name.rfind('.')]

            if len(img_ext) < 5: # probably a valid file.  hacky way to skip "parent directory" type shit
                scraped_data.append(UrlData(img_name, img_url, img_ext))
    scrape_progress.close()
    return scraped_data

def custom_unit_scale(x):
    if x < 1024:
        return "{:.2f} B".format(x)
    elif 1024 <= x < 1024 * 1024:
        return "{:.2f} KB".format(x / 1024)
    else:
        return "{:.2f} MB".format(x / (1024 * 1024))
    
def GetFiles(out_dir, thread_index, progress_bar, progress_total):
    with print_lock:
        print("Thread {thread} starting".format(thread=thread_index))
    image_data = claim_missing_csv()
    while image_data is not None:
        final_file = "{path}/{id}.{ext}".format(path=out_dir, id = image_data.name, ext = image_data.ext)
        image_data.final_file = final_file

        # download the file
        file_request = requests.get(image_data.url, stream=True)
        total_size_in_bytes= int(file_request.headers.get('content-length', 0))
        block_size = 10240 #10 KB

        # reset the bar
        progress_bar.bar_format='{desc}: {percentage:3.0f}% | {rate_fmt}{postfix} | ' + image_data.name
        progress_bar.total = total_size_in_bytes
        progress_bar.reset()
        with open(final_file, 'wb') as file_out:
            for data in file_request.iter_content(block_size):
                progress_bar.update(len(data))
                file_out.write(data)     
            with progress_lock:
                progress_total.update(1)   

        finalize_csv_data(image_data) # write back to the csv
        image_data = claim_missing_csv() # attempt to loop again
    with print_lock:
        print("Thread {thread} closing".format(thread=thread_index))

def main(args):

    # ensure the output data exists
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)

    # ensure our database directory exists
    if not os.path.exists(args.db_dir):
        os.makedirs(args.db_dir)

    # Check if CSV file exists
    global csv_filename
    global csv_data
    csv_filename = args.db_dir + "/" + url_to_filename(args.collection_url) + ".csv"
    if not os.path.exists(csv_filename):
        # If not, create a new one with headers
        with open(csv_filename, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["name", "url", "ext", "progress", "final_file"])

    # Read existing data from CSV
    print("Reading database {url}".format(url=csv_filename))
    existing_data = pandas.read_csv(csv_filename)
    already_completed = 0

    # Convert existing_data DataFrame to list of CSVData objects
    for _, row in tqdm(iterable=existing_data.iterrows(), desc='Parsing database'):
        # Update progress status of existing entries from "pending" to "missing"
        progress = row['progress']
        final_file = row['final_file']
        if progress == ProgressStatus.DONE.value:
            already_completed += 1
        elif progress == ProgressStatus.IN_PROGRESS.value:
            progress = ProgressStatus.MISSING
            final_file = ""
        csv_data.append(CSVData(row['name'], row['url'], row['ext'], ProgressStatus(progress), final_file))

    # scrape all the URL data from the page
    scraped_urls = ScrapeUrls(args.collection_url, args.skip)

    # Append new data to existing data
    num_added = 0
    for url_data in tqdm(iterable=scraped_urls, desc='Preparing database'):
        if not any(url_data.url == data.url for data in csv_data):            
            #print("adding " + url_data.name)
            csv_data.append(CSVData(url_data.name, url_data.url, url_data.ext, ProgressStatus.MISSING, ""))
            num_added += 1
    print("Added: {num}".format(num=num_added))

    # Save the updated CSV file
    save_csv()
    
    progress_bars = []
    with ThreadPoolExecutor(max_workers=args.threads) as executor:
        # create the progress bars
        for thread_index in range(args.threads):
            progress_bar = tqdm(desc="Thread {thread}: ".format(thread=thread_index), unit='B', unit_divisor=1024, unit_scale=True)
            progress_bars.append(progress_bar)

        progress_total = len(scraped_urls)
        progress_total = tqdm(desc="Progress: ", initial=already_completed, total=progress_total, unit='items', bar_format='{desc}: {n}/{total} | {percentage:3.0f}%|{bar}')

        # create the threads
        futures = []
        for thread_index in range(args.threads):
            futures.append(executor.submit(GetFiles, args.out_dir, thread_index, progress_bars[thread_index], progress_total))
        
        # Wait for all threads to complete
        for future in futures:
            future.result()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='deep-dream')
    parser.add_argument('--collection_url', '-u')
    parser.add_argument('--out_dir', '-o', default='./output')
    parser.add_argument('--db_dir', '-d', default='./.db')
    parser.add_argument('--threads', '-t', type=int, default=1)
    parser.add_argument('--skip', '-s', type=int, default=1)
    args = parser.parse_args()

    result = main(args)
    sys.exit(result)
