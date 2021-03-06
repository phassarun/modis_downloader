import concurrent.futures
import os
import sys

import requests
from parsel import Selector


def download_file(url, dir_name):
    file_name = url.split('/')[-1]
    dir_file = '{}/{}'.format(dir_name, file_name)
    if os.path.exists(dir_file):
        return False

    path_file_name = os.path.join(dir_name, file_name)
    r = requests.get(url, allow_redirects=True, cookies=cookies)

    # download file
    with open(path_file_name, 'wb') as file:
        file.write(r.content)

    return True


def create_folder(folder_name):
    dir_name = '{}/{}'.format(DIR_EXPORTS, folder_name)
    
    # Create target Directory if don't exist
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    
    return dir_name


def get_folder_urls(BASE_URL, ALLOW_YEARS):
    allow_year_str = '|'.join(ALLOW_YEARS)
    regex_pattern = r'^((%s)\.\d{2}\.\d{2})' % allow_year_str

    r = requests.get(BASE_URL, allow_redirects=True)
    sel = Selector(text=r.text)
    folder_list = sel.css('a::attr(href)').re(regex_pattern)
    folder_filtered_list = list(filter(lambda folder_name: '.' in folder_name, folder_list))
    folder_urls = list(map(lambda folder_name: '%s/%s' % (BASE_URL, folder_name), folder_filtered_list))
    return folder_urls


def get_file_urls(folder_url, ALLOW_FILE_NAME):
    allow_file_name_str = '|'.join(ALLOW_FILE_NAME)
    regex_pattern = r'(\w{7}\.\w{8}\.(%s)\.\d{3}\.\d{13}.hdf$)' % (allow_file_name_str)

    r = requests.get(folder_url, allow_redirects=True)
    sel = Selector(text=r.text)
    file_name_list = sel.css('a::attr(href)').re(regex_pattern)
    filtered_file_name = list(filter(lambda file_name: 'hdf' in file_name, file_name_list))
    file_urls = list(map(lambda file_name: '%s/%s' % (folder_url, file_name), filtered_file_name))
    return file_urls


def run_requests(urls, dir_name):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(download_file, url, dir_name): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
            else:
                if data:
                    print('%r has been saved.' % url)
                else:
                    print('%r is ingored' % url)


def get_folder_name_from_url(url):
    return url.split('/')[-1]


def run_app(folder_url):
    folder_name = get_folder_name_from_url(folder_url)
    dir_name = create_folder(folder_name)
    file_urls = get_file_urls(folder_url, ALLOW_FILE_NAME)
    run_requests(file_urls, dir_name)
    return True


def get_cookies(folder_url, ALLOW_FILE_NAME):
    file_urls = get_file_urls(folder_url, ALLOW_FILE_NAME)
    auth_url = requests.get(file_urls[0], allow_redirects=True).url
    r = requests.get(auth_url, auth=(username, password))
    cookies = dict(r.history[-1].cookies)
    
    return cookies


ALLOW_YEARS = ['2018']
ALLOW_FILE_NAME = ['h27v06', 'h27v07', 'h27v08', 'h28v07', 'h28v08']
BASE_URL = 'https://e4ftl01.cr.usgs.gov/MOLT/MOD11A1.006/'
DIR_EXPORTS = 'exports'

if __name__ == "__main__":
    username = sys.argv[1]
    password = sys.argv[2]
    
    if not os.path.exists(DIR_EXPORTS):
        os.mkdir(DIR_EXPORTS)
    
    folder_urls = get_folder_urls(BASE_URL, ALLOW_YEARS)
    cookies = get_cookies(folder_urls[0], ALLOW_FILE_NAME)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        # Start the load operations and mark each future with its URL
        future_to_url = {executor.submit(run_app, url): url for url in folder_urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
            except Exception as exc:
                print('%r generated an exception: %s' % (url, exc))
