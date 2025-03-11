#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Mar 10 20:50:33 2025

@author: Tiangeng Lu

- Scraped OPM raw data ended with `.zip` from https://www.opm.gov/data/datasets/
- Challenge in this scrape: the url doesn't contain actual file names.
- The webpage has a web-based data catalog table. The table needs to be scraped and filtered to match the scrape urls.
- The OPM web-based data catalog was scraped in a different program.


"""
import pandas as pd
import os
import time as tm
import requests # .get().content from a url
from scrapy import Selector # Selector(text =)
from urllib import request # .urlretrieve(url, filename)
from zipfile import ZipFile


target_folder = 'opm_raw'

if not os.path.exists(target_folder):
    os.makedirs(target_folder)
    print(f'Creating folder {target_folder}')
else: print(f'Folder {target_folder} exists as of {tm.strftime("%Y-%m-%d, %A, %H:%M")}.')

main_url = "https://www.opm.gov/data/datasets/"
main_html = requests.get(main_url).content
main_selector = Selector(text = main_html)

all_links = main_selector.xpath(query = '//*[contains(@href, ".zip")]/@href').extract()

     
# get the FULL valid urls with "http"
full_urls = ["https://www.opm.gov" + link for link in all_links if not link.startswith('http')]
for i in range(len(full_urls)):
    print(i,full_urls[i])
# start the download
os.chdir(target_folder)
os.listdir()
for i in range(len(full_urls)):
    print(f'Now downloading {full_urls[i]}, {tm.strftime("%Y-%m-%d, %H:%M:%S")}')
    request.urlretrieve(full_urls[i], "file_"+str(i)+".zip")

# This scrape task is different from previous ones. The urls are not informative and are in random order.

file_names=["file_"+str(i)+".zip" for i in range(len(full_urls))]


download_catalog=pd.DataFrame({'url': full_urls, 
              'file_name': file_names,
              'sizeMB':[round(os.path.getsize(f)/1024/1024, 2) for f in file_names]})
# save scraped catalog (url, filename, size) to file
download_catalog.to_csv("download_catalog.csv", index = True)
# import the scraped and edited opm-provided data catalog
scrape_catalog = pd.read_csv("scrape_catalog.csv", index_col = 0)
# the purpose of joining the two versions of catalogs is to find the approriate file name
# good news is, the downloaded size the published size match, meaning the data catalog is correct
catalog_full=pd.concat([download_catalog, scrape_catalog], axis = 1)
# output the joined catalog
catalog_full.drop(columns = ['file_name']).to_csv("opm_raw_zip_catalog_publish.csv", index = False)
