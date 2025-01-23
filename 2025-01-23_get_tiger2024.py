#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 2025

@author: Tiangeng Lu

Download geo shape files from https://www2.census.gov/geo/tiger/TIGER2024/
"""
# create the folder if haven't done it yet
import os
if not os.path.exists(r'tiger2024'):
    os.makedirs('tiger2024')
else: print('Folder exists.')
# change directory to the new folder
os.chdir('tiger2024')
# check system version
import sys
print(sys.version)
import time as tm # create time stamps for each iteration

import requests # .get().content from a url
from scrapy import Selector # Selector(text =)
from urllib import request # .urlretrieve(url, filename)

main_url = "https://www2.census.gov/geo/tiger/TIGER2024/TABBLOCK20/"
main_html = requests.get(main_url).content
main_selector = Selector(text = main_html)
# get the target objects to download
all_links = main_selector.xpath(query = '//*[contains(@href, ".zip")]/@href').extract()
# get the FULL valid urls with "http"
full_urls = [main_url + link for link in all_links if not link.startswith('http')]
# start the download
for i in range(len(all_links)):
    if os.path.exists(all_links[i]):
        print(f"File {all_links[i]} exists.")
    else:
        print(f'Now downloading {all_links[i]}, {tm.strftime("%Y-%m-%d, %H:%M:%S")}')
        request.urlretrieve(full_urls[i], all_links[i])