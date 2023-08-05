#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug  5 00:31:41 2023

@author: Tiangeng Lu

Download the H-1b employer data hub files between FY09 and FY23
"""

import requests
from scrapy import Selector
import os
from urllib import request
# urls
main_url = 'https://www.uscis.gov/tools/reports-and-studies/h-1b-employer-data-hub/h-1b-employer-data-hub-files'
main_html = requests.get(main_url).content
main_selector = Selector(text = main_html)
all_links = main_selector.xpath('//*[contains(@href, ".csv")]/@href').extract()
# the string split details depends on the link
df_names = [link.split('/')[-1].split('.')[0].replace('export-','_') for link in all_links]

# Create the directory, download and save data
if not os.path.exists('h1b'):
    os.makedirs('h1b')    
for i in range(len(all_links)):
    request.urlretrieve(all_links[i], 'h1b/'+df_names[i]+'.csv')

# Data were saved to local directories without cleaning.
# Next steps include running basic summary statistics by FY and adding NAICS definition to the data