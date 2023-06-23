#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 21 23:49:56 2023

@author: Tiangeng Lu

Simple webscrape online tables from three pages
Scrape tables of visa classsification and descriptions
"""
import requests
from scrapy import Selector
import pandas as pd
iv_link1 = 'https://fam.state.gov/fam/09FAM/09FAM050201.html#M502_1_3'
niv_link1 = 'https://fam.state.gov/FAM/09FAM/09FAM040201.html'
visa_categories = 'https://travel.state.gov/content/travel/en/us-visas/visa-information-resources/all-visa-categories.html'

### IV
iv_html = requests.get(iv_link1).content
iv_selector = Selector(text = iv_html)
extracted_raw = iv_selector.xpath('//table').extract()
# length = 1
len(extracted_raw)
# MUST index [0] to see the element, w/o [0], it's the whole list
# print() for better view of the raw table
print(extracted_raw[0])
# MUST add [0] so that it returns a dataframe, without the last [0], the type would be a list
df_iv = pd.read_html(extracted_raw[0], header = 0)[0]

### NIV
niv_raw = Selector(text = requests.get(niv_link1).content).xpath('//table').extract()
df_niv = pd.read_html(niv_raw[0], header = 0)[0]

# DoS Supplement Categories 
visa_categories_raw = Selector(text = requests.get(visa_categories).content).xpath('//table').extract()
# length = 2, one for immigrant visa, the other for nonimmigrant visa
print(len(visa_categories_raw))
df_niv_more = pd.read_html(visa_categories_raw[0])[0]
df_iv_more = pd.read_html(visa_categories_raw[1])[0]