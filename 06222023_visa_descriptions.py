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
df_niv.columns = df_niv.columns.str.upper()
df_niv['CATEGORY'] = 'N'

### DoS Supplement Categories 
visa_categories_raw = Selector(text = requests.get(visa_categories).content).xpath('//table').extract()
# length = 2, one for immigrant visa, the other for nonimmigrant visa
print(len(visa_categories_raw))
df_niv_more = pd.read_html(visa_categories_raw[0], header = 0)[0]
# niv has one more column "Required: Before applying for visa"
df_niv_more.columns = [0,1,2]
df_niv_more['CATEGORY'] = 'N'

df_iv_more = pd.read_html(visa_categories_raw[1], header = 0)[0]
df_iv_more.columns = [0,1]
df_iv_more = df_iv_more.dropna(subset = [1])
df_iv_more['CATEGORY'] = 'I'
## Concatenate supplemental descriptions
visa_directory_more = pd.concat([df_niv_more, df_iv_more], axis = 0)\
    .rename(columns = {0:'CLASS', 1:'SYMBOL', 2:'PREREQUISITE'})  
visa_directory_more.to_csv('visa_directory_supplement.csv', index = False)

################ CLEANING #################
print(df_iv.columns)
# the following are the category descriptions
print([row.split(' ') for row in df_iv['SYMBOL'] if len(row.split(' '))>1])
# keep the original index info, if add the category descriptions back
df_iv_category = df_iv[df_iv['SYMBOL'].str.split().str.len() >= 2]
df_iv_clean = df_iv[df_iv['SYMBOL'].str.split().str.len() < 2]
df_iv_clean.columns = df_iv_clean.columns.str.upper()
df_iv_clean['CATEGORY'] = 'I'

### CONCATENATE IV & NIV ###
visa_directory = pd.concat([df_iv_clean, df_niv], axis = 0)
visa_directory.to_csv('visa_directory.csv', index = False)
