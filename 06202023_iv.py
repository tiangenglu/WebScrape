#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 2023

@author: Tiangeng Lu

Auto-download monthly immigrant visa statistics .pdf files with minimum loops.
This program checks the download status of the targeted files before downloading all files.
This program will only download the files that haven't been downloaded yet.
"""

import requests
from scrapy import Selector
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import os
from urllib import request
from datetime import datetime

# user-defined function(s)
def dtime(file):
    return datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d, %A, %H:%M:%S")


# URL
main_url = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics/immigrant-visa-statistics/monthly-immigrant-visa-issuances.html'
# request content from the main url
main_html = requests.get(main_url).content
# object type is 'bytes'
type(main_html)
# select text, `main_selector` type is `selector.unified.Selector`, NOT subscriptable
main_selector = Selector(text = main_html)
# get urls that contain .pdf; this selector.xpath.extract() is important
all_links = main_selector.xpath('//*[contains(@href, ".pdf")]/@href').extract()
# examine the PATTERNS of these links, then subset them
print(all_links[:5])
# the following step is data/url-specific, select the urls that contain certain pattern
national_links = [link for link in all_links if "FSC" in link]
national_links = list(set(national_links))
print(len(national_links))

# Count the URLs in each year: 2017 should have 10 (from March), 2023 has 4 (up to April), and all other years should have 12
print("2017:",len([link for link in national_links if "2017" in link]))
print("2018:",len([link for link in national_links if "2018" in link]))
print("2019:",len([link for link in national_links if "2019" in link]))
print("2020:",len([link for link in national_links if "202020" in link])) # not a typo but by the actual links
print("2021:",len([link for link in national_links if "2021" in link]))
print("2022:",len([link for link in national_links if "2022" in link]))
print("2023:",len([link for link in national_links if "2023" in link]))

# add prefix
prefix = 'https://travel.state.gov'
# list comprehension/generator to replace a loop over list elements
national_links = [prefix + link for link in national_links if link.startswith('/content')]
# save urls
file = open('URL_iv_visa_nationality.txt', 'w')
for url in national_links:
    file.write(url + "\n")
file.close()

# build catalog by extracting yyyy-mm info
# given the observed patterns, subset the substrings help get the yyyy-mm info
print(national_links[0].split('/')[-1].split('%'))
# use list comprehension/generator to replace loops over list elements
month = [link.split('/')[-1].split('%')[0].upper() for link in national_links]
year = [link.split('/')[-1].split('%')[1][2:] for link in national_links]
print(set(month)); print(set(year))
# fix the irregular patterns, starting with singling out the problematic observations
# the following will serve as the .iloc indexer
temp_list = [i for i, yr in enumerate(year) if len(yr) != 4]

national_catalog = pd.DataFrame(
    {'url': national_links,
     'year': year,
     'month':month
     })
# observe the problematic rows, it's easy to fix: month has 'AUGUST2021', and year has '-'
# for year, get the last four characters from the month column
national_catalog['year'].iloc[temp_list] = national_catalog['month'].iloc[temp_list].str[-4:]
# for month, remove the last four characters
national_catalog['month'].iloc[temp_list] = national_catalog['month'].iloc[temp_list].str[:-4]
# the following gives the numeric index value 
# fix september
extr_fix_id = [i for i, mon in enumerate(national_catalog['month']) if mon == "SEPT"]
national_catalog['month'][extr_fix_id] = "SEPTEMBER"
# alternative code
# national_catalog['month'].iloc[national_catalog[national_catalog['month']=='SEPT'].index] = 'SEPTEMBER'
print(set(national_catalog['month']))
# create time stamp
national_catalog['mmyy'] = national_catalog['year'].str.cat(national_catalog['month'], sep = '-')
# shift to month end given the nature of the data
national_catalog['mmyy'] = pd.to_datetime(national_catalog['mmyy'], errors = 'ignore') + MonthEnd()
# remove HH:MM
national_catalog['mmyy'] = national_catalog['mmyy'].dt.date
national_catalog = national_catalog.sort_values('mmyy', ascending = True).reset_index(drop = True)

path = 'iv'
path_Exist = os.path.exists(path)
if not path_Exist:
    os.makedirs(path)
print("The folder was last created at:", datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d, %A"))
# print-out the last document, must be sorted by time
sorted(os.listdir(path))[-1]
# create a list of filenames
all_filenames = [None] * len(national_catalog)
for i in range(len(national_catalog)):
    all_filenames[i] = os.getcwd() + '/' + path + '/' + 'iv_' + str(national_catalog['mmyy'][i])[:10] + '.pdf'     
all_filenames
national_catalog['filename'] = all_filenames
# check whether all pdfs have been downloaded, and then add the download status to the catalog data
status = [None]*len(national_catalog)
# check the download status for each filename
for i,k in enumerate(national_catalog['filename']):
    if os.path.isfile(k):
        status[i] = True
        print(dtime(k))
    else:
        status[i] = False
# update the download status in national_catalog
if len(status) == len(national_catalog):
    national_catalog['downloaded'] = status 
# if the file hasn't been downloaded yet, download it now
for i in range(len(national_catalog)):
    if national_catalog['downloaded'][i] == True:
        print(national_catalog['filename'][i], "was last downloaded at", dtime(national_catalog['filename'][i]))
    else:
        print(national_catalog['filename'][i], 'will be downloaded now.')
        request.urlretrieve(national_catalog['url'][i], national_catalog['filename'][i])

# update the national_catalog dataframe to reflect the most recent downloads
status = [None]*len(national_catalog)
# check the download status for each filename
for i,k in enumerate(national_catalog['filename']):
    if os.path.isfile(k):
        status[i] = True
        print(dtime(k))
    else:
        status[i] = False
# update the download status in national_catalog
if len(status) == len(national_catalog):
    national_catalog['downloaded'] = status
# save the catalog data
national_catalog['download_time'] = [dtime(pdf) for pdf in national_catalog['filename']]
national_catalog.to_csv('iv_catalog.csv', index = False)