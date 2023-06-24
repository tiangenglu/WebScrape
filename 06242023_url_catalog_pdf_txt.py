#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 24 10:58:16 2023

@author: Tiangeng Lu

Building data catalog from URLs using nonimmigrant visa page.

"""

import os
from PyPDF2 import PdfReader
from datetime import datetime
import time as tm
import pandas as pd
import requests
from scrapy import Selector
from pandas.tseries.offsets import MonthEnd
from urllib import request

# user-defined function(s)
def dtime(file):
    from datetime import datetime
    return datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d, %A, %H:%M:%S")


### SCENARIO 1: get URLs from web ####
main_url = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics/nonimmigrant-visa-statistics/monthly-nonimmigrant-visa-issuances.html'
all_links = Selector(text = requests.get(main_url).content).xpath('//*[contains(@href, "Class.pdf")]/@href').extract()
print(len(all_links))
links = [link for link in all_links if ("Nationality" in link or "nationality" in link or "Nationlity" in link)]

# Count the URLs in each year: 2017 should have 10 (from March), 2023 has 4 (up to April), and all other years should have 12
print("2017:",len([link for link in links if "2017" in link]))
print("2018:",len([link for link in links if "2018" in link]))
print("2019:",len([link for link in links if "2019" in link]))
print("2020:",len([link for link in links if "202020" in link])) # not a typo but by the actual links
print("2021:",len([link for link in links if "2021" in link]))
print("2022:",len([link for link in links if "2022" in link]))
print("2023:",len([link for link in links if "2023" in link]))

prefix = 'https://travel.state.gov'
links = [prefix + link for i,link in enumerate(links) if link.startswith('/content')]

file = open('niv_url.txt', 'w')
for link in links:
    file.write(link + '\n')
file.close()    

#### SCENARIO 2: existing url list ####
catalog = pd.read_csv('niv_url.txt', header = None).rename(columns = {0:'url'})
catalog['month'] = [link.split('/')[-1].split('%')[0].upper() for link in catalog['url']]
catalog['year'] = [link.split('/')[-1].split('%')[1][2:] for link in catalog['url']]
# check for irregular elements
print(set(catalog['month']))
print(set(catalog['year']))
# get irregular list of index with irregular year & month info
temp_list = [i for i, yr in enumerate(catalog['year']) if len(yr) != 4]
# manually fix the irregularities
catalog['year'].iloc[temp_list] = catalog['month'].iloc[temp_list].str[-4:]
catalog['month'].iloc[temp_list] = catalog['month'].iloc[temp_list].str[:-4]
catalog['month'].iloc[catalog[catalog['month']=='SEPT'].index] = 'SEPTEMBER'
# create timestamp
catalog['mmyy'] = catalog['year'].str.cat(catalog['month'], sep = '-')
catalog['mmyy'] = pd.to_datetime(catalog['mmyy'], infer_datetime_format = True, errors = 'ignore') + MonthEnd(0)
# remove hour/minute/second from date
catalog['mmyy'] = catalog['mmyy'].dt.date
# sort the catalog data by date and reset index
catalog = catalog.sort_values('mmyy', ascending = True).reset_index(drop = True)

# Create a folder to store all pdf files if the folder isn't there yet
path = 'niv'
path_Exist = os.path.exists(path)
if not path_Exist:
    os.makedirs(path)
print("Now, does the path exist?", os.path.exists(path))

# create a list of filenames
all_filenames = [None] * len(catalog)
for i in range(len(catalog)):
    all_filenames[i] = os.getcwd() + '/' + path + '/' + 'niv_' + str(catalog['mmyy'][i])[:10] + '.pdf'     
all_filenames
catalog['filename'] = all_filenames

status = [None]*len(catalog)
# check the download status for each filename
for i,k in enumerate(catalog['filename']):
    if os.path.isfile(k):
        status[i] = True
        print(dtime(k))
    else:
        status[i] = False

# add the download status in catalog
if len(status) == len(catalog):
    catalog['downloaded'] = status 

# if the file hasn't been downloaded yet, download it now
for i in range(len(catalog)):
    if catalog['downloaded'][i] == True:
        print(catalog['filename'][i], "was last downloaded at", dtime(catalog['filename'][i]))
    else:
        print(catalog['filename'][i], 'will be downloaded now.')
        request.urlretrieve(catalog['url'][i], catalog['filename'][i])

# update the catalog dataframe to reflect the most recent downloads
status = [None]*len(catalog)
# check the download status for each filename
for i,k in enumerate(catalog['filename']):
    if os.path.isfile(k):
        status[i] = True
        print(dtime(k))
    else:
        status[i] = False
# update the download status in catalog
if len(status) == len(catalog):
    catalog['downloaded'] = status
# save the catalog data
catalog['download_time'] = [dtime(pdf) for pdf in catalog['filename']]
catalog.to_csv('niv_catalog.csv', index = False)


#### txt ####
path_txt = 'nivtxt'
path_Exist = os.path.exists(path_txt)
if not path_Exist:
    os.makedirs(path_txt)
print("Now, does the path exist?", os.path.exists(path_txt))

# check whether the catalog dataframe exists
if 'catalog' in globals():
    print("Is the catalog data available? \nYes. The catalog data is already in the environment.")
else:
    print("Is the catalog data available? \nNo. Import the catalog data from disk. The 'mmyy' column has to be set to datetime.")
    catalog = pd.read_csv('niv_catalog.csv')
    catalog['mmyy'] = pd.to_datetime(catalog['mmyy']).dt.date

txtnames = ['niv_'+str(txt)+'.txt' for txt in catalog['mmyy']]

# convert every pdf file (including all pages within the pdf) to a txt
TEXT = [None] * len(catalog['filename'])
# outer loop: pdf files
for n, nth_pdf in enumerate(catalog['filename']):
    # create a reader object
    reader = PdfReader(nth_pdf)
    Pages = [None] * len(reader.pages)
    Text = [None] * len(reader.pages)
    # inner loop: pdf pages
    for i in range(len(reader.pages)):
        Pages[i] = reader.pages[i]
        Text[i] = Pages[i].extract_text()
    TEXT[n] = Text
# save all txt
# two layers of loops
for i, txt in enumerate(txtnames):
    file = open(path_txt + '/' + txt, 'w')
    for individual_page in TEXT[i]:
        file.write(individual_page + "\n")
    file.close()
print("The txt folder now has", str(len(os.listdir(path_txt))), "files as of", tm.strftime("%Y-%m-%d, %A, %H:%M"))
