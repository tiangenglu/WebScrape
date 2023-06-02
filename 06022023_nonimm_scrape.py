#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 08:41:00 2023

@author: Tiangeng Lu
"""
import requests
from scrapy import Selector
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import os
from urllib import request

# URLs
main_url = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-statistics/nonimmigrant-visa-statistics/monthly-nonimmigrant-visa-issuances.html'
main_html = requests.get(main_url).content
main_selector = Selector(text = main_html)
all_links = main_selector.xpath('//*[contains(@href, "Class.pdf")]/@href').extract()
national_links = [link for link in all_links if ("Nationality" in link or "nationality" in link)]
# Count the URLs in each year: 2017 should has 10 (from March), 2023 has 4 (up to April), and all other years should have 12
print("2017:",len([link for link in national_links if "2017" in link]))
print("2018:",len([link for link in national_links if "2018" in link]))
print("2019:",len([link for link in national_links if "2019" in link]))
print("2020:",len([link for link in national_links if "202020" in link])) # not a typo but by the actual links
print("2021:",len([link for link in national_links if "2021" in link]))
print("2022:",len([link for link in national_links if "2022" in link]))
print("2023:",len([link for link in national_links if "2023" in link]))

# 2017 has one month missing, print out links & identify December is missing
# print out all links in 2017
print([link for link in national_links if "2017" in link])
# manually append the following link
# /content/dam/visas/Statistics/Non-Immigrant-Statistics/MonthlyNIVIssuances/DECEMBER%202017%20-%20NIV%20Issuances%20by%20Nationlity%20and%20Visa%20Class.pdf
national_links.append('/content/dam/visas/Statistics/Non-Immigrant-Statistics/MonthlyNIVIssuances/DECEMBER%202017%20-%20NIV%20Issuances%20by%20Nationlity%20and%20Visa%20Class.pdf')
national_links = list(set(national_links))
len(national_links)

prefix = 'https://travel.state.gov'
for i, link in enumerate(national_links):
    if link.startswith('/content'):
        national_links[i] = prefix + link
# save urls        
file = open('URL_nonimm_visa_issuances_nationality.txt','w')
for url in national_links:
    file.write(url + "\n")
file.close()    

# Build Catalog
national_links[0].split('/')[-1].split('%')

month = [None]*len(national_links)
year = [None]*len(national_links)
for i, link in enumerate(national_links):
    month[i] = link.split('/')[-1].split('%')[0].upper()
    year[i] = link.split('/')[-1].split('%')[1][2:]

print(set(month))
print(set(year))

# get a list of irregular elements
temp_list = []
for i,yr in enumerate(year):
    if len(yr)!=4:
        temp_list.append(i)
temp_list 

national_catalog = pd.DataFrame(
    {
     'url': national_links,
     'year': year,
     'month':month
     })

national_catalog['year'].iloc[temp_list] = national_catalog['month'].iloc[temp_list].str[-4:]
national_catalog['month'].iloc[temp_list] = national_catalog['month'].iloc[temp_list].str[:-4]
national_catalog['month'].iloc[national_catalog[national_catalog['month']=='SEPT'].index] = 'SEPTEMBER'

# create timestamp
national_catalog['mmyy'] = national_catalog['year'].str.cat(national_catalog['month'], sep = '-')
national_catalog['mmyy'] = pd.to_datetime(national_catalog['mmyy'], infer_datetime_format = True, errors = 'ignore') + MonthEnd(0)
# remove hour/minute/second from date
national_catalog['mmyy'] = national_catalog['mmyy'].dt.date
# sort the catalog data by date and reset index
national_catalog = national_catalog.sort_values('mmyy', ascending = True).reset_index(drop = True)

# Create a folder to store all pdf files if the folder isn't there yet
path = 'nonimm'
os.path.exists(path)

path_Exist = os.path.exists(path)
if not path_Exist:
    os.makedirs(path)

print("Now, does the path exist?", os.path.exists(path))

# Download all pdfs to the folder
for i in range(len(national_catalog)):
    fullfilename = path + '/' + 'nonimm_' + str(national_catalog['mmyy'][i])[:10] + '.pdf'
    request.urlretrieve(national_catalog['url'][i], fullfilename)

# Check the files
len(os.listdir(path))
sorted(os.listdir(path))
