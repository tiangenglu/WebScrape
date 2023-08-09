#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug  5 00:31:41 2023

@author: Tiangeng Lu

Download the H-1b employer data hub files between FY09 and FY23
Download the H-2b data
Download LPR Yearbook Tables 8 to 11 Expanded: https://www.dhs.gov/immigration-statistics/readingroom/lpr/table_8_to_11_expanded
Download OIS LPRs by Citizenship and Major Classes of Admission: https://www.dhs.gov/immigration-statistics/readingroom/LPR/LPR-by-major-class-and-country
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

####################### Collect the H-2b Data, online .csv data have to be saved to drive. 
####################### Online tables can be cleaned upon download

h2b_url = 'https://www.uscis.gov/tools/reports-and-studies/h-2b-employer-data-hub/h-2b-employer-data-hub-files'
h2b_selector = Selector(text = requests.get(h2b_url).content)
h2b_links = h2b_selector.xpath('//*[contains(@href, ".csv")]/@href').extract()
h2b_names = ['h2b_20'+link.split('/')[-1].split('.')[0].split('%')[1][-2:] for link in h2b_links]
if not os.path.exists('h2b'): os.makedirs('h2b')
for i in range(len(h2b_links)):
    request.urlretrieve(h2b_links[i], 'h2b/'+ h2b_names[i] + '.csv')
    
####################### Collect LPR Yearbook Tables 8 to 11 Expanded #######################
####################### Three tabs in each EXCEL file. Summary Statistics only #############
lpr_url = 'https://www.dhs.gov/immigration-statistics/readingroom/lpr/table_8_to_11_expanded' 
lpr_selector = Selector(text = requests.get(lpr_url).content)
lpr_links = lpr_selector.xpath('//*[contains(@href, ".xlsx")]/@href').extract()
# Links are in different structures. The following string split is an ad-hoc solution.
lpr_names = ['lpr_'+link.split('fy')[1].split('_')[0] for link in lpr_links]
if not os.path.exists('lpr'): os.makedirs('lpr')
for i in range(len(lpr_links)):
    request.urlretrieve(lpr_links[i], 'lpr/'+lpr_names[i]+'.xlsx')
    
####### OIS LPRs by Citizenship and Major Classes of Admission
lpr_by_url = 'https://www.dhs.gov/immigration-statistics/readingroom/LPR/LPR-by-major-class-and-country'  
lpr_by_selector = Selector(text = requests.get(lpr_by_url).content)
lpr_by_links = lpr_by_selector.xpath('//*[contains(@href, ".xlsx")]/@href').extract()
# 'Lawful Permanent Residents (LPRs) by Citizenship and Major Class of Admission: FY2005 - FY2021',
# 'Derivative (Spouses and Children) Employment-Based LPRs by Citizenship and Major Class of Admission: FY2005 - FY2021'
lpr_by_names = lpr_by_selector.xpath('//*[contains(@data-sort-value, "FY2005 - FY2021")]/@data-sort-value').extract()
if not os.path.exists('lprs_cit_classes'): os.makedirs('lprs_cit_classes')
file = open('lprs_cit_classes/readme.txt','w')
for name in lpr_by_names:
    file.write(name + "\n")
file.close()    
# use simplified names instead of extracting information from web
for i in range(len(lpr_by_links)):
    request.urlretrieve(lpr_by_links[i], 'lprs_cit_classes/' + 'lpr_classes' + str(i) + '.xlsx')  