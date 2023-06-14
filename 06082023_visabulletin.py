#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun  8 10:31:07 2023

@author: Tiangeng Lu

Today is a beautiful day!
This is the .py(spyder) version of the published notebook: 
    https://github.com/tiangenglu/WebScrape/blob/main/05292023_web_scrape.ipynb
The next step is: adding codes to update only the new webpage(s) so that this program won't have to re-run the most time-consuming portion.
"""

# import packages
import requests
from scrapy import Selector
import numpy as np
import pandas as pd

# urls
main_url = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-bulletin.html'
main_html = requests.get(main_url).content
main_selector = Selector(text = main_html)
# how many elements? 1886
len(main_selector.xpath('//*'))

# extract from selector
all_links = main_selector.xpath('//*[contains(@href,"visa-law0/visa-bulletin/20")]/@href').extract()
# as of 6/12/2023, visa bulletin for july-2023 is available
all_links[:5]
# full url
prefix = "https://travel.state.gov"
for i, link in enumerate(all_links):
    if link.startswith('/content'):
        all_links[i] = prefix + link
# save the url links
file = open('urls_visa_bulletin.txt','w')
for url in all_links:
    file.write(url + "\n")
file.close()

# retrieve time-stamp info from the urls
month = [None] * len(all_links)
year = [None] * len(all_links)
for i, link in enumerate(all_links):
    month[i] = link.split('/')[-1].split('-')[-2].upper()
    year[i] = link.split('/')[-1].split('-')[-1].split('.')[0]
# print out unique year and month results to check irregularities
print(set(year)); print(set(month))
# remove "bad" urls iteratively. create a new list to avoid changing the original url list
new_urls = list(all_links)
new_urls = [link for link in all_links if ('visa-availability' not in link and '2007/july-2007-visa-bulletin.html' not in link)]
new_urls = list(set(new_urls))
# 260 urls remained
len(new_urls)
# update the url links
file = open('urls_visa_bulletin.txt','w')
for url in new_urls:
    file.write(url + "\n")
file.close()

month = [None] * len(new_urls)
year = [None] * len(new_urls)
# loop over the list again
for i, link in enumerate(new_urls):
    # for each individual url, first split by `/`, work on the last element, then split by `-`
    month[i] = link.split('/')[-1].split('-')[-2].upper()
    year[i] = link.split('/')[-1].split('-')[-1].split('.')[0]
# keep working on the urls until all months and years were extracted. This approach is highly data-dependent
print(set(month)); print(set(year))
# convert to time-stamp data
mmyy = [None] * len(month) # specify the length of an empty list
for i in range(len(month)):
    mmyy[i] = month[i] + '_' + year[i]   
# this is how to convert string to data-time data
from datetime import datetime
mmyy_dt = [None] * len(mmyy)
time_stamp = [None] * len(mmyy)
for i in range(len(mmyy)):
    mmyy_dt[i] = datetime.strptime(mmyy[i],"%B_%Y")
    time_stamp[i] = mmyy_dt[i].strftime("%Y-%m-%d")
# bind the urls and time-stamp into one dataframe. This is the catalog data
url_time_df = pd.DataFrame({'url':new_urls, 'mmyy':mmyy, 'stamp':time_stamp})
# sort the dates ascendingly
url_time_df = url_time_df.sort_values(['stamp'], ascending=True)
url_time_df = url_time_df.reset_index(drop = True)
url_time_df.to_csv("visa_statistics_catalog.csv", index=False)
# count frequency of years and months
import collections
print(collections.Counter(year))

## TIME-CONSUMING ##
str_all = [None] * len(new_urls)
# locate and extract the table content from each url
for i in range(len(url_time_df)):
    html = requests.get(url_time_df['url'][i]).content
    # create a new Selector with text equals to the html created from above, 
    # not from the beginning of the program
    sel = Selector(text = html)
    str_all[i] = sel.xpath('//table').extract() 
# as of 6/12/23, len = 260. In future, run after this.
print(len(str_all))
# only include tables that contain the word "Employment"
tables = [None] * len(str_all)
for i in range(len(str_all)):
    tables[i] = [tab for tab in str_all[i] if "Employment" in tab]
# for each webpage, how many tables does it have? 1 or 2
table_len = [None] * len(str_all)
for i in range(len(str_all)):
    table_len[i] = len(tables[i])
print(table_len)   

employment_tab_url = pd.DataFrame({
    'length': table_len, 'table': tables
})

# update the catalog info
info_df = pd.concat([url_time_df, employment_tab_url], axis = 1)
info_df.to_csv("employment_tab_url.csv", index = False)
# make each table a separate row, or, the "long" dataframe
info_df_long = info_df.explode('table')[['url','stamp','table']]
# save the "long" raw table to file
info_df_long.to_csv("employment_tab_long.csv", index = False)
info_df_long = info_df_long.reset_index(drop = True)

# html to df
# If there're more than one Employment-Based table, keep the first one
info_df_short = info_df_long.drop_duplicates(subset='stamp', keep = 'first')
info_df_short = info_df_short.reset_index(drop = True)

## TIME-CONSUMING ##
DF_list = [None] * len(info_df_short)
for raw in info_df_short['table']:
    # .dropna(how = "any") is optional and specific given the properties of these dataframes
    # without the .dropna() statement, the executing time would reduce to half
    DF_list = [pd.read_html(raw, header=0)[0].dropna(how = "any") for raw in info_df_short['table']]
# Change column names. Note that we can't just change the name of the first column.
# We have to address all column names
for df in DF_list:
    df.columns = ["Employment-Based"] + list(df.columns[1:])

# check document size
import sys
# Create a duplicated list so that we always have an original version of the dataframe list
DF_list_archive = list(DF_list)
# 2136 bytes
print(sys.getsizeof(DF_list_archive))

# few tables have some of the column names appeared in the first data row. Move them to column names
# Get the indeces of dataframes with problematic column names
edit_index = []
for i,df in enumerate(DF_list):
    if df.iloc[0].str.contains("Chargeability", case = False).any() == True:
        print(str(i),df.iloc[0].str.contains("Chargeability", case = False).any())
        edit_index.append(i)
print(edit_index)    
# make first row as column names
for i in edit_index:
    DF_list[i].columns = DF_list[i].iloc[0]    
# drop first row if the first row is column names
for i in edit_index:
    if DF_list[i].iloc[0].str.contains("Chargeability", case = False).any() == True:
        DF_list[i] = DF_list[i].iloc[1:]   
# Certain column names are too long and prone to spelling/spacing variations.
# Assign a consistent spelling/spacing to it
for df in DF_list:
    # can only concatenate list (not "str") to list
    df.columns = ["Employment-Based"] + ["All_Chargeability_Except_Listed"] + list(df.columns[2:])
# Standardize column (country) names
# initiate an empty dictionary
counts_col = {}

# hierarchical(nested) loop over every column name in every dataframe in the list of dataframes
for df in DF_list:
    for col in df.columns:
        if col in counts_col.keys():
            counts_col[col] += 1
        else:
            counts_col[col] = 1
# print the resulting dictionary of column name counts
counts_col

# China
# Collect all variations of China
CHINA = []
for key in counts_col.keys():
    if "CHINA" in key.upper():
        CHINA.append(key)
CHINA
# the following dictionary is a rename from-to guideline     
rename1 = dict(zip(CHINA, ["CHINA-MAINLAND"]*len(CHINA)))
rename1

# Other country abbrs
rename2 = {
    'PHILLIPINES':'PHILIPPINES',
    'CH':'CHINA-MAINLAND',
    'IN':'INDIA',
    'ME':'MEXICO',
    'PH':'PHILIPPINES'
}
# update rename1 by appending items in rename2
rename1.update(rename2)
# rename1 is the final dictionary that includes information in rename2
rename1

# rename all dataframes using the dictionary guideline
for i in range(len(DF_list)):
    DF_list[i] = DF_list[i].rename(columns = rename1)   

# After standardizing the columns of of China, re-run the dictionary
counts_col = {}

# hierarchical(nested) loop over every column name in every dataframe in the list of dataframes
for df in DF_list:
    for col in df.columns:
        if col in counts_col.keys():
            counts_col[col] += 1
        else:
            counts_col[col] = 1
# print the resulting dictionary of column name counts
counts_col

# Add timestamp by using the info_df_short dataframe
for i in range(len(DF_list)):
    DF_list[i]['time'] = info_df_short['stamp'][i]

# Merge all dataframes into one
alldata = pd.concat([df for df in DF_list]).set_index(['time'])
# fill NA with 'C'
alldata = alldata.fillna('C')
alldata['time'] = alldata.index
alldata = alldata.reset_index(drop = True)

# make a copy of the merged dataframe
df_work = alldata.copy(deep = True)
# 1298983
sys.getsizeof(df_work)
# Create a country list whose column values are either 'C' or a date
country_list = ['All_Chargeability_Except_Listed', 'INDIA', 'MEXICO', 'PHILIPPINES',
       'CHINA-MAINLAND', 'DOMINICAN REPUBLIC',
       'EL SALVADOR  GUATEMALA  HONDURAS', 'VIETNAM']
# Check the length of in all cells in all countries
lens = []
for col in df_work[country_list]:
    for row in df_work[col]:
        length = len(row)
        lens.append(length)
dict(zip(*np.unique(lens, return_counts=True)))     

# get column index through list comprehension
country_list_index = [df_work.columns.get_loc(c) for c in country_list if c in df_work]
# these are the nth columns in which are country names
country_list_index
# convert the date format, to be consisent with the timestamp column
# nested loop over the i th row and the j th column
for i in range(len(df_work)):
    for j in country_list_index:
        if len(df_work.iloc[i,j]) == 7:
            df_work.iloc[i,j] = datetime.strptime(df_work.iloc[i,j], "%d%b%y").strftime("%Y-%m-%d")       

# create a finite list of values
eb_dict = dict(zip(*np.unique(df_work['Employment-Based'], return_counts=True)))
eb_rename_dict = {
    '5th Non-Regional\xa0Center (C5 and T5)':'5th Non-Regional Center',
    '5th Non-Regional Center (C5 and T5)':'5th Non-Regional Center',
    '5th Regional\xa0Center (I5 and R5)':'5th Non-Regional Center',
    '5th\xa0Non-Regional\xa0Center (C5 and T5)':'5th Non-Regional Center',
    '5th Pilot Progams':'5th Targeted Employment Areas',
    '5th Pilot Programs':'5th Targeted Employment Areas',
    '5th Regional Center (I5 and R5)':'5th Targeted Employment Areas',
    '5th Regional\xa0Center (I5 and R5)':'5th Targeted Employment Areas',
    '5th\xa0Regional\xa0Center (I5 and R5)':'5th Targeted Employment Areas',
    '5th Unreserved (C5, T5, and all others)':'5th Unreserved',
    '5th Unreserved (including C5, T5, I5, R5)':'5th Unreserved',
    '5th\xa0Unreserved (I5 and R5)':'5th Unreserved',
    '5th Set Aside: High Unemployment (10%)':'5th Set Aside',
    '5th Set Aside: Infrastructure (2%)':'5th Set Aside',
    '5th Set Aside: Rural (20%)':'5th Set Aside',
    'Iraqi & Afghani Translators':'Iraqi & Afghani Translators',
    'Other Worker':'Other Workers',
    'Other Workers':'Other Workers',
    'Other Workers*':'Other Workers',
    'Other\xa0Workers':'Other Workers',
    'Schedule A  Workers':'Schedule A Workers',
    'Schedule A Workers':'Schedule A Workers',
    'Schedule A\xa0Workers':'Schedule A Workers',
    'Schedule\xa0A\xa0Workers':'Schedule A Workers',
    'Certain Religious  Workers':'Certain Religious Workers',
    'Certain Religiuos  Workers':'Certain Religious Workers',
    'Certain Religious Workers':'Certain Religious Workers',
    'Employment-Based':'Employment-Based',
    '1st':'1st',
    '2nd':'2nd',
    '3rd':'3rd',
    '4th':'4th',
    '5th':'5th'
}

eb5_tar = []
for key in eb_dict.keys():
    if "TARGETED" in key.upper():
        eb5_tar.append(key)

eb_dict = dict(zip(eb5_tar, ["5th Targeted Employment Areas"]*len(eb5_tar)))
eb_rename_dict.update(eb_dict)
set(eb_rename_dict.values())


# Map new column from dictionary
df_work['category'] = df_work['Employment-Based'].map(eb_rename_dict)
# Examine the unmapped values, then modify the dictionary until all values are mapped
df_work.isna().sum()[df_work.isna().sum()>0]
# This identify the unmapped keys
print(df_work['Employment-Based'][df_work['category'].isnull()].value_counts())
print(df_work['category'].value_counts())
# output the final dataframe
df_work.to_csv("visa_bulletin_updated.csv", index=False)
