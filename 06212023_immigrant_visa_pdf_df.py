#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun 20 2023

@author: Tiangeng Lu

Auto-download monthly immigrant visa statistics .pdf files with minimum loops.
This program checks the download status of the targeted files before downloading all files.
This program will only download the files that haven't been downloaded yet.

This program converts all pdfs to txts.
This program contains immigrant-visa-specific data cleaning steps.
This program converts all txts to a single dataframe.
This program saves monthly dataframes.
"""

import requests
from scrapy import Selector
import pandas as pd
from pandas.tseries.offsets import MonthEnd
import os
from urllib import request
from datetime import datetime
from PyPDF2 import PdfReader
import time as tm

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
########################################################

########################################################
iv_folder = 'ivtxt'
if not os.path.exists(iv_folder):
    os.makedirs(iv_folder)
print(dtime(iv_folder))
# check whether the catalog dataframe exists
if 'national_catalog' in globals():
    print("Yes. The catalog data is already here.")
else:
    print("No. Import the catalog data from disk.")
    national_catalog = pd.read_csv('iv_catalog.csv')

# convert every pdf file (including all pages within the pdf) to a txt
TEXT = [None] * len(national_catalog['filename'])
# outer loop: pdf files
for n, nth_pdf in enumerate(national_catalog['filename']):
    # create a reader object
    reader = PdfReader(nth_pdf)
    Pages = [None] * len(reader.pages)
    Text = [None] * len(reader.pages)
    # inner loop: pdf pages
    for i in range(len(reader.pages)):
        Pages[i] = reader.pages[i]
        Text[i] = Pages[i].extract_text()
    TEXT[n] = Text
# save txt to file
# can only concatenate str (not "datetime.date") to str
txtnames = ['iv_'+str(txt)+'.txt' for txt in national_catalog['mmyy']]
# two layers of loops
for i, txt in enumerate(txtnames):
    file = open(iv_folder + '/' + txt, 'w')
    for individual_page in TEXT[i]:
        file.write(individual_page + "\n")
    file.close()
print("The txt folder now has", str(len(os.listdir(iv_folder))), "files as of", tm.strftime("%Y-%m-%d, %A, %H:%M"))

# txt files to dataframe
# start with a raw container of dataframes
DF_raw = [None] * len(TEXT)
# the following is another nested loop
# OUTER: each txt converted from pdf document
for n in range(len(TEXT)):
    # the length of df_list gets updated in every iteration over n
    df_list = [None] * len(TEXT[n])
    # INNER: the ith page in the nth txt file
    for i, page in enumerate(TEXT[n]):
        # loop over each page
        df_list[i] = pd.DataFrame(page.split('\n'))
        # retain the page number from their original txt(pdf) file
        df_list[i]['pg'] = i + 1
        # end of INNER: pages in the nth txt file
    # OUTER: append the dataframe for every page to one bigger dataframe
    df_file = pd.concat([df_i for df_i in df_list]).reset_index(drop = True)
    df_file = df_file.rename(columns = {0:'V'})
    # set to upper case and trim
    df_file['V'] = df_file['V'].str.upper().apply(lambda x: x.strip())
    # add yyyy-mm timestamp column, both should work
    #df_file['time'] = txtnames[n][3:-4]
    df_file['time'] = national_catalog['mmyy'][n].strftime("%Y-%m-%d")
    if df_file['V'].str.contains('GRAND TOTAL', case = False).any() == False:
        # Are there any documents that do not have a grand total?
        print(n)
    else:
        # If there's a "Grand Total" in any of the pages, get the row/index ids that are on or after the "Grand Total"
        remove = df_file.index >= list(df_file[df_file['V'].str.contains('GRAND TOTAL', case = False)].index)[0]
        # exclude the rows from the `remove` row ids/indeces
        df_file = df_file.iloc[remove == False]
    # the finished df_file becomes the nth dataframe in the DF_raw list
    DF_raw[n] = df_file
# concatenate all data frames, the following has 6-digit observations, all contents in column 'V'
# yyyy-mm and page information were added
DF = pd.concat([df_raw for df_raw in DF_raw]).reset_index(drop = True)
# Save a back-up copy of DF
DF_archive = DF.copy(deep = True)

# immigrant-visa specific headers and footers, observe the pdf files
headers = ['PAGE ', 'FOREIGN STATE OF', 'CHARGEABILITY', 'PLACE OF BIRTH', '\(FY 20', '\(FY20']

# the following df are the headers/footers rows
DF_headers = DF[DF['V'].str.contains('|'.join(headers))]
# get the indeces/row ids
remove_ids = list(DF_headers.index)
# remove rows by remove_ids
DF = DF[~DF.index.isin(remove_ids)]
# remove blank rows in column 'V'
DF = DF[DF['V'].str.len() > 1]
# make sure all V values end with number
comingled_rows = DF[DF['V'].str.endswith('ISSUANCES')]
# remove the unwanted partial string that were in the same row with data entries
str_remove = 'IMMIGRANT VISA ISSUANCES'
DF['V'] = DF['V'].str.replace(str_remove, '')
DF['V'] = DF['V'].apply(lambda x: x.strip())
# the following should return nothing
for row in DF['V']:
    if row[-1] not in ['1','2','3','4','5','6','7','8','9','0']:
        print(row)

# Create new columns
visa = [None]*len(DF)
issue = [None]*len(DF)
nationality = [None]*len(DF)
for i, row in enumerate(DF['V']):
    issue[i] = row.split(' ')[-1]
    visa[i] = row.split(' ')[-2]   
    nationality[i] = ' '.join(row.split(' ')[:-2])

## CREATE A NEW DATAFRAME

DF_final = pd.DataFrame({
    'time': DF['time'],
    'pg': DF['pg'],
    'nationality': nationality,
    'visa': visa,
    'issuance':issue   
    })   

# country list
print(sorted(list(set(DF_final['nationality']))))
nationality_ct = DF_final['nationality'].value_counts().to_frame().reset_index().\
    rename(columns = {'index':'country','nationality':'count'}).\
        sort_values(['count','country'], ascending = True).reset_index(drop = True)
nationality_ct.to_csv('iv_nationalities.csv', index = False)

# see the visa type count, make sure to remove extra leading/trailing space
DF_final['visa'] = DF_final['visa'].apply(lambda x: x.strip())
visa_ct = DF_final['visa'].value_counts().to_frame().reset_index()
visa_ct = visa_ct.rename(columns = {'visa':'count','index':'type'})
visa_ct.to_csv('iv_visa.csv', index = False)

# remove the big mark "," from the number column
print(DF_final['issuance'].dtypes)
# the monthly issuances of immigrant visas are much lower than nonimmigrant visas
DF_final['issuance'] = DF_final['issuance'].str.replace(',', '')
# this should be blank
DF_final[DF_final['issuance'].str.contains('[A-Za-z]')]
# the following should equal to the number of rows in the final dataframe
print(len(DF_final[DF_final['issuance'].str.contains('[0-9]')]))
# convert to integer
DF_final['issuance'] = DF_final['issuance'].astype('int')
print(DF_final['issuance'].dtypes)

# save final dataframe
DF_final.to_csv('iv_alltime.csv', index = False)

# final Step, save separate dataframes for each month
release_date = sorted(list(set(DF_final['time'])))
print(release_date)
# create a new dataframe folder
path_df = 'ivdf'
if os.path.exists(path_df) == True:
    print("YES. The directory already exists. \nNO ACTIONS REQUIRED \n")
    print(sorted(os.listdir(path_df)))
else:
    print("The directory doesn't exist. Create it right now.")
    os.makedirs(path_df)
    print("Does the directory exist now?", os.path.exists(path_df))
# df names for each month
df_month_filenames = ['iv_'+str(name)+'.csv' for name in national_catalog['mmyy']]
# save dataframes of each month
for i in range(len(release_date)):
    DF_final.loc[DF_final['time'] == release_date[i]].to_csv(path_df + '/' + df_month_filenames[i], index = False)
