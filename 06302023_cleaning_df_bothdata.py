#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 30 20:35:08 2023
Last script in the first half of 2023
Last revised on November 15, 2024
Most recent execution on November 15, 2024

@author: Tiangeng Lu

What are the requirements before running this program?
- NIV and IV .txt files are available in folders.

What's new in this program, compared with previous programs? 
- Revised from `06032023_scraped_to_df.py` that only deals with NIV.
- This program includes cleaning of both NIV and IV.
- This program fixes the excessive space in the initially-removed-then-restored rows.
- This program uses less loops and more list comprehentions.
- This program outputs the following:
    a) visa_alltime.csv
    b) countries.csv
"""

import os
import pandas as pd
from datetime import datetime

####################### NIV #################################

niv_pdf = 'niv'

if os.path.exists(niv_pdf):
    print("First document:",sorted(os.listdir(niv_pdf))[0])
    print("Last document:",sorted(os.listdir(niv_pdf))[-1])
else:
    print("No such directory.")

# folder
niv_pdf_folder = os.getcwd() + '/' + niv_pdf + "/"
# short name
niv_pdf_short = sorted(os.listdir(niv_pdf))
# full name
niv_pdf_full = [niv_pdf_folder + pdf for pdf in niv_pdf_short]

niv_txt = 'nivtxt'
if os.path.exists(niv_txt):
    print("First document:",sorted(os.listdir(niv_txt))[0])
    print("Last document:",sorted(os.listdir(niv_txt))[-1])
else:
    print("No txt folder available.")
# full txt path for all txt    
niv_txt_full = [os.getcwd() + '/' + niv_txt + '/' + txt for txt in sorted(os.listdir(niv_txt))]
# mac has an invisible file .DS_Store
niv_txt_full = [txt for txt in niv_txt_full if 'niv_' in txt]


### all txt files into one df
niv_convert_start = datetime.now().strftime("%Y-%m-%d, %H:%M:%S") 
print("The conversion from all .txt documents to one dataframe started at:", niv_convert_start)
#pd.read_csv(niv_txt_full[0], delimiter = "\t", header = None)
niv_DF_raw = [None] * len(niv_txt_full)
for n in range(len(niv_txt_full)):    
    df_file = pd.read_csv(niv_txt_full[n], delimiter = "\t", header = None)
    df_file = df_file.rename(columns = {0:'V'})
    df_file['V'] = df_file['V'].str.upper().apply(lambda x: x.strip())
    # extract the timestamp substring
    df_file['time'] = niv_txt_full[n].split('/')[-1].split('.')[0].split('_')[1]
    if df_file['V'].str.contains('GRAND TOTAL', case = False).any() == False:
        print(n)
    else:
        remove = df_file.index >= list(df_file[df_file['V'].str.contains('GRAND TOTAL', case = False)].index)[0]
        df_file = df_file.iloc[remove == False]
    niv_DF_raw[n] = df_file
niv_DF = pd.concat([df_raw for df_raw in niv_DF_raw]).reset_index(drop = True)
niv_convert_end = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
print("The conversion ended at:", niv_convert_end)

### CLEANING
niv_DF_archive = niv_DF.copy(deep = True)
niv_DF['V'] = niv_DF['V'].apply(lambda x: x.strip())
niv_DF = niv_DF[niv_DF['V'].str.len() > 1]
# don't forget to escape \
niv_headers = ['NONIMMIGRANT','NATIONALITY VISA','PAGE','\(FY', '\#SBU']
niv_DF_headers = niv_DF[niv_DF['V'].str.contains('|'.join(niv_headers))]
niv_removed_initial = list(niv_DF_headers.index)
niv_DF = niv_DF[~niv_DF.index.isin(niv_removed_initial)]


# split the column ['V'] for prelimiary output

niv_DF_final = pd.DataFrame()
niv_DF_final['nationality'] = [' '.join(row.split(' ')[:-2]).strip() for row in niv_DF['V']]
niv_DF_final['visa'] = [row.split(' ')[-2].strip() for row in niv_DF['V']]
niv_DF_final['issue'] = [row.split(' ')[-1].strip() for row in niv_DF['V']]
# wrap with list() to remove original index info
niv_DF_final['time'] = list(niv_DF['time'])

# verify that all issue rows are numbers
not_end_num = [row for row in niv_DF_final['issue'] if row[-1] not in [str(num) for num in list(range(0,10,1))]]
not_start_num = [row for row in niv_DF_final['issue'] if row[0] not in [str(num) for num in list(range(0,10,1))]]
if len(not_end_num) == 0 & len(not_start_num) == 0:
    print("The issue column looks good.")
else:
    print("There are problems in the issue column. Some of them are not numbers.")

# get country list
niv_countries = sorted(list(set(niv_DF_final['nationality'])))

## RESTORE ROWS that contain both data and header/footer
niv_restored_rows = []
for row in niv_DF_headers['V'] + ',' + niv_DF_headers['time']:
    # locate any matches
    if any([x in row for x in niv_countries]):
        print(row)
        niv_restored_rows.append(row)
# construct a new dataframe with restored rows, same structure that has nationality, visa, issue, and time
niv_restored_df = pd.DataFrame()
niv_restored_df['nationality'] = [' '.join(row.split('NONIMMIGRANT')[0].strip().split(' ')[:-2]) for row in niv_restored_rows]
niv_restored_df['visa'] = [row.split('NONIMMIGRANT')[0].strip().split(' ')[-2] for row in niv_restored_rows]
niv_restored_df['issue'] = [row.split('NONIMMIGRANT')[0].strip().split(' ')[-1] for row in niv_restored_rows]
niv_restored_df['time'] = [row.split(',')[-1].strip() for row in niv_restored_rows]
# concatenate the original and the restored rows
niv_DF_final = pd.concat([niv_DF_final, niv_restored_df],axis = 0).\
    drop_duplicates().sort_values(by = ['time','nationality','visa']).\
        reset_index(drop = True).rename(columns = {'issue':'count'})

if niv_DF_final['count'].dtype != "int":
    niv_DF_final['count'] = niv_DF_final['count'].str.replace(',', '')
    niv_DF_final['count'] = niv_DF_final['count'].astype('int')
print(niv_DF_final['count'].dtype)
assert niv_DF_final['count'].dtype == "int"

#### added on 08/02/2023, clean country names to reduce their variations
print('Before text cleaning, there are', str(len(set(niv_DF_final['nationality']))), 'country/region names.')
niv_DF_final['nationality'] = niv_DF_final['nationality'].str.replace('*','')
niv_DF_final['nationality'] = niv_DF_final['nationality'].str.replace(r'\s+',' ', regex = True)
niv_DF_final['nationality'] = niv_DF_final['nationality'].str.replace(' - ','-')
niv_DF_final['nationality'] = niv_DF_final['nationality'].str.replace(' – ','-')
niv_DF_final['nationality'] = niv_DF_final['nationality'].str.replace('- ','-')
niv_DF_final['nationality'] = niv_DF_final['nationality'].str.replace(' -','-')
print('After text cleaning, there are', str(len(set(niv_DF_final['nationality']))), 'country/region names.')
# save NIV locally
niv_DF_final.to_csv('niv_alltime.csv', index = False)

####################### IV ##################################
iv_pdf = 'iv'

if os.path.exists(iv_pdf):
    print("First document:",sorted(os.listdir(iv_pdf))[0])
    print("Last document:",sorted(os.listdir(iv_pdf))[-1])
else:
    print("No such directory.")

# folder
iv_pdf_folder = os.getcwd() + '/' + iv_pdf + "/"
# short name
iv_pdf_short = sorted(os.listdir(iv_pdf))
# full name
iv_pdf_full = [iv_pdf_folder + pdf for pdf in iv_pdf_short]

iv_txt = 'ivtxt'
if os.path.exists(iv_txt):
    print("First document:",sorted(os.listdir(iv_txt))[0])
    print("Last document:",sorted(os.listdir(iv_txt))[-1])
else:
    print("No txt folder available.")
# full txt path for all txt    
iv_txt_full = [os.getcwd() + '/' + iv_txt + '/' + txt for txt in sorted(os.listdir(iv_txt))]
# mac has an invisible file .DS_Store
iv_txt_full = [txt for txt in iv_txt_full if 'iv_' in txt]


### all txt files into one df
iv_convert_start = datetime.now().strftime("%Y-%m-%d, %H:%M:%S") 
print("The conversion from all .txt documents to one dataframe started at:", iv_convert_start)
#pd.read_csv(iv_txt_full[0], delimiter = "\t", header = None)
iv_DF_raw = [None] * len(iv_txt_full)
for n in range(len(iv_txt_full)):    
    df_file = pd.read_csv(iv_txt_full[n], delimiter = "\t", header = None)
    df_file = df_file.rename(columns = {0:'V'})
    df_file['V'] = df_file['V'].str.upper().apply(lambda x: x.strip())
    # extract the timestamp substring
    df_file['time'] = iv_txt_full[n].split('/')[-1].split('.')[0].split('_')[1]
    if df_file['V'].str.contains('GRAND TOTAL', case = False).any() == False:
        print(n)
    else:
        remove = df_file.index >= list(df_file[df_file['V'].str.contains('GRAND TOTAL', case = False)].index)[0]
        df_file = df_file.iloc[remove == False]
    iv_DF_raw[n] = df_file
iv_DF = pd.concat([df_raw for df_raw in iv_DF_raw]).reset_index(drop = True)
iv_convert_end = datetime.now().strftime("%Y-%m-%d, %H:%M:%S")
print("The conversion ended at:", iv_convert_end)

### CLEANING
iv_DF_archive = iv_DF.copy(deep = True)
iv_DF['V'] = iv_DF['V'].apply(lambda x: x.strip())
iv_DF = iv_DF[iv_DF['V'].str.len() > 1]
# don't forget to escape \
iv_headers = ['PAGE ', 'FOREIGN STATE OF', 'CHARGEABILITY', 'PLACE OF BIRTH', '\(FY 20', '\(FY20','IMMIGRANT VISA']
iv_DF_headers = iv_DF[iv_DF['V'].str.contains('|'.join(iv_headers))]
iv_removed_initial = list(iv_DF_headers.index)
iv_DF = iv_DF[~iv_DF.index.isin(iv_removed_initial)]


# split the column ['V'] for prelimiary output

iv_DF_final = pd.DataFrame()
iv_DF_final['nationality'] = [' '.join(row.split(' ')[:-2]).strip() for row in iv_DF['V']]
iv_DF_final['visa'] = [row.split(' ')[-2].strip() for row in iv_DF['V']]
iv_DF_final['issue'] = [row.split(' ')[-1].strip() for row in iv_DF['V']]
# wrap with list() to remove original index info
iv_DF_final['time'] = list(iv_DF['time'])

# verify that all issue rows are numbers
not_end_num = [row for row in iv_DF_final['issue'] if row[-1] not in [str(num) for num in list(range(0,10,1))]]
not_start_num = [row for row in iv_DF_final['issue'] if row[0] not in [str(num) for num in list(range(0,10,1))]]
if len(not_end_num) == 0 & len(not_start_num) == 0:
    print("The issue column looks good.")
else:
    print("There are problems in the issue column. Some of them are not numbers.")

# get country list
iv_countries = sorted(list(set(iv_DF_final['nationality'])))

## RESTORE ROWS that contain both data and header/footer
iv_restored_rows = []
for row in iv_DF_headers['V'] + ',' + iv_DF_headers['time']:
    if any([x in row for x in iv_countries]):
        print(row)
        iv_restored_rows.append(row)
# construct a new dataframe with restored rows, same structure that has nationality, visa, issue, and time
iv_restored_df = pd.DataFrame()

iv_restored_df['nationality'] = [' '.join(row.split('IMMIGRANT')[0].strip().split(' ')[:-2]) for row in iv_restored_rows]
iv_restored_df['visa'] = [row.split('IMMIGRANT')[0].strip().split(' ')[-2] for row in iv_restored_rows]
iv_restored_df['issue'] = [row.split('IMMIGRANT')[0].strip().split(' ')[-1] for row in iv_restored_rows]
iv_restored_df['time'] = [row.split(',')[-1].strip() for row in iv_restored_rows]
# concatenate the original and the restored rows
iv_DF_final = pd.concat([iv_DF_final, iv_restored_df],axis = 0).\
    drop_duplicates().sort_values(by = ['time','nationality','visa']).\
        rename(columns = {'issue':'count'})
iv_DF_final = iv_DF_final.reset_index(drop = True)

not_end_num = [row for row in iv_DF_final['count'] if row[-1] not in [str(num) for num in list(range(0,10,1))]]
not_start_num = [row for row in iv_DF_final['count'] if row[0] not in [str(num) for num in list(range(0,10,1))]]

if iv_DF_final['count'].dtype == 'object':
    iv_DF_final['count'] = iv_DF_final['count'].str.replace(',', '')
    iv_DF_final['count'] = iv_DF_final['count'].astype('int')
print(iv_DF_final['count'].dtype)
assert iv_DF_final['count'].dtype == "int"

# save IV locally
iv_DF_final.to_csv('iv_alltime.csv', index = False)

#### added 08/02/2023, reduce country/region name variations
print('Before text cleaning, there are', str(len(set(iv_DF_final['nationality']))), 'country/region names.')
iv_DF_final['nationality'] = iv_DF_final['nationality'].str.replace('*','')
iv_DF_final['nationality'] = iv_DF_final['nationality'].str.replace(r'\s+',' ', regex = True)
iv_DF_final['nationality'] = iv_DF_final['nationality'].str.replace(' - ','-')
iv_DF_final['nationality'] = iv_DF_final['nationality'].str.replace(' – ','-')
iv_DF_final['nationality'] = iv_DF_final['nationality'].str.replace('- ','-')
iv_DF_final['nationality'] = iv_DF_final['nationality'].str.replace(' -','-')
print('After text cleaning, there are', str(len(set(iv_DF_final['nationality']))), 'country/region names.')

########################## CONCATENATE IV & NIV, OUTPUT VISA_ALLTIME ############################
iv_DF_final['type'] = 'I'
niv_DF_final['type'] = 'N'
visa_alltime = pd.concat([iv_DF_final, niv_DF_final],axis = 0).sort_values(by = ['time','type','nationality','visa']).reset_index(drop = True)

#### added on 10/19/2024, edit on China, Hong Kong, Macau, Taiwan, United Kingdom
countries = sorted(list(set(visa_alltime['nationality']))) # 1st draft of countries series
china_list = [country for country in countries if "china".upper() and "mainland".upper() in country]
tw_list = [country for country in countries if "taiwan".upper() in country]
hk_list =[country for country in countries if "hong kong".upper() in country]
mc_list=[country for country in countries if "macau".upper() in country or "macao".upper() in country]
uk_list = [country for country in countries if "great britain".upper() in country]
need_rename_list = china_list + tw_list + hk_list + mc_list + uk_list
# must use ["CHINA"], not "CHINA". If doesn't wrap with [], returns the following:
# 'CHINACHINATAIWANTAIWANTAIWANMACAUMACAUMACAU'
renamed_list = ["CHINA"] * len(china_list) + \
    ["TAIWAN"] * len(tw_list) + ["HONG KONG"] * len(hk_list) + ["MACAU"] * len(mc_list) +\
        ["UNITED KINGDOM"] * len(uk_list)
rename_dict = dict(zip(need_rename_list, renamed_list))
# iv_DF_final['nationality'] = iv_DF_final['nationality'].map(rename_dict)
#for index, row in visa_alltime.iterrows():
#    if 'china'.upper() in row['nationality'] or 'taiwan'.upper() in row['nationality'] or 'hong kong'.upper() in row['nationality'] or 'macau'.upper() in row['nationality']:
        # AttributeError: 'str' object has no attribute 'map'
#        row['nationality'] = row['nationality'].map(rename_dict)
new_nationality = visa_alltime['nationality'].map(rename_dict).fillna(visa_alltime['nationality'])
visa_alltime['nationality'] = new_nationality
##### OUTPUT COUNTRY LIST #####
countries = sorted(list(set(visa_alltime['nationality']))) # update the countries series
pd.DataFrame(countries).rename(columns = {0: 'country'}).to_csv('countries.csv', index = False) 
visa_alltime.to_csv('visa_alltime.csv', index = False)
