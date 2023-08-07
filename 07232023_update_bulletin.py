#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 23 16:22:33 2023

@author: Tiangeng Lu

- Updates existing visa bulletin
- The main webpage contains 200+ urls.
- Each url contains multiple tables.
- Scrape selected tables, append the existing data then save as 'visa_bulletin_alltime'
- Output catalog data 'bulletin_catalog.csv' for future use
"""
import os
from datetime import datetime
import datetime as dt
import pandas as pd
import requests
from scrapy import Selector
import time as tm
def dtime(file):
    return datetime.fromtimestamp(os.path.getmtime(file)).strftime("%Y-%m-%d, %A, %H:%M:%S")

# URLs
main_url = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-bulletin.html'
# type = str
main_selector = Selector(text = requests.get(main_url).content)
all_links = main_selector.xpath('//*[contains(@href,"visa-law0/visa-bulletin/20")]/@href').extract()

# full url
prefix = "https://travel.state.gov"
for i, link in enumerate(all_links):
    if link.startswith('/content'):
        all_links[i] = prefix + link

# retrieve time-stamp info from the urls
month = [None] * len(all_links)
year = [None] * len(all_links)
for i, link in enumerate(all_links):
    month[i] = link.split('/')[-1].split('-')[-2].upper()
    year[i] = link.split('/')[-1].split('-')[-1].split('.')[0]
# print out unique year and month results to check irregularities
print(set(year)); print(set(month))
# remove "bad" urls iteratively. create a new list to avoid changing the original url list
urls = list(all_links)
urls = [link for link in all_links if ('visa-availability' not in link and '2007/july-2007-visa-bulletin.html' not in link)]
urls = list(set(urls))

# read-in existing catalog
catalog = pd.read_csv('bulletin_catalog.csv').dropna()
# the following step is necessary
catalog['year'] = catalog['year'].astype(str)
print(catalog.info())

#catalog['month'] = [mmyy.split('_')[0] for mmyy in catalog['mmyy']]
#catalog['year'] = [mmyy.split('_')[-1] for mmyy in catalog['mmyy']]
#catalog[['url','year','month','stamp']].to_csv('bulletin_catalog.csv', index = False)
#catalog['stamp'] = pd.to_datetime(catalog['stamp'], format = "%m/%d/%y").dt.date

### The following are the new urls
new_urls = list(set(urls).difference(catalog['url']))

if len(new_urls) == 0:
    print("No updates needed.")
# NameError: name 'exit' is not defined
else:
    print("Needs update dataset from the following URL(s):")
    print(new_urls)
    pass
##### WHEN UPDATE IS NEEDED #####

# extract month and year from new url(s)
new_year = [link.split('/')[-1].split('.')[0].split('-')[-1] for link in new_urls]
new_month= [link.split('/')[-1].split('.')[0].split('-')[-2].upper() for link in new_urls]

# current year
current_year = str(dt.date.today().year)
next_year = str(dt.date.today().year + 1)

# check whether month and year info were correctly extracted
for item in new_month:
    if item in list(set(catalog['month'])):
        print("MONTH in existing month list")
    else:
        raise ValueError("Error in extracted MONTH")
for item in new_year:
    if item in list(set(catalog['year'])):
        print("YEAR in existing year list")
    elif item in current_year:
        print("YEAR is current year that is not yet in the catalog.")
    elif item in next_year:
        print("YEAR is next year that is not yet in the catalog.")
    else:
        raise ValueError("ERROR in extracted YEAR")
##### UPDATE CATALOG #####
    
df_new = pd.DataFrame({
    'url': new_urls,
    'year': new_year,
    'month': new_month})

#df_new['month'].str.cat(df_new['year'], sep = '/')
df_new['stamp'] = pd.to_datetime(df_new['month'].str.cat(df_new['year'], sep = '/'), errors = 'raise', format = "%B/%Y").dt.date

##### Scrape #####
all_tables = [None] * len(df_new)
for i in range(len(df_new)):
    html = requests.get(df_new['url'][i]).content
    sel = Selector(text = html)
    all_tables[i] = sel.xpath('//table').extract()
print(len(all_tables))

## employment-based
emp_tables = [None]*len(all_tables)
for i in range(len(all_tables)):
    emp_tables[i] = [tab for tab in all_tables[i] if "Employment" in tab]
# How many months need to be updated?
print(len(emp_tables))
# E.g., How many employment-based tables in the first url?
print(len(emp_tables[0]))

table_len = [None] * len(all_tables)
for i in range(len(all_tables)):
    table_len[i] = len(emp_tables[i])
print(table_len)   

employment_tab_url = pd.DataFrame({
    'length': table_len, 'table': emp_tables
})

info_df = pd.concat([df_new, employment_tab_url], axis = 1)
# make each table a separate row, or, the "long" dataframe
info_df_long = info_df.explode('table')[['url','stamp','table']]

# html to df
# If there're more than one Employment-Based table, keep the first one
info_df_short = info_df_long.drop_duplicates(subset='stamp', keep = 'first')
info_df_short = info_df_short.reset_index(drop = True)

##### Convert #####
DF_list = [None] * len(info_df_short)
for raw in info_df_short['table']:
    # .dropna(how = "any") is optional and specific given the properties of these dataframes
    # without the .dropna() statement, the executing time would reduce to half
    DF_list = [pd.read_html(raw, header=0)[0].dropna(how = "any") for raw in info_df_short['table']]
# Change column names. Note that we can't just change the name of the first column.
# We have to address all column names
for df in DF_list:
    df.columns = ["Employment-Based"] + list(df.columns[1:])

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
print(counts_col)

# China
# Collect all variations of China
CHINA = []
for key in counts_col.keys():
    if "CHINA" in key.upper():
        CHINA.append(key)
CHINA
# the following dictionary is a rename from-to guideline     
rename1 = dict(zip(CHINA, ["CHINA-MAINLAND"]*len(CHINA)))
print(rename1)

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
print("Updated column rename dictionary:")
print(rename1)

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
print(counts_col)

# Add timestamp by using the info_df_short dataframe
for i in range(len(DF_list)):
    DF_list[i]['time'] = info_df_short['stamp'][i]
    
# Merge all dataframes into one
current_data = pd.concat([df for df in DF_list]).set_index(['time'])
# fill NA with 'C'
current_data = current_data.fillna('C')
current_data['time'] = current_data.index
current_data = current_data.reset_index(drop = True)

###### NEW on 07/30/2023 #######
### add 'Preference' to new data rows
current_data['Employment-Based'] = current_data['Employment-Based'].str.replace('\xa0',' ')
current_data['Employment-Based'] = current_data['Employment-Based'].str.replace('-  ','')
current_data['Employment-Based'] = current_data['Employment-Based'].str.replace(r'\s+', ' ', regex=True)
current_data['Employment-Based'] = current_data['Employment-Based'].str.replace('*','')

eb_categories = {
    '1st':'E1',
    '2nd':'E2',
    '3rd':'E3',
    '4th':'E4',
    'Other Workers':'E3_U',
    'Other Worker':'E3_U',
    '5th':'E5'}
current_data['Preference'] = current_data['Employment-Based'].map(eb_categories)

for i in range(len(current_data)):
    if '5th ' in current_data['Employment-Based'][i]:
        current_data['Preference'][i] = 'E5'
    elif 'Targeted' in current_data['Employment-Based'][i]:
        current_data['Preference'][i] = 'E5'
    elif 'Religi' in current_data['Employment-Based'][i]:
        current_data['Preference'][i] = 'E4_R'
    elif 'Schedule A' in current_data['Employment-Based'][i]:
        current_data['Preference'][i] = 'A'
    elif 'Translator'in current_data['Employment-Based'][i]:
        current_data['Preference'][i] = 'SIV'
current_data['Preference'] = current_data['Preference'].fillna('Unknown')
print(current_data['Preference'].value_counts())

###### END of 07/30/2023 REVISIONS #######

# current country list
current_countries = [col for col in current_data.columns if col not in ['time', 'Employment-Based']]
# Get the column index
current_countries_index = [current_data.columns.get_loc(c) for c in current_countries]
print("Current country column indeces:")
print(current_countries_index)
## update the 08SEP15 date format to 2015-09-08
for i in range(len(current_data)):
    for j in current_countries_index:
        if len(current_data.iloc[i,j]) == 7:
            current_data.iloc[i,j] = datetime.strptime(current_data.iloc[i,j], "%d%b%y").strftime("%Y-%m-%d")

##### Append to existing data #####
# This needs additional work because columns are different in different releases

existing = pd.read_csv('visa_bulletin_alltime.csv')
existing['time'] = pd.to_datetime(existing['time']).dt.date
print(existing.info())
print("The existing data is up to date as of", str(existing['time'].max()))

## Q1: Are there columns, potentially new countries/regions? ##
if len(set(current_data.columns).difference(existing.columns)) == 0:
    print("No new columns in this scrape. Go ahead and append the new data.")
else:
    print("There're new columns. Pay attention to them before appending the new data.")
    print(set(current_data.columns).difference(existing.columns))

all_data = pd.concat([existing.loc[:, existing.columns != 'category'], current_data]).fillna('C').drop_duplicates()
all_data['time'] = pd.to_datetime(all_data['time']).dt.date
all_data = all_data.drop_duplicates().reset_index(drop = True)
print("Done with data preparation. Data were output at", tm.strftime("%Y-%m-%d, %A, %H:%M"))
print("Visa bulletin updated to", str(all_data['time'].max()))
print(all_data.shape)
all_data.to_csv('visa_bulletin_alltime.csv', index = False)

###### OUTPUT UPDATED CATALOG #######
catalog_updated = pd.concat([catalog, df_new], axis = 0).drop_duplicates().reset_index(drop = True)
catalog_updated.to_csv('bulletin_catalog.csv', index = False)