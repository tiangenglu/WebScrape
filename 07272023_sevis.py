#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 21:08:18 2023
@author: Tiangeng Lu
This script takes longer to execute (20 minutes +), particularly the `pd.read_html()` part for STEM students.
Another personal note: I'm saddened to know that my neighbor Cynthia passed away a few weeks ago due to a tragic car accident. My deepest condolences to her family.
"""

import requests
from scrapy import Selector
import numpy as np
import pandas as pd
import os
from datetime import datetime
from pandas.tseries.offsets import MonthEnd
import datetime as dt
os.chdir('/Users/tiangeng/Documents/Python Files')

# urls
main_url = 'https://studyinthestates.dhs.gov/sevis-by-the-numbers/sevis-by-the-numbers-data'
main_html = requests.get(main_url).content
main_selector = Selector(text = main_html)
# how many elements? 266 as of 07/27/2023
len(main_selector.xpath('//*'))

# <a data-entity-substitution="canonical" data-entity-type="node" data-entity-uuid="98b24128-4146-433f-9288-77fad251a802" href="/sevis-data-mapping-tool/march-2023-stem-sevis-data-mapping-tool-data">March 2023 STEM SEVIS Data Mapping Tool Data</a>
# extract from selector
all_links = main_selector.xpath('//*[contains(@href,"sevis-data-mapping-tool-data")]/@href').extract()
# urls to stem students
stem_links = [link for link in all_links if '-stem-' in link]
# urls to overall data
sevis = [link for link in all_links if '-stem-' not in link]

# https://studyinthestates.dhs.gov/sevis-data-mapping-tool/march-2023-stem-sevis-data-mapping-tool-data
prefix = "https://studyinthestates.dhs.gov"
sevis = [prefix + link for link in sevis if link.startswith('/sevis')]
sevis = list(set(sevis))
stem_links = [prefix + link for link in stem_links if link.startswith('/sevis')]

############### CATALOG ################

sevis_catalog = pd.DataFrame(data = {
    'url': sevis,
    'year': [link.split('/')[4].split('-')[1].upper() for link in sevis],
    'month': [link.split('/')[4].split('-')[0].upper() for link in sevis]})
sevis_catalog['stamp'] = (sevis_catalog['month'].str.cat(sevis_catalog['year'], sep = '-'))
sevis_catalog['stamp'] = [datetime.strptime(stamp, "%B-%Y").strftime("%Y-%m-%d") for stamp in sevis_catalog['stamp']]
sevis_catalog['stamp'] = (pd.to_datetime(sevis_catalog['stamp'], errors = 'ignore') + MonthEnd()).dt.date
sevis_catalog = sevis_catalog.sort_values('stamp', ascending = True)
sevis_catalog = sevis_catalog.drop_duplicates()
sevis_catalog[['url','year','month','stamp']].to_csv('sevis_catalog.csv', index = False)
stem_catalog = pd.DataFrame(data = {
    'url': stem_links,
    'year':[link.split('/')[4].split('-')[1].upper() for link in stem_links],
    'month': [link.split('/')[4].split('-')[0].upper() for link in stem_links]})
stem_catalog['stamp'] = (stem_catalog['month'].str.cat(stem_catalog['year'], sep = '-'))
stem_catalog['stamp'] = [datetime.strptime(stamp, "%B-%Y").strftime("%Y-%m-%d") for stamp in stem_catalog['stamp']]
stem_catalog['stamp'] = (pd.to_datetime(stem_catalog['stamp'], errors = 'ignore') + MonthEnd()).dt.date
stem_catalog = stem_catalog.sort_values('stamp', ascending = True)

from collections import Counter
sevis_counter = Counter(sevis_catalog['year'])
print(sorted(sevis_counter.items()))
stem_counter = Counter(stem_catalog['year'])
print(sorted(stem_counter.items()))
stem_catalog[['url','year','month','stamp']].to_csv('stem_catalog.csv', index = False)
################ EXTRACTING WEB TABLES ####################
sevis_elements = [None] * len(sevis_catalog)

for i in range(len(sevis_catalog)):
    html = requests.get(sevis_catalog['url'][i]).content
    sel = Selector(text = html)
    sevis_elements[i] = sel.xpath('//table').extract() 
print("Are there any SEVIS webpages have more than 1 tables?\n",[len(element) for element in sevis_elements if len(element) > 1])

# "unlist" the list of one table. `type(sevis_elements[0]` is a "list",)
sevis_catalog['element'] = [element[0] for element in sevis_elements]

stem_elements = [None] * len(stem_catalog)

for i in range(len(stem_catalog)):
    html = requests.get(stem_catalog['url'][i]).content
    sel = Selector(text = html)
    stem_elements[i] = sel.xpath('//table').extract() 
print("Are there any STEM webpages have more than 1 tables?\n",[len(element) for element in stem_elements if len(element) > 1])
# The webpage of STEM July 2018 is blank as of 07/27/2023. I found this out after seeing an "out of range" error message.
[len(element) for element in stem_elements]
# which webpage doesn't have tables?
blank_page_id = [i for i, element in enumerate(stem_elements) if len(element) < 1]
if len(blank_page_id) == 0:
    print("Looks good. Go ahead clean the tables!")
else:
    print("ALERT! Skip the following because it's blank:\n", str(blank_page_id))
    print("Also, remember to update the catalog!")
# select data row(s) to remove
stem_catalog = stem_catalog.drop(blank_page_id, axis = 0)
stem_catalog = stem_catalog.reset_index()
# update the elements in stem_elements by removing the blank one
stem_elements = [element for element in stem_elements if len(element) > 0]
print(len(stem_elements))
##################### HTML to DATA (SEVIS) ###########################

#mar2023 = pd.read_html(sevis_elements[0][-1], header = 0)[0]
SEVIS_dfs = [None] * len(sevis_catalog)
for element in sevis_elements:
    SEVIS_dfs = [pd.read_html(element[0], header = 0)[0] for element in sevis_elements]

for i in range(len(SEVIS_dfs)):
    SEVIS_dfs[i].columns = SEVIS_dfs[i].columns.str.upper()

counts_col = {}
for df in SEVIS_dfs:
    for col in df.columns:
        if col in counts_col.keys():
            counts_col[col] += 1
        else:
            counts_col[col] = 1
# print the resulting dictionary of column name counts
print(counts_col)

rename_dict = {
    'COUNTRY OF CITIZENSHIP': 'CITIZENSHIP',
    '# OF ACTIVE STUDENTS': 'ACTIVE_STUDENTS',
    'BACHELORS':'BACHELOR',
    "BACHELOR'S":'BACHELOR',
    "MASTER'S": "MASTER",
    'MASTERS':"MASTER",
    'FLIGHT TRAINING': "FLIGHT",
    'FLIGHT_SCHOOL':'FLIGHT',
    'HIGH SCHOOL': 'HIGH_SCHOOL',
    'LANGUAGE TRAINING':'LANGUAGE_TRAINING',
    'OTHER VOCATIONAL SCHOOL':'OTHER_VOCATIONAL_SCHOOL'}
### AFTER STANDARDIZING COLUMN NAMES
for i in range(len(SEVIS_dfs)):
    SEVIS_dfs[i] = SEVIS_dfs[i].rename(columns = rename_dict)
counts_col = {}
for df in SEVIS_dfs:
    for col in df.columns:
        if col in counts_col.keys():
            counts_col[col] += 1
        else:
            counts_col[col] = 1
# print the resulting dictionary of column name counts
print(counts_col)

# Add timestamp by using the info_df_short dataframe
for i in range(len(SEVIS_dfs)):
    SEVIS_dfs[i]['time'] = sevis_catalog['stamp'][i]
    SEVIS_dfs[i]['year'] = sevis_catalog['year'][i]
    SEVIS_dfs[i]['month'] = sevis_catalog['month'][i]
#### OUTPUT SEPARATE CSV ####
sevis_csv_names = ["sevis_"+ stamp + '.csv' for stamp in sevis_catalog['stamp'].astype(str)]
if not os.path.exists('sevis'):
    os.makedirs('sevis')    
for i in range(len(SEVIS_dfs)):
    SEVIS_dfs[i].drop(['time','year','month'], axis = 1).to_csv('sevis/'+sevis_csv_names[i], index = False)

SEVIS_all = pd.concat([df for df in SEVIS_dfs]).set_index(['year','month'])
total_active = pd.pivot_table(SEVIS_all, values = 'ACTIVE_STUDENTS', index = 'time', aggfunc = 'sum').reset_index()
#### OUTPUT EXCEL ####
with pd.ExcelWriter('sevis.xlsx') as writer:
    SEVIS_all.to_excel(writer, sheet_name = 'detail', freeze_panes = (1,2))
    total_active.to_excel(writer, sheet_name = 'total', freeze_panes = (1,0), index = False)
##################### HTML to DATA (SEVIS-STEM) to be continued ###########################
STEM_dfs = [None] * len(stem_catalog)
for element in stem_elements:
    STEM_dfs = [pd.read_html(element[0], header = 0)[0] for element in stem_elements]

for i in range(len(STEM_dfs)):
    # if repeatedly executed, remove the 'TIME' column
    #STEM_dfs[i] = STEM_dfs[i].drop('TIME', axis = 1)
    STEM_dfs[i]['TIME'] = stem_catalog['stamp'][i]
    STEM_dfs[i].columns = STEM_dfs[i].columns.str.upper()

counts_col = {}
for df in STEM_dfs:
    for col in df.columns:
        if col in counts_col.keys():
            counts_col[col] += 1
        else:
            counts_col[col] = 1
# print the resulting dictionary of column name counts
print(counts_col)

# knowing the stem data is HUGE, print shape
for i in range(len(STEM_dfs)):
    print(STEM_dfs[i].columns)
    print(STEM_dfs[i].shape)

stem_rename_dict = {
    'ABBREVIATION': 'ST',
    'ABBRV': 'ST',
    'EDUCATION LEVEL': 'LEVEL',
    'STUDENT EDUCATION LEVEL': 'LEVEL',
    'COUNTRY OF CITIZENSHIP': 'CITIZENSHIP',
    'COC': 'CITIZENSHIP',
    'ACTIVE NUMBER OF STUDENTS':'COUNT',
    'COUNT OF ACTIVE STEM STUDENTS':'COUNT'
    }
for i in range(len(STEM_dfs)):
    STEM_dfs[i] = STEM_dfs[i].rename(columns = stem_rename_dict)
# There's one webpage that contains a table with a blank column. This case it's easy to fix--just select the useful columns
    STEM_dfs[i] = (STEM_dfs[i])[['STATE', 'ST', 'LEVEL', 'CITIZENSHIP', 'GENDER', 'COUNT', 'TIME']]
############# FINALLY, OUTPUT CSV ##################
stem_csv_names = ['stem_' + str(stamp) + '.csv' for stamp in stem_catalog['stamp']]
if not os.path.exists('sevis_stem'):
    os.makedirs('sevis_stem')
for i in range(len(STEM_dfs)):
    STEM_dfs[i].to_csv('sevis_stem/' + stem_csv_names[i], index = False)

############ STEM SUMMARY STATISTICS ##############
stem_summary = [None] * len(STEM_dfs)
# aggregate by State, level of education, and gender
for i in range(len(STEM_dfs)):
    stem_summary[i] = pd.pivot_table(STEM_dfs[i], values = 'COUNT', index = ['TIME','ST','LEVEL','GENDER'], aggfunc = 'sum')
stem_sums = pd.concat([df for df in stem_summary])
print(stem_sums.shape)
print(stem_sums.index)

stem_national = [None] * len(STEM_dfs)
for i in range(len(STEM_dfs)):
    stem_national[i] = pd.pivot_table(STEM_dfs[i], values = 'COUNT', index = ['TIME','LEVEL'], aggfunc = 'sum')
stem_national_all = pd.concat([df for df in stem_national])

for df in STEM_dfs:
    totals= pd.concat([pd.pivot_table(df, values = 'COUNT', index = 'TIME', aggfunc = 'sum') for df in STEM_dfs])
totals = totals.reset_index(drop = False)
## State totals by time
for df in STEM_dfs:
    states_time = pd.concat([pd.pivot_table(df, values = 'COUNT', index = ['ST','TIME'], aggfunc = 'sum').reset_index(drop = False) for df in STEM_dfs])
states_time = states_time.sort_values(['ST','TIME'], ascending = True)
states_time['ST'] = states_time['ST'].astype('category')
states_time = states_time.set_index('ST')
########## OUTPUT STEM SUMMARY TO EXCEL #############
with pd.ExcelWriter('sevis_stem.xlsx') as writer:
    totals.to_excel(writer, sheet_name = 'total', index = False)
    stem_national_all.to_excel(writer, sheet_name = 'edu_level', freeze_panes = (1,2))
    states_time.to_excel(writer, sheet_name = 'state_time', freeze_panes = (1,1), merge_cells = True)
    stem_sums.to_excel(writer, sheet_name = 'detail', freeze_panes = (1,4))
