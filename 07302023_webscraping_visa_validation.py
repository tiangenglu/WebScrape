#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul 30 22:06:45 2023

@author: Tiangeng Lu

Comparing total visa issuances between the Department of State published total and scraped total.
"""

import os
import pandas as pd
from datetime import datetime
# Get the paths and filenames of the immigrant and nonimmigrant files.
# I need the "grand total" row from the .txt files.
iv_txts = [os.getcwd() + '/' + 'ivtxt' + '/' + txt for txt in sorted(os.listdir('ivtxt')) if 'iv_' in txt]
print("The immigrant visa folder has", str(len(iv_txts)),\
      "files.\n","The most recent file is: ", str(iv_txts[-1].split('/')[-1]))
niv_txts = [os.getcwd() + '/' + 'nivtxt' + '/' + txt for txt in sorted(os.listdir('nivtxt')) if 'niv_' in txt]
print("The non-immigrant visa folder has", str(len(niv_txts)),\
      "files.\n","The most recent file is: ", str(niv_txts[-1].split('/')[-1]))

############ IMMIGRANT VISA TOTALS FROM TXT ############
iv_totals = [None] * len(iv_txts)
# I could read all pages and rows of the .txt files. But I'd like to try something more efficient.
# Read in only the "grand total" row from thousands of rows.

iv_read_begin = datetime.now().strftime("%H:%M:%S")
print("Immigrant visa GRAND TOTAL row scan begins at:",iv_read_begin)

for n,k in enumerate(iv_txts):
    txt_file = open(iv_txts[n], "r")
    lines_as_list = list(txt_file.readlines())
    txt_file.close()
    iv_totals[n] = [row.strip().upper() for row in lines_as_list if 'GRAND TOTAL' in row.upper()][0]

iv_read_finish = datetime.now().strftime("%H:%M:%S")
print("Immigrant visa GRAND TOTAL row scan finishes at:",iv_read_finish)

df_iv_totals = pd.DataFrame(data = {
    'time': [txt.split('/')[-1].split('_')[-1].split('.')[0] for txt in iv_txts],
    'publish_total': [tot.split(' ')[-1].replace(',','') for tot in iv_totals]})
df_iv_totals['publish_total'] = df_iv_totals['publish_total'].astype('int')   

### IMPORT SCRAPED IMMIGRANT MICRO DATA ###
iv_micro = pd.read_csv('iv_alltime.csv')
iv_sum_totals = iv_micro.pivot_table(values = 'count', index = 'time', aggfunc = 'sum').\
    reset_index().rename(columns = {'count':'scraped_total'})

## Compare two datasets
iv_compare = df_iv_totals.merge(iv_sum_totals, left_on = 'time', right_on = 'time', how = 'left')
iv_compare['diff'] = iv_compare['publish_total'] - iv_compare['scraped_total']
# The 'diff' column should be all 0s.
    
if len((iv_compare['time'])[iv_compare['diff'] != 0]) == 0:
    print("All totals of immigrant visa issuance match.")
else:
    print("Not all immigrant totals match. Numbers in the following months don't match.")
    print((iv_compare['time'])[iv_compare['diff'] != 0])
    
########### NONIMMIGRANT VISA TOTALS FROM TXT ###################
niv_totals = [None] * len(niv_txts)
niv_read_begin = datetime.now().strftime("%H:%M:%S")
print("Nonimmigrant visa GRAND TOTAL row scan begins at:",niv_read_begin)

for n,k in enumerate(niv_txts):
    txt_file = open(niv_txts[n], "r")
    lines_as_list = list(txt_file.readlines())
    txt_file.close()
    niv_totals[n] = [row.strip().upper() for row in lines_as_list if 'GRAND TOTAL' in row.upper()][0]

niv_read_finish = datetime.now().strftime("%H:%M:%S")
print("Nonimmigrant visa GRAND TOTAL row scan finishes at:",niv_read_finish)

df_niv_totals = pd.DataFrame(data = {
    'time': [txt.split('/')[-1].split('_')[-1].split('.')[0] for txt in niv_txts],
    'publish_total': [tot.split(' ')[-1].replace(',','') for tot in niv_totals]})
df_niv_totals['publish_total'] = df_niv_totals['publish_total'].astype('int')   

### IMPORT SCRAPED NONIMMIGRANT MICRO DATA ###
niv_micro = pd.read_csv('niv_alltime.csv')
niv_micro = niv_micro.drop_duplicates()
niv_sum_totals = niv_micro.pivot_table(values = 'count', index = 'time', aggfunc = 'sum').\
    reset_index().rename(columns = {'count':'scraped_total'})
## Compare two datasets
niv_compare = df_niv_totals.merge(niv_sum_totals, left_on = 'time', right_on = 'time', how = 'left')
niv_compare['diff'] = niv_compare['publish_total'] - niv_compare['scraped_total']

if len((niv_compare['time'])[niv_compare['diff'] != 0]) == 0:
    print("All totals of nonimmigrant visa issuance match.")
else:
    print("Not all nonimmigrant totals match. Numbers in the following months don't match.")
    print(len((niv_compare['time'])[niv_compare['diff'] != 0]))
    print((niv_compare['time'])[niv_compare['diff'] != 0])

############# OUTPUT REVIEW/VALIDATION RESULTS ################
with pd.ExcelWriter('visa_statistics_compare.xlsx') as writer:
    iv_compare.to_excel(writer, sheet_name = 'immigrant', index = False)
    niv_compare.to_excel(writer, sheet_name = 'nonimmigrant', index = False)