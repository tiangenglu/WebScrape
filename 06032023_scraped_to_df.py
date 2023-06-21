#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun  3 08:37:38 2023

@author: Tiangeng Lu

Scraped pdf(s)-->txt(s)-->dataframe-->dataframes, SIMPLIFIED CODES
Including iterative cleaning steps designed for this specific dataset
"""

import os
from PyPDF2 import PdfReader
import pandas as pd

### STEP 1: PDF 
# check the existence of directory
if os.path.exists('nonimm') == True:
    print(sorted(os.listdir('nonimm'))[:5],'\n...\n', sorted(os.listdir('nonimm'))[-5:])
else:
    print("The directory doesn't exist.")
    
# read-in all .pdf files from directory
fullpath = os.getcwd()+'/'+'nonimm'+'/'
# short_names are the pdf file names
short_names = sorted(os.listdir('nonimm'))
# full names are the fullpath
fullnames = [None] * len(short_names)
for i, pdf in enumerate(short_names):
    fullnames[i] = fullpath + pdf

### STEP 2: CREATE .TXT

# convert every individual .pdf to a corresponding .txt
TEXT = [None] * len(fullnames)
# THIS TAKES MINUTES TO RUN
# The following is a nested loop, the OUTER loops over all pdf files
for n, nth_pdf in enumerate(fullnames):
    # create a pdf-reader project
    reader = PdfReader(nth_pdf)
    Pages = [None] * len(reader.pages)
    Text = [None] * len(reader.pages)
    # the INNER loops over every page within the nth_pdf
    for i in range(len(reader.pages)):
        Pages[i] = reader.pages[i]
        Text[i] = Pages[i].extract_text()
    TEXT[n] = Text

### STEP 3: SAVE .txt files

if os.path.exists('nonimmtxt') == True:
    print("YES. The directory already exists.\n NO ACTIONS REQUIRED.")
    print(sorted(os.listdir('nonimmtxt'))[:5],"\n...\n...", sorted(os.listdir('nonimmtxt'))[-5:])
else:
    print("The directory doesn't exist. Create it right now.")
    os.makedirs('nonimmtxt')
    print("Does the directory exist now?", os.path.exists('nonimmtxt'))

# create folder + names for the .txt files   
txtnames = [None]*len(fullnames)   
for i,txt in enumerate(short_names):
    # DON'T add '/' at the beginning
    txtnames[i] = 'nonimmtxt' +'/'+ txt[:-4] + '.txt'    
# One loop works because the txtnames and the TEXT have the same length
for i,txt in enumerate(txtnames):
    file = open(txt, 'w')
    for page in TEXT[i]:
        file.write(page + "\n")
    file.close()    
# Confirm that .txt documents are already in the directory
len(os.listdir('nonimmtxt'))
# DONE with SAVING .txt files   

### STEP 4: .TXT TO DATAFRAME

DF_raw = [None] * len(TEXT)    
# OUTER loop for the each .txt(pdf) document scraped from webpage(s)
for n in range(len(TEXT)):
    # INNER loop for each page within one .txt(converted from pdf, page info retained from pdf) document
    df_list = [None] * len(TEXT[n])
    for i, page in enumerate(TEXT[n]):
        # work on each page
        df_list[i] = pd.DataFrame(page.split('\n'))
        df_list[i]['pg'] = i + 1
        ### END OF INNER LOOP ABOUT i and pages within one file
    # BACK to OUTER LOOP
    # Append the data frame for every page to one bigger dataframe via generator
    df_file = pd.concat([df_i for df_i in df_list]).reset_index(drop = True)
    df_file = df_file.rename(columns = {0:'V'})
    # Use this syntax to set to upper case and trim
    df_file['V'] = df_file['V'].str.upper().apply(lambda x: x.strip())
    df_file['time'] = short_names[n][7:-4]
    if df_file['V'].str.contains('GRAND TOTAL', case = False).any() == False:
        print(n)
    else:
        remove = df_file.index >= list(df_file[df_file['V'].str.contains('GRAND TOTAL', case = False)].index)[0]
        df_file = df_file.iloc[remove == False]
    DF_raw[n] = df_file
DF = pd.concat([df_raw for df_raw in DF_raw]).reset_index(drop = True)
# Save a back-up copy of DF
DF_archive = DF.copy(deep = True)
# Get row indeces for rows of headers, footnotes, along with other non-entry rows  
headers = DF[(DF['V'].str.startswith('NONIMMIGRANT')) | (DF['V'].str.startswith('NATIONALITY')) | (DF['V'].str.startswith('PAGE'))].index
notes = DF[DF['V'].str.contains('\(FY 2', case = False) | DF['V'].str.contains('\#SBU ', case = False)].index
remove_ids = list(set(list(headers) + list(notes)))

# KEEP A COPY OF REMOVED ROWS, JUST IN CASE..
# THIS IS THE FIRST VERSION OF IT
rows_rm = DF_archive['V'][DF_archive.index.isin(remove_ids)].value_counts().to_frame().reset_index(drop = False)
rows_rm = rows_rm.rename(columns = {'index':'rows'})
rows_rm = rows_rm.sort_values('rows', ascending = True).reset_index(drop = True)
# import the country and visa list first:
visas = pd.read_csv('nonimm_visa.csv')
countries = pd.read_csv('nonimm_nationalities.csv')
# create country and visa list
rows_rm['token'] = rows_rm['rows'].str.split(' ')
restore = []
for i in range(len(rows_rm['token'])):
    # If elements in tokens exist in country list
    if set(rows_rm['token'][i]) & set(countries.country.tolist()):
        restore.append(rows_rm['rows'][i])
        print(rows_rm['rows'][i])

restore_ids = []        
for i in range(len(restore)):
    restore_ids.append(list(DF_archive[DF_archive['V'] == restore[i]].index))
# release a nested list using a list comprehension     
restore_ids = [item for sublist in restore_ids for item in sublist]
remove_ids=list(set(remove_ids) - set(restore_ids))
# common elements in both lists
set(restore_ids) & set(remove_ids)

# Remove the rows from the dataframe
DF = DF[~DF.index.isin(remove_ids)]
# Compare the differences of rows before & after
print(DF_archive.shape[0])
print(DF.shape[0])
# update the rows_rm dataframe, NOW SAVE FOR FUTURE REFERENCE
rows_rm = DF_archive['V'][DF_archive.index.isin(remove_ids)].value_counts().to_frame().reset_index(drop = False)
rows_rm = rows_rm.rename(columns = {'index':'rows'})
rows_rm = rows_rm.sort_values('rows', ascending = True).reset_index(drop = True)
rows_rm.to_csv('nonimm_rm_rows.csv', index = False)

# Create new columns
visa = [None]*len(DF)
issue = [None]*len(DF)
nationality = [None]*len(DF)
for i, row in enumerate(DF['V']):
    issue[i] = row.split(' ')[-1]
    visa[i] = row.split(' ')[-2]   
    nationality[i] = ' '.join(row.split(' ')[:-2])
    
DF.columns


## CREATE A NEW DATAFRAME

DF_final = pd.DataFrame({
    'time': DF['time'],
    'pg': DF['pg'],
    'nationality': nationality,
    'visa': visa,
    'issuance':issue   
    })   

### STEP 5: AD-HOC CLEANING

## 5-1: Start with the country column. This is a finite list of 200+ unique items. Single out the suspicious ones.
nationality_ct = DF_final['nationality'].value_counts().to_frame().reset_index().\
    rename(columns = {'index':'country','nationality':'count'}).sort_values(['count','country'], ascending = True).reset_index(drop = True)
# Find the common pattern of problematic rows in the DF, get their indeces
problems_archieve = DF_final[DF_final['nationality'].str.contains("NONIMMIGRANT")].index
# This way, retains the indeces
problems = list(problems_archieve)
problems
# These are the problematic rows. We can replace the content in the nationality, visa, and issuance columns
DF_final[DF_final.index.isin(problems)]
DF_final[DF_final.index.isin(problems)].to_csv('nonimm_problematic_rows.csv', index = True)

# 'V' is a list of the useful information to re-construct the problematic rows
V = [None] * len(problems)
for i, row in enumerate(problems):
    V[i] = DF_final['nationality'][row].split('NONIMMIGRANT')[0]
# Again, keep a copy of it
V_new = list(V)
 
# create a TUPLE: an ORDERED LIST with INDEX and V
problem_tup = [(problems[i], V_new[i]) for i in range(len(problems))]
problem_tup 

## 5-2: Update problematic row cells with correct information
for i, row in problem_tup:
    print(i,' '.join(row.split(' ')[:-2]))
    DF_final['nationality'][i] = ' '.join(row.split(' ')[:-2])
    DF_final['visa'][i] = row.split(' ')[-2]
    DF_final['issuance'][i] = row.split(' ')[-1]

DF_final[DF_final.index.isin(problems)]

# update the country count table, confirm the bad entries are gone
nationality_ct = DF_final['nationality'].value_counts().to_frame().reset_index().\
    rename(columns = {'index':'country','nationality':'count'}).sort_values(['count','country'], ascending = True).reset_index(drop = True)
# Save the country count table, I'll use it to match country codes in a different program
nationality_ct = nationality_ct.sort_values('country', ascending = True)
nationality_ct.to_csv('nonimm_nationalities.csv', index = False)

# see the visa type count, make sure to remove extra leading/trailing space
DF_final['visa'] = DF_final['visa'].apply(lambda x: x.strip())
visa_ct = DF_final['visa'].value_counts().to_frame().reset_index()
visa_ct = visa_ct.rename(columns = {'visa':'count','index':'type'})
# output the visa count
visa_ct.to_csv('nonimm_visa.csv',index = False)
## 5-3: Remove ',' in the issurance count, check whether there're non-numeric entries
DF_final['issuance'].dtypes
DF_final['issuance'] = DF_final['issuance'].str.replace(',', '')
# The output should be blank
print(DF_final['issuance'].dtype)
# convert to integer]
DF_final[DF_final['issuance'].str.contains('[A-Za-z]')]
# convert to integer
DF_final['issuance'] = DF_final['issuance'].astype('int')
# FYI only, what if the problematic rows were not addressed? How many issuance count differences would create?
DF_final['issuance'][DF_final.index.isin(problems)].sum()
DF_final['issuance'].sum()
# by %
100 * DF_final['issuance'][DF_final.index.isin(problems)].sum() / DF_final['issuance'].sum()

### STEP 6: SAVE FINAL DATAFRAME
DF_final.to_csv('nonimm_alltime.csv', index = False)

### STEP 7: SUBSET by release date and SAVE separately
release_date = sorted(list(set(DF_final['time'])))
print(release_date)

path_df = 'nonimmdf'
if os.path.exists(path_df) == True:
    print("YES. The directory already exists. \nNO ACTIONS REQUIRED \n")
    print(sorted(os.listdir(path_df)))
else:
    print("The directory doesn't exist. Create it right now.")
    print("Does the directory exist now?", os.path.exists(path_df))

# Generate a list of dataframe names using list comprehension
df_filenames = [name[:-4] + '.csv' for name in short_names]
# Loop over all release dates and save the visa statistics by month to disk
for i in range(len(release_date)):
    DF_final.loc[DF_final['time'] == release_date[i]].to_csv(path_df + '/' + df_filenames[i], index = False)
