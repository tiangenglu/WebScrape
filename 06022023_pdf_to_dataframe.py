#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 15:05:00 2023

@author: Tiangeng Lu
"""

# packages
import os
import pandas as pd
from PyPDF2 import PdfReader

# browse the directory

path = 'nonimm'
os.path.exists(path)

if os.path.exists(path) == True:
    print(sorted(os.listdir(path)))
else:
    print("The directory doesn't exist.")
    
# create a list of full file names
fullpath = os.getcwd()+'/'+path+'/'
short_names = sorted(os.listdir(path))

fullnames = [None] * len(short_names)
for i, pdf in enumerate(short_names):
    fullnames[i] = fullpath + pdf
    
# test with ONE PAGE in an individual pdf document

fullnames[0]
# create the reading object
reader = PdfReader(fullnames[0])
type(reader)
# PyPDF2._reader.PdfReader
len(reader.pages)
# 106, equals to the number of pages
# get the first page
page = reader.pages[10]
text = page.extract_text()
text
len(text)
type(text) # str
text.split('\n')

# save sample page to .txt
file = open('samplepdfconvert.txt','w')
file.write(text + "\n") # save at the unit of page
file.close()
# local txt file looks as expected

# Next, try to save the ENTIRE pdf document to a .txt
# keep the same reader object
reader = PdfReader(fullnames[0])
type(reader.pages) # PyPDF2._page._VirtualList
Pages = [None] * len(reader.pages)
Text = [None] * len(reader.pages)
# loop over every page
range(len(reader.pages))
for i in range(len(reader.pages)):
    Pages[i] = reader.pages[i]
    Text[i] = Pages[i].extract_text()

len(Text) # 106, equals to the number of pdf pages
Text[-1] # shows the last page of the content
type(Text) # list

file = open('samplepdfconvert.txt', 'w')
for page in Text:
    file.write(page + "\n")
file.close()

# DONE with download of individual pdf document of 100+ pages.
#

# The next step is to apply the validated codes to all the pdf documents in the directory.
# page-->all pages in a single pdf--> all pdfs within the directory


# First, create a new directory


path_txt = 'nonimmtxt'

if os.path.exists(path_txt) == True:
    print("YES. The directory already exists & No actions required.")
    print(sorted(os.listdir(path_txt)))
else:
    print("The directory doesn't exist. Create it right now.")
    os.makedirs(path_txt)
    print("Does the directory exist now?", os.path.exists(path_txt))

# Next, develop a NESTED LOOP
len(fullnames) # 74
type(fullnames) # list

TEXT = [None] * len(fullnames)
for n, nth_pdf in enumerate(fullnames):
    reader = PdfReader(nth_pdf)
    Pages = [None] * len(reader.pages)
    Text = [None] * len(reader.pages)
    # inner loop
    for i in range(len(reader.pages)):
        Pages[i] = reader.pages[i]
        Text[i] = Pages[i].extract_text()
    TEXT[n] = Text

len(TEXT[0]) # 106

# Finally, save these .txt documents to local directory

txtnames = [None]*len(fullnames)

os.getcwd()

for i,txt in enumerate(short_names):
    # DON'T add '/' at the beginning
    txtnames[i] = path_txt +'/'+ txt[:-4] + '.txt'

# One loop works because the txtnames and the TEXT have the same length
for i,txt in enumerate(txtnames):
    file = open(txt, 'w')
    for page in TEXT[i]:
        file.write(page + "\n")
    file.close()

# Confirm that .txt documents are already in the directory
len(os.listdir(path_txt))
sorted(os.listdir(path_txt))
# LOOKS GOOD!

################################ DATA CLEANING ###############################

# NEXT, concatenate all .txt files into one

## START with cleaning individual data frame
df_list = [None] * len(TEXT[0])

for i, page in enumerate(TEXT[0]):
    df_list[i] = pd.DataFrame(page.split('\n'))
# append the data frame for every page to one bigger dataframe via generator
df_file = pd.concat([df for df in df_list]).reset_index(drop = True)

df_file = df_file.rename(columns = {0:'V'})
# Use this syntax to set to upper case and trim
df_file['V'] = df_file['V'].str.upper().apply(lambda x: x.strip())

# CONDITIONS


# 1. Get index for specific rows
headers = df_file[(df_file['V'].str.startswith('NONIMMIGRANT')) | (df_file['V'].str.startswith('NATIONALITY')) | (df_file['V'].str.startswith('PAGE'))].index
# Get index for the Grand Total row, use [0] to index the element
df_file[df_file['V'].str.contains('GRAND TOTAL', case = False)].index[0]
# 2. Get index for all the rows after Grand Total
footnotes = df_file.index[df_file.index >= df_file[df_file['V'].str.contains('GRAND TOTAL', case = False)].index[0]]

#remove_ids = list(df_file[(df_file['V'].str.startswith('Nonimmigrant')) | (df_file['V'].str.startswith('Nationality')) | (df_file['V'].str.startswith('Page')) | (df_file['V'].str.startswith('page'))].index
#)\
#    + list(df_file.index[df_file.index >= df_file[df_file['V'].str.contains('Grand Total', case = False)].index[0]])

remove_ids = list(headers) + list(footnotes)
remove_ids = list(set(remove_ids))
len(remove_ids)

# remove the non-entry rows
df_file = df_file[~df_file.index.isin(remove_ids)]

# Create new columns
visa = [None]*len(df_file)
issue = [None]*len(df_file)
nationality = [None]*len(df_file)

range(len(df_file['V']))

for i, row in enumerate(df_file['V']):
    issue[i] = row.split(' ')[-1]
    visa[i] = row.split(' ')[-2]
    
    nationality[i] = ' '.join(row.split(' ')[:-2])

# Create a new dataframe with the new columns
df_new = pd.DataFrame({
    'nationality': nationality,
    'visa':visa,
    'issuances':issue,
    'time':short_names[0][7:-4]
    })
df_new['issuances'] = df_new['issuances'] .str.replace(',','')
df_new['issuances'].dtypes
df_new['issuances'] = df_new['issuances'].astype('int')
df_new.info()
# END

# LOOP OVER ALL .TXT TO DATAFRAME

path_df = 'nonimmdf'

if os.path.exists(path_df) == True:
    print("YES. The directory already exists & No actions required.")
    print(sorted(os.listdir(path_df)))
else:
    print("The directory doesn't exist. Create it right now.")
    os.makedirs(path_df)
    print("Does the directory exist now?", os.path.exists(path_df))


# Irregularities have been observed in the scraped .txt from .pdfs. The irregularities are not from python conversion
# Rather, when I manually copy the .pdf into a .txt, the same irregularities exist
# What displays in the .pdf document isn't necessarily the same as the scraped .txt
# I found splitting the columns after concatenating all is less buggy

DF_raw = [None] * len(TEXT)    
# OUTER loop for the each .txt(pdf) document scraped from webpage(s)
for n in range(len(TEXT)):
    # INNER loop for each page within one .txt(converted from pdf, page info retained from pdf) document
    df_list = [None] * len(TEXT[n])
    for i, page in enumerate(TEXT[n]):
        # work on each page
        df_list[i] = pd.DataFrame(page.split('\n'))
        df_list[i]['pg'] = i + 1
        # append the data frame for every page to one bigger dataframe via generator
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


# 1. Get index for specific rows
headers = DF[(DF['V'].str.startswith('NONIMMIGRANT')) | (DF['V'].str.startswith('NATIONALITY')) | (DF['V'].str.startswith('PAGE'))].index
notes = DF[DF['V'].str.contains('\(FY 2', case = False) | DF['V'].str.contains('\#SBU ', case = False)].index
#SBU
len(notes)

remove_ids = list(set(list(headers) + list(notes)))
len(remove_ids)

DF = DF[~DF.index.isin(remove_ids)]



# Create new columns
visa = [None]*len(DF)
issue = [None]*len(DF)
nationality = [None]*len(DF)
for i, row in enumerate(DF['V']):
    issue[i] = row.split(' ')[-1]
    visa[i] = row.split(' ')[-2]   
    nationality[i] = ' '.join(row.split(' ')[:-2])
    
DF.columns


## HIGHLY RECOMMENDED to start with a new dataframe

DF_final = pd.DataFrame({
    'time': DF['time'],
    'pg': DF['pg'],
    'nationality': nationality,
    'visa': visa,
    'issuance':issue   
    })

# A good way to identify problematic/uncleaned rows is to see the frequency of countries
DF_nat = DF_final['nationality'].value_counts().to_frame().reset_index().\
    rename(columns = {'index':'country','nationality':'count'}).sort_values(['count','country'], ascending = True).reset_index(drop = True)

##### MANUAL ADJUSTMENT
# Find the common pattern of problematic rows in the DF, get their indeces
problems_archieve = DF_final[DF_final['nationality'].str.contains("NONIMMIGRANT")].index
# This way, retains the indeces
problems = list(problems_archieve)
len(problems)
problems
# These are the problematic rows. We can replace the content in the nationality, visa, and issuance columns
DF_final[DF_final.index.isin(problems)]

DF_nat['country'].str.contains("NONIMMIGRANT")


DF_final['nationality'][DF_final['nationality'].str.contains("NONIMMIGRANT")].str.split('NONIMMIGRANT')
print(problems)
# Fill in the columns with correct values
V = [None] * len(problems)
for i, row in enumerate(problems):
    V[i] = DF_final['nationality'][row].split('NONIMMIGRANT')[0]

V_new = list(V_new) 

V_new

# create a tuple
problem_tup = [(problems[i], V_new[i]) for i in range(len(problems))]
problem_tup   

for i, row in problem_tup:
    print(i,' '.join(row.split(' ')[:-2]))
    DF_final['nationality'][i] = ' '.join(row.split(' ')[:-2])
    DF_final['visa'][i] = row.split(' ')[-2]
    DF_final['issuance'][i] = row.split(' ')[-1]

DF_final[DF_final.index.isin(problems)]

nationality_ct = DF_final['nationality'].value_counts().to_frame().reset_index()
nationality_ct


DF_final['visa'] = DF_final['visa'].apply(lambda x: x.strip())
visa_ct = DF_final['visa'].value_counts().to_frame().reset_index()
# observe the patterns
# Now, remove the , in the issuances column
DF_final['issuance'] = DF_final['issuance'].str.replace(',', '')

# Are there any alphabetic characters in the issuance column?
DF_final[DF_final['issuance'].str.contains('[A-Za-z]')]

DF_final['issuance'] = DF_final['issuance'].astype('int')

# Check how many visa issuance counts would be impacted by the previously problematic rows?
DF_final['issuance'][DF_final.index.isin(problems)].sum()

# save final csv to disk
DF_final.to_csv('nonimm_alltime.csv', index = False)