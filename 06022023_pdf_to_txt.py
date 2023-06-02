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
