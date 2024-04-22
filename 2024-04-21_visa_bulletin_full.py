#!/usr/bin/env python
# coding: utf-8

# # One-Time Scrape Visa-Bulletin

# In[1]:


import os
from datetime import datetime
import datetime as dt
import pandas as pd
import requests
from scrapy import Selector
import time as tm
from io import StringIO # newly added for pd.read_html(StringIO(link))


# In[2]:


pd.set_option('display.max_rows', 50)


# # Get URLs

# In[3]:


main_url = 'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-bulletin.html'
# type = str
main_selector = Selector(text = requests.get(main_url).content)
all_links = main_selector.xpath('//*[contains(@href,"visa-law0/visa-bulletin/20")]/@href').extract()


# In[4]:


prefix = "https://travel.state.gov"


# In[5]:


all_links=[prefix + link for link in all_links if link.startswith('/content')]


# # Catalog

# In[6]:


month = [None] * len(all_links)
year = [None] * len(all_links)
for i, link in enumerate(all_links):
    month[i] = link.split('/')[-1].split('-')[-2].upper()
    year[i] = link.split('/')[-1].split('-')[-1].split('.')[0]
# print out unique year and month results to check irregularities
print(set(year)); print(set(month))


# In[7]:


urls = list(all_links)
#  'https://travel.state.gov/content/travel/en/legal/visa-law0/visa-bulletin/2007/visa-bulletin-for-september-2007.html'
urls = [link for link in all_links if ('visa-availability' not in link and '2007/july-2007-visa-bulletin.html' not in link)]
urls = list(set(urls))


# In[8]:


new_year = [link.split('/')[-1].split('.')[0].split('-')[-1] for link in urls]
new_month= [link.split('/')[-1].split('.')[0].split('-')[-2].upper() for link in urls]


# In[9]:


print(set(new_year))
print(set(new_month))


# In[10]:


catalog=pd.DataFrame(data = {'url': urls,'year': new_year, 'month': new_month})
catalog['stamp'] = pd.to_datetime(catalog['month'].str.cat(catalog['year'],sep = '/'), errors = 'raise', format = "%B/%Y").dt.date
catalog = catalog.sort_values(by = 'stamp').reset_index(drop = True)
catalog.tail()


# # Scrape Raw Tables

# In[11]:


all_tables = [None] * len(catalog)
for i in range(len(catalog)):
    html = requests.get(catalog['url'][i]).content
    sel = Selector(text = html)
    all_tables[i] = sel.xpath('//table').extract()
    if i%5 == 0:
        print(i,tm.strftime("%Y-%m-%d,%H:%M:%S"))    


# In[12]:


emp_tables = [None]*len(all_tables)
for i in range(len(all_tables)):
    emp_tables[i] = [tab for tab in all_tables[i] if "Employment" in tab]


# In[13]:


table_len = [None] * len(all_tables)
for i in range(len(all_tables)):
    table_len[i] = len(emp_tables[i])
# the number of tables to be scraped varies by web pages
print(table_len)   


# In[14]:


employment_tab_url = pd.DataFrame({
    'length': table_len, 'table': emp_tables})


# In[15]:


info_df = pd.concat([catalog, employment_tab_url], axis = 1)
# "unlist" nested values in the 'table' column
info_df_long = info_df.explode('table')[['url','stamp','table']]
info_df_short = info_df_long.drop_duplicates(subset='stamp', keep = 'first')
info_df_short = info_df_short.reset_index(drop = True)


# In[16]:


info_df_short.head()


# # Raw to DataFrame

# In[17]:


DF_list = [None] * len(info_df_short)
print(tm.strftime("%H:%M:%S"))
for raw in info_df_short['table']:
    # .dropna(how = "any") is optional and specific given the properties of these dataframes
    # without the .dropna() statement, the executing time would reduce to half
    # wrap `raw` with StringIO()
    DF_list = [pd.read_html(StringIO(raw), header=0)[0].dropna(how = "any") for raw in info_df_short['table']]
print(tm.strftime("%H:%M:%S"))


# In[18]:


for df in DF_list:
    df.columns = ["Employment-Based"] + list(df.columns[1:]) # set name of the 1st col


# In[19]:


edit_index = []
for i,df in enumerate(DF_list): # check whether colnames still exist in the first row of any dfs
    if df.iloc[0].str.contains("Chargeability", case = False).any() == True:
        print(str(i),df.iloc[0].str.contains("Chargeability", case = False).any())
        edit_index.append(i)
print(edit_index)  


# In[20]:


# make first row as column names
for i in edit_index:
    DF_list[i].columns = DF_list[i].iloc[0]    
# drop first row if the first row is column names
for i in edit_index:
    if DF_list[i].iloc[0].str.contains("Chargeability", case = False).any() == True:
        DF_list[i] = DF_list[i].iloc[1:] 


# In[21]:


# Assign a consistent spelling/spacing to it
for df in DF_list:
    # can only concatenate list (not "str") to list
    df.columns = ["Employment-Based"] + ["All_Chargeability_Except_Listed"] + list(df.columns[2:])


# In[22]:


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


# In[23]:


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


# In[24]:


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


# In[25]:


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


# In[26]:


# Add timestamp by using the info_df_short dataframe
for i in range(len(DF_list)):
    DF_list[i]['time'] = info_df_short['stamp'][i]


# # Merge to One DataFrame

# In[27]:


# Merge all dataframes into one
current_data = pd.concat([df for df in DF_list]).set_index(['time'])


# In[28]:


current_data['time'] = current_data.index
current_data = current_data.reset_index(drop = True)
current_data['Employment-Based'] = current_data['Employment-Based'].str.replace('\xa0',' ')
current_data['Employment-Based'] = current_data['Employment-Based'].str.replace('-  ','')
current_data['Employment-Based'] = current_data['Employment-Based'].str.replace(r'\s+', ' ', regex=True)
current_data['Employment-Based'] = current_data['Employment-Based'].str.replace('*','')


# In[29]:


# start with most common categories
eb_categories = {
    '1st':'E1',
    '2nd':'E2',
    '3rd':'E3',
    '4th':'E4',
    'Other Workers':'E3_U',
    'Other Worker':'E3_U',
    '5th':'E5'}
current_data['Preference'] = current_data['Employment-Based'].map(eb_categories)
# assign standardized values to not-so-common values
current_data.loc[current_data['Employment-Based'].str.contains('5th '), "Preference"] = 'E5'
current_data.loc[current_data['Employment-Based'].str.contains('Targeted'), "Preference"] = 'E5'
current_data.loc[current_data['Employment-Based'].str.contains('Religi'), "Preference"] = 'E4_R'
current_data.loc[current_data['Employment-Based'].str.contains('Schedule A'), "Preference"] = 'A'
current_data.loc[current_data['Employment-Based'].str.contains('Translator'), "Preference"] = 'SIV'
# end of 04/21/2024 revision
# fill na with 'Unknown'
current_data['Preference'] = current_data['Preference'].fillna('Unknown')
print(current_data['Preference'].value_counts())


# In[30]:


print(current_data.columns)


# In[31]:


# has to include all chargeability
current_countries = [col for col in current_data.columns 
                     if col not in ['time', 'Employment-Based','Preference']]
# Get the column index, e.g., `[1, 2, 3, 4, 5]`
current_countries_index = [current_data.columns.get_loc(c) for c in current_countries]
print(f"Current country column indeces: {current_countries_index}")


# In[32]:


current_data.isnull().sum()[current_data.isnull().sum()>0]


# In[33]:


country_cols=[col for col in current_countries if col != 'All_Chargeability_Except_Listed']
for col in country_cols:
    current_data[col] = current_data[col].fillna(current_data['All_Chargeability_Except_Listed'])


# In[34]:


current_data.isnull().sum()[current_data.isnull().sum()>0]


# In[35]:


current_data.tail()


# In[36]:


# update the 08SEP15 date format to 2015-09-08
# If NAs haven't been filled, the type becomes 'float' and then leads to error
for i in range(len(current_data)):
    for j in current_countries_index: # j is the number of country columns
        if len(current_data.iloc[i,j]) == 7:
            current_data.iloc[i,j] = datetime.strptime(current_data.iloc[i,j], "%d%b%y").strftime("%Y-%m-%d")


# In[37]:


current_data.loc[current_data['All_Chargeability_Except_Listed']!='C'].head()


# In[38]:


current_data['time'] = pd.to_datetime(current_data['time']).dt.date
current_data = current_data.sort_values(by = ['time','Preference'])
current_data = current_data.drop_duplicates().reset_index(drop = True)


# # Save Data

# In[39]:


current_data.to_csv('visa_bulletin_'+tm.strftime("%b%Y")+'.csv', index = False)


# In[40]:


catalog['stamp'] = catalog['stamp'].astype(str)
catalog = catalog.reset_index(drop = True)
#catalog.to_csv('bulletin_catalog.csv', index = False)


# # Raw .html Tables

# In[41]:


emp_tables[-1]


# In[42]:


pd.read_html(StringIO(emp_tables[-1][0]), header=0)[0]

