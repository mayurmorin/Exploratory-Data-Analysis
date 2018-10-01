
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')


# In[2]:


df = pd.read_csv('https://raw.githubusercontent.com/jackiekazil/data-wrangling/master/data/chp3/data-text.csv')
df.head(2)


# In[3]:


df1 = pd.read_csv('https://raw.githubusercontent.com/kjam/data-wrangling-pycon/master/data/berlin_weather_oldest.csv')
df1.head(2)


# Task 2.1 Get the Metadata from the above files.

# In[4]:


df.info()
print('\n\n')
df1.info()


# Task 2.2. Get the row names from the above files.

# In[5]:


df.index.values


# In[6]:


df1.index.values


# Task 2.3 Change the column name from any of the above file.

# In[7]:


df.rename(columns={'Indicator': 'Indicator_id'}).head(3)


# Task 2.4 Change the column name from any of the above file and store the changes made
# permanently.

# In[8]:


df.rename(columns={'Indicator': 'Indicator_id'},inplace=True)
df.head()


# Task 2.5 Change the names of multiple columns.

# In[9]:


df.rename(columns={'PUBLISH STATES': 'Publication Status','WHO region':'WHO Region'},inplace=True)
df.head(2)


# Task 2.6. Arrange values of a particular column in ascending order.

# In[10]:


df.sort_values('Year',ascending=True).head()


# Task 2.7 Arrange multiple column values in ascending order.

# In[11]:


df.sort_values(['Year','Numeric'],ascending=[True,True]).head()
#order of the columns in a data frame
df[['Indicator_id', 'Country', 'Year', 'WHO Region', 'Publication Status']].head(3)


# Task 2.8. Make country​ as the first column of the dataframe.

# In[12]:


df = df.reindex(['Country'] + list(df.columns.drop(['Country'])), axis=1)
df.head()


# Task 2.9. Get the column array using a variable

# In[13]:


col = "WHO Region"
df[[col]].values[:, 0]


# Task. 2.10 Get the subset rows 11, 24, 37

# In[14]:


df.loc[[11,24,37]]


# Task 2.11. Get the subset rows excluding 5, 12, 23, and 56

# In[15]:


df.drop([5,12,23,56], axis=0).head()


# In[16]:


# Load datasets from CSV
users = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/users.csv')
sessions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/sessions.csv')
products = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/products.csv')
transactions = pd.read_csv('https://raw.githubusercontent.com/ben519/DataWrangling/master/Data/transactions.csv')

# Convert date columns to Date type
#users['Registered'] = pd.to_datetime(users.Registered)
#users['Cancelled'] = pd.to_datetime(users.Cancelled)
transactions['TransactionDate'] = pd.to_datetime(transactions.TransactionDate)
#sessions['SessionDate']=pd.to_datetime(sessions.SessionDate)


# In[17]:


display(users.head())
display(sessions.head())
display(products.head())
display(transactions.head())


# Task 2.12 Join users to transactions, keeping all rows from transactions and only matching rows from
# users (left join)

# In[18]:


transactions.merge(users, how='left', on='UserID')


# Task 2.13. Which transactions have a UserID not in users?

# In[19]:


transactions[~transactions['UserID'].isin(users['UserID'])]


# Task 2.14. Join users to transactions, keeping only rows from transactions and users that match via
# UserID (inner join)

# In[20]:


transactions.merge(users, how='inner', on='UserID')


# Task 2.15. Join users to transactions, displaying all matching rows AND all non-matching rows (full
# outer join)

# In[21]:


transactions.merge(users, how='outer', on='UserID')


# Task 2.16. Determine which sessions occurred on the same day each user registered

# In[22]:


pd.merge(left=users, right=sessions, how='inner', left_on=['UserID', 'Registered'], right_on=['UserID', 'SessionDate'])


# Task 2.17. Build a dataset with every possible (UserID, ProductID) pair (cross join)

# In[23]:


df1 = pd.DataFrame({'key': np.repeat(1, users.shape[0]), 'UserID': users.UserID})
df2 = pd.DataFrame({'key': np.repeat(1, products.shape[0]), 'ProductID': products.ProductID})
pd.merge(df1, df2,on='key')[['UserID', 'ProductID']]


# Task 2.18. Determine how much quantity of each product was purchased by each user

# In[24]:


#Create DataFrame 1 with 5 records which has key with UserID from 0 to 4 using np.repeat
df1 = pd.DataFrame({'key': np.repeat(1, users.shape[0]), 'UserID': users.UserID})

#Create DataFrame 2 with 5 records which has key with ProductID from 0 to 4 using np.repeat
df2 = pd.DataFrame({'key': np.repeat(1, products.shape[0]), 'ProductID': products.ProductID})

#Create cross join on both df1-users and df2-products which will have 5x5=25 records
user_products = pd.merge(df1, df2,on='key')[['UserID', 'ProductID']]

#For Each product purchased by each User and merging with transactions to calculate quantity sum.
pd.merge(user_products, transactions, how='left', on=['UserID', 'ProductID']).groupby(['UserID', 'ProductID']).apply(lambda x: pd.Series(dict(
    Quantity=x.Quantity.sum()
))).reset_index().fillna(0)


# Task 2.19. For each user, get each possible pair of pair transactions (TransactionID1, TransacationID2)

# In[25]:


pd.merge(transactions, transactions, on='UserID')


# Task 2.20. Join each user to his/her first occuring transaction in the transactions table

# In[26]:


data=pd.merge(users, transactions.groupby('UserID').first().reset_index(), how='left', on='UserID')
data


# Task 2.21. Test to see if we can drop columns
# Code with Output :
# my_columns = list(data.columns)
# my_columns
# ['UserID',
# 'User',
# 'Gender',
# 'Registered',
#  'Cancelled',
# 'TransactionID',
# 'TransactionDate',
# 'ProductID',
# 'Quantity']
# list(data.dropna(thresh=int(data.shape[0] * .9), axis=1).columns) #set threshold to drop NAs
# ['UserID', 'User', 'Gender', 'Registered']
# missing_info = list(data.columns[data.isnull().any()])
# missing_info
# ['Cancelled', 'TransactionID', 'TransactionDate', 'ProductID', 'Quantity']
# //for col in missing_info:
# ​ num_missing = data[data[col].isnull() == True].shape[0]
# print('number missing for column {}: {}'.format(col, num_missing))
# Output: Count of missing data
# number missing for column Cancelled: 3
# number missing for column TransactionID: 2
# number missing for column TransactionDate: 2
# number missing for column ProductID: 2
# number missing for column Quantity: 2
# 
# //for col in missing_info:
# num_missing = data[data[col].isnull() == True].shape[0]
# print('number missing for column {}: {}'.format(col, num_missing)) #count of missing data
# for col in missing_info:
# percent_missing = data[data[col].isnull() == True].shape[0] / data.shape[0]
# print('percent missing for column {}: {}'.format(
# col, percent_missing))
# Output of percentage missing data
# percent missing for column Cancelled: 0.6
# percent missing for column TransactionID: 0.4
# percent missing for column TransactionDate: 0.4
# percent missing for column ProductID: 0.4
# percent missing for column Quantity: 0.4

# In[27]:


my_columns = list(data.columns)
my_columns


# In[28]:


list(data.dropna(thresh=int(data.shape[0] * .9), axis=1).columns) #set threshold to drop NAs


# In[29]:


missing_info = list(data.columns[data.isnull().any()])
missing_info


# In[30]:


for col in missing_info:
    num_missing = data[data[col].isnull() == True].shape[0]
    print('number missing for column {}: {}'.format(col, num_missing))


# In[31]:


#for col in missing_info:
#    num_missing = data[data[col].isnull() == True].shape[0]
    #print('number missing for column {}: {}'.format(col, num_missing)) #count of missing data
for col in missing_info:
    percent_missing = data[data[col].isnull() == True].shape[0] / data.shape[0]
    print('percent missing for column {}: {}'.format(col, percent_missing))

