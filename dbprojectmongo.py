#!/usr/bin/env python
# coding: utf-8

# In[29]:


pip install pymongo


# In[30]:


import pymongo


# In[31]:


client = pymongo.MongoClient("mongodb://localhost:27017/")


# In[32]:


db = client['dbproject']


# In[33]:


collection = db['collisions']
collection = db['parties']
collection = db['victims']


# In[34]:


# Fetching all documents from the collection
cursor = collection.find()

# Iterating over the cursor to print each document
for document in cursor:
    print(document)


# # Count the Number of Victims in Each Collision:

# In[36]:


pipeline = [
    {
        "$group": {
            "_id": "$case_id",
            "total_victims": {"$sum": 1}
        }
    }
]
cursor = db.victims.aggregate(pipeline)
for document in cursor:
    print(document)


# # Count the Number of Victims with Fatal Injuries:

# In[41]:


fatal_victims_count = db.victims.count_documents({"victim_degree_of_injury": "fatal"})
print("Total number of victims with fatal injuries:", fatal_victims_count)


# # calculating the median age for each case ID group.

# In[91]:


from pymongo import MongoClient
from statistics import median

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["dbproject"]

# Define the aggregation pipeline
pipeline = [
    {
        "$group": {
            "_id": "$case_id",
            "victim_ages": {"$push": "$victim_age"}
        }
    }
]

# Aggregate data
result = db.victims.aggregate(pipeline)

# Calculate median age for each group
for entry in result:
    case_id = entry["_id"]
    victim_ages = entry["victim_ages"]
    median_age = median(victim_ages) if victim_ages else None
    print(f"Case ID: {case_id}, Median Age: {median_age}")

