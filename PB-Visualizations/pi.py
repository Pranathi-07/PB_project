#!/usr/bin/env python
# coding: utf-8

# In[120]:


import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.context import SparkContext
from pyspark.sql import functions
from pyspark.sql import types
from datetime import date, timedelta, datetime
import time


# In[37]:


def convert_to_dict(output, indexName, valueName):
    genderDf = output.toPandas()
    result = genderDf.set_index(indexName).to_dict()
    return result.get(valueName)


# In[66]:


def barchart(inputdict, title, xlabel, ylabel):
    fig = plt.figure(figsize=(12, 4))
    fig.suptitle(title, fontsize=16)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    labels = inputdict.keys()
    values = inputdict.values()
    ax.bar(labels, values, width=0.5)
    plt.show()


# In[101]:


def piechart(diction):
    label = diction.keys()
    tweet_count = diction.values()
    colors = ['#ff9999', '#66b3ff']
    fig1, ax1 = plt.subplots()
    ax1.pie(tweet_count, colors=colors, labels=label,
            autopct='%1.1f%%', startangle=90)
    # draw circle
    centre_circle = plt.Circle((0, 0), 0.70, fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)
    # Equal aspect ratio ensures that pie is drawn as a circle
    ax1.axis('equal')
    plt.tight_layout()
    plt.show()


# In[11]:


sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()


# In[12]:


# Reading Data from File and creating a temp table
total_data = sc.read.json('C:\\Users\\gudiy\\Desktop\\PB-Visualizations\\data\\tweets_data.txt')
total_data.registerTempTable("datatable")


# In[115]:


# Ecom site tweets extraction and creating temp table
total_ecomsite_data = sc.sql("SELECT id as tweet_id, user.id as user_id, user.name as username, lang, user.verified as verified," +
                             "user.location as location, text, created_at, retweet_count as retweets," +
                             "place.country_code as country," +
                             "CASE WHEN text like '%amazon%' THEN 'AMAZON'" +
                             "WHEN text like '%flipkart%' THEN 'FLIPKART'" +
                             "WHEN text like '%walmart%' THEN 'WALMART'" +
                             "WHEN text like '%snapdeal%' THEN 'SNAPDEAL'" +
                             "WHEN text like '%ebay%' THEN 'EBAY'" +
                             "WHEN text like '%etsy%' THEN 'ETSY'" +
                             "WHEN text like '%home depot%' THEN 'HOME DEPOT'" +
                             "WHEN text like '%target%' THEN 'TARGET'" +
                             "WHEN text like '%best buy%' THEN 'BEST BUY'" +
                             "WHEN text like '%wayfair%' THEN 'WAY FAIR'" +
                             "WHEN text like '%macys%' THEN 'MACYS'" +
                             "WHEN text like '%lowes%' THEN 'Lowes'" +
                             "END AS ecomsite from datatable where text is not null")
total_ecomsite_data.registerTempTable("totalecomsitetable")
ecomsite_data = sc.sql(
    "select * from totalecomsitetable where ecomsite is not null")
ecomsite_data.registerTempTable("ecomsitetable")


# In[73]:


# Extract category based tweets from the  Ecom site table
total_categories_data = sc.sql("SELECT *," +
                               "CASE WHEN (text like '%electronics%' or text like '%Electronics%' or text like '%mobile%' or text like '%phone%')THEN 'ELECTRONICS'" +
                               "WHEN (text like '%fashion%' or text like '%jeans%' or text like '%shirt%') THEN 'FASHION'" +
                               "WHEN text like '%book%' THEN 'BOOKS'" +
                               "WHEN (text like '%beauty%' or text like '%makeup%' or text like '%cosmetics%')THEN 'BEAUTY'" +
                               "WHEN (text like '%home%' or text like '%house%') THEN 'HOUSEHOLD'" +
                               "END as category from ecomsitetable")
total_categories_data.createOrReplaceTempView("totalcategorytable")
categories_data = sc.sql(
    "select * from totalcategorytable where category is not null")
categories_data.createOrReplaceTempView("categorytable")


# In[67]:


# Query 1. Get count of each ecom site
count_ecomsite = sc.sql(
    "select ecomsite, count(ecomsite) as count from ecomsitetable group by ecomsite order by count desc")
count_ecomsite.show()
result = convert_to_dict(count_ecomsite, 'ecomsite', 'count')
barchart(result, 'Count of each Ecommerce site',
         'Ecom site name', 'No.of tweets')


# In[90]:


# Query 2 - Country count
countryTweetsCount = sc.sql(
    "SELECT country, count(country) as count from ecomsitetable where country is not null GROUP BY country ORDER BY count DESC")
countryTweetsCount.show()
result = convert_to_dict(countryTweetsCount, 'country', 'count')
barchart(result, 'Count of tweets from each country',
         'Country Name', 'No.of tweets')


# In[123]:


# Query -3 Time analysis of eact tweet
time_data = sc.sql(
    "SELECT SUBSTRING(created_at,12,5) as time_in_hour, COUNT(*) AS count FROM ecomsitetable GROUP BY time_in_hour ORDER BY time_in_hour")
time_data.show()
x = pd.to_numeric(time_data.toPandas()["time_in_hour"].str[:2].tolist(
)) + pd.to_numeric(time_data.toPandas()["time_in_hour"].str[3:5].tolist())/60
y = time_data.toPandas()["count"].values.tolist()
tick_spacing = 2
fig, ax = plt.subplots(1, 1)
ax.plot(x, y)
ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))

plt.title("Tweets Distribution By Minute")
plt.xlabel("Hours (UTC)")
plt.ylabel("Number of Tweets")


# In[94]:


# Query -4 Tweets from each Language
total_language_count = sc.sql("SELECT *," +
                              "CASE when lang LIKE '%en%' then 'English'" +
                              "when lang LIKE '%ja%' then 'Japanese'" +
                              "when lang LIKE '%es%' then 'Spanish'" +
                              "when lang LIKE '%fr%' then 'French'" +
                              "when lang LIKE '%it%' then 'Italian'" +
                              "when lang LIKE '%ru%' then 'Russian'" +
                              "when lang LIKE '%ar%' then 'Arabic'" +
                              "when lang LIKE '%bn%' then 'Bengali'" +
                              "when lang LIKE '%cs%' then 'Czech'" +
                              "when lang LIKE '%da%' then 'Danish'" +
                              "when lang LIKE '%de%' then 'German'" +
                              "when lang LIKE '%el%' then 'Greek'" +
                              "when lang LIKE '%fa%' then 'Persian'" +
                              "when lang LIKE '%fi%' then 'Finnish'" +
                              "when lang LIKE '%fil%' then 'Filipino'" +
                              "when lang LIKE '%he%' then 'Hebrew'" +
                              "when lang LIKE '%hi%' then 'Hindi'" +
                              "when lang LIKE '%hu%' then 'Hungarian'" +
                              "when lang LIKE '%id%' then 'Indonesian'" +
                              "when lang LIKE '%ko%' then 'Korean'" +
                              "when lang LIKE '%msa%' then 'Malay'" +
                              "when lang LIKE '%nl%' then 'Dutch'" +
                              "when lang LIKE '%no%' then 'Norwegian'" +
                              "when lang LIKE '%pl%' then 'Polish'" +
                              "when lang LIKE '%pt%' then 'Portuguese'" +
                              "when lang LIKE '%ro%' then 'Romanian'" +
                              "when lang LIKE '%sv%' then 'Swedish'" +
                              "when lang LIKE '%th%' then 'Thai'" +
                              "when lang LIKE '%tr%' then 'Turkish'" +
                              "when lang LIKE '%uk%' then 'Ukrainian'" +
                              "when lang LIKE '%ur%' then 'Urdu'" +
                              "when lang LIKE '%vi%' then 'Vietnamese'" +
                              "when lang LIKE '%zh-cn%' then 'Chinese (Simplified)'" +
                              "when lang LIKE '%zh-tw%' then 'Chinese (Traditional)'" +
                              "END AS language from ecomsitetable")
total_language_count.createOrReplaceTempView("languagetable")
language_count = sc.sql(
    "SELECT language, count(language) as count from languagetable where language is not null group by language order by count DESC")
language_count.show()
result = convert_to_dict(language_count, 'language', 'count')
barchart(result, 'Tweets from each Language about Ecommerce sites',
         'Language codes', 'No.of tweets')


# In[102]:


category = sc.sql(
    "select category, count(category) as count from categorytable where group By category order by count desc")
category.show()
result = convert_to_dict(category, 'category', 'count')
piechart(result)


# In[105]:


# Query -6 : Count of verified and non verified users
user_data = sc.sql(
    "select user_id, CASE when verified like '%true%' THEN 'VERIFIED' WHEN verified like '%false%' THEN 'NON-VERIFIED' END AS verified from ecomsitetable")
user_data.registerTempTable("verifiedtable")
verified_data = sc.sql(
    "select verified, count(verified) as count from verifiedtable group by verified")
verified_data.show()
result = convert_to_dict(verified_data, 'verified', 'count')
piechart(result)


# In[107]:


# Query -7 : Count of retweets for each ecom site
retweetCount = sc.sql("select ecomsite, count(retweets) as retweet_count from ecomsitetable where retweets is not null " +
                      "group BY ecomsite order by retweet_count DESC")
retweetCount.show()
result = convert_to_dict(retweetCount, 'ecomsite', 'retweet_count')
barchart(result, 'Retweet count for each Ecom site',
         'Name of E-com site', 'No.of Retweets')


# In[112]:


# Query -8 Tweets about offers by each ecomm site
offers_data = sc.sql("select ecomsite, " +
                     "CASE WHEN (text like '%offer%' OR text like '%deal%' OR text like '%exclusive%' " +
                     "OR text like '%best deal%' OR text like '%best offer%') THEN 'OFFERS' END as offers from ecomsitetable")
offers_data.createOrReplaceTempView("offerstable")
offers_count = sc.sql(
    "select ecomsite, count(offers) as offers_count from offerstable where offers is not null GROUP BY ecomsite ORDER BY offers_count DESC")
offers_count.show()
result = convert_to_dict(offers_count, 'ecomsite', 'offers_count')
barchart(result, 'Tweets about offers by each ecomm site',
         'Name of E-com site', 'No.of Tweets about offers')


# In[ ]:


