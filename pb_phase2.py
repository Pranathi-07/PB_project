from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.context import SparkContext
from pyspark.sql import functions
from pyspark.sql import types
from datetime import date, timedelta, datetime
# from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import time
import gmplot


def convert_to_dict(output, indexName, valueName):
    genderDf = output.toPandas()
    result = genderDf.set_index(indexName).to_dict()
    return result.get(valueName)

# def barchart(inputdict, title, xlabel, ylabel):
#     fig = plt.figure(figsize=(6, 4))
#     fig.suptitle(title, fontsize=16)
#     ax = fig.add_axes([0.1, 0.2, 0.75, 0.5])
#     ax.set_xlabel(xlabel)
#     ax.set_ylabel(ylabel)
#     labels = inputdict.keys()
#     values = inputdict.values()
#     ax.bar(labels, values, width=0.5)
#     plt.show()

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
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query5.png')
# -------------------query 1:From which country, more tweets received for corona virus----------------
def query1() :
    country_count = sc.sql("SELECT distinct Country, count(*) as count FROM totaltweetsdata GROUP BY Country ORDER BY count DESC")
    country_count.show(10)
    x = country_count.toPandas()["Country"].values.tolist()[:15]
    y = country_count.toPandas()["count"].values.tolist()[:15]
    figure = plt.figure(figsize=(10, 10))
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'green')
    plt.title("tweets distribution countries")
    plt.ylabel("Country code")
    plt.xlabel("Number of tweets")
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query1.png')
    # result = convert_to_dict(country_count, 'Country', 'count')
    # barchart(result, 'count of tweets from each country about coronavirus','Country', 'No.of tweets')

# ------------------------------query 2: location wise tweets distribution on coronavirus in US-------------------------------------
def query2() :
    state_distribution = sc.sql("SELECT location, COUNT(location) AS count FROM totaltweetsdata WHERE Country='US' AND location is NOT NULL GROUP BY location ORDER BY count DESC")
    state_distribution.show() 
    labels = state_distribution.toPandas()["location"].values.tolist()[:12]
    sizes = state_distribution.toPandas()["count"].values.tolist()[:12]
    explode = (0.1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 )  # only "explode" the 1st slice

    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, explode=explode, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.

    plt.title("Tweets Distribution in USA")
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query2.png')
    # result = convert_to_dict(state_distribution, 'location', 'count')
    # piechart(result)

# ---------------------------------query 3:Popular language used for tweets --------------------------------
def query3() :
    lang_data = sc.sql("SELECT lang, COUNT(*) AS Count FROM datatable WHERE lang IS NOT NULL GROUP BY lang ORDER BY Count DESC")
    lang_data.show()
    labels = lang_data.toPandas()["lang"].values.tolist()[:5]
    sizes = lang_data.toPandas()["Count"].values.tolist()[:5]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title("Tweets Distribution among top 5 languages used ")
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query3.png')

    # res = convert_to_dict(lang_data, 'lang', 'Count')
    # piechart(res)

# -------------------------query 4: Time analysis of the tweets on coronavirus------------------------
def query4() :
    time_analysis = sc.sql("SELECT SUBSTRING(created_at,12,5) as time_in_hour, COUNT(*) AS count FROM totaltweetsdata "+
    "GROUP BY time_in_hour ORDER BY time_in_hour")
    time_analysis.show()
    x = pd.to_numeric(time_analysis.toPandas()["time_in_hour"].str[:2].tolist()) + pd.to_numeric(time_analysis.toPandas()["time_in_hour"].str[3:5].tolist())/60
    y = time_analysis.toPandas()["count"].values.tolist()
    tick_spacing = 2
    fig, ax = plt.subplots(1, 1)
    ax.plot(x, y)
    ax.xaxis.set_major_locator(ticker.MultipleLocator(tick_spacing))
    plt.title("Time analysis of tweets by minutes")
    plt.xlabel("Hours (UTC)")
    plt.ylabel("Number of Tweets")
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query4.png')
# -------------------------------query 5: verified accounts tweets about coronavirus--------------------------
def query5() :
    acct_type = sc.sql("SELECT Country, " +
     "CASE when userVerified LIKE '%true%' THEN 'VERIFIED ACCOUNT'"+
     "when userVerified LIKE '%false%' THEN 'NON-VERIFIED ACCOUNT'"+
     "END AS Verified from totaltweetsdata where text is not null")
    acct_type.registerTempTable("verified")
    acctverfication = sc.sql("SELECT  Verified, Count(*) as Count from verified group by Verified")
    acctverfication.show()
    status = convert_to_dict(acctverfication, 'Verified', 'Count')
    piechart(status)
# -----------------------query 6: top 10 retweets of users---------------
def query6() :
    # retweet_data = sc.sql("SELECT distinct UserName, retweet_count as count from totaltweetsdata where UserName is not null order by count desc ")
    # retweet_data.registerTempTable("retweetdata")
    # user_retweet_count = sc.sql("SELECT  UserName, Count(Retweet) as Count from retweetdata group by UserName order by Count DESC")
    # user_retweet_count.show(10)
    user_retweet_count = sc.sql("SELECT user.screen_name as UserName, retweeted_status.retweet_count as count from datatable where user.name is not null order by count desc")
    user_retweet_count.show(10)
    x = user_retweet_count.toPandas()["UserName"].values.tolist()[:10]
    y = user_retweet_count.toPandas()["count"].values.tolist()[:10]
    figure = plt.figure()
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'blue')
    plt.title("Top 10 People Who Have retweets")
    plt.ylabel("Users")
    plt.xlabel("Number of retweets")
    plt.xticks(rotation=40)
    plt.xticks(fontsize=7)
    # plt.show()
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query6.png')
    # results = convert_to_dict(user_retweet_count, 'Retweet', 'Count')
    # barchart(results, 'Top 10 users with retweets on corona','Users', 'No.of tweets')
# ---------------------------query 7: Top 10 people who tweeted most about corona------------------------------
# def query7() :
#     devices_data = sc.sql("SELECT source, COUNT(*) AS  total_count FROM datatable WHERE source IS NOT NULL GROUP BY source ORDER BY total_count DESC")
#     devices_data.show(10)
#     first = devices_data.toPandas()["source"].str.index(">")+1
#     last = devices_data.toPandas()["source"].str.index("</a>")
#     text = devices_data.toPandas()["source"].values.tolist()
#     x =[]
#     for i in range(len(text)):
#         x.append(text[i][first[i]:last[i]])
#     y = devices_data.toPandas()["total_count"].values.tolist()[:10]


#     figure = plt.figure()
#     axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
#     plt.barh(x,y, color = 'blue')
#     # plt.title("Top ", len(x), " Devices")
#     plt.ylabel("Device name")
#     plt.xlabel("Number of Devices")
#     plt.title("Top Devices Used in the Tweets")
#     plt.show()
#     # device_distribution = convert_to_dict(devices_data,'source','total_count')
#     # barchart(device_distribution,'Top 10 devices used', 'Device Name', 'Tweet count')
def query7():
    user_tweets = sc.sql("SELECT distinct UserName, COUNT(*) AS  total_count FROM totaltweetsdata WHERE UserName IS NOT NULL GROUP BY UserName ORDER BY total_count DESC")
    user_tweets.show(10)
    x = user_tweets.toPandas()["UserName"].values.tolist()[:10]
    y = user_tweets.toPandas()["total_count"].values.tolist()[:10]
    figure = plt.figure()
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'red')
    plt.title("Top 10 tweeter")
    plt.ylabel("Users")
    plt.xlabel("Number of tweets out corona")
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query7.png')

#--------------------------query 8: Top 10 of hashtags used -------------------------------
def query8() :
    hashtagsDF = sc.sql("SELECT hashtags, COUNT(*) AS count FROM (SELECT explode(entities.hashtags.text) AS hashtags "+
    "FROM datatable) WHERE hashtags IS NOT NULL GROUP BY hashtags ORDER BY count DESC")
    hashtagsDF.show(10)
    labels = hashtagsDF.toPandas()["hashtags"].values.tolist()[:9]
    sizes = hashtagsDF.toPandas()["count"].values.tolist()[:9]
    fig1, ax1 = plt.subplots()
    ax1.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=False, startangle=90)
    ax1.axis('equal') 
    # ax1.legend(fig1, labels, loc="best")
    plt.title(" Top 9 Hashtags Distribution")
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query8.png')

# ------------------------query 9: analysis on user creation ----------------------------
def query9() :
    user_year = sc.sql("SELECT SUBSTRING(user.created_at,27,4) as year from datatable where text is not null")
    user_year.registerTempTable("yeartable")
    user_creation = sc.sql("SELECT year, COUNT(*) as count from yeartable where year is not null group by year order by year DESC limit 10")
    user_creation.show()
    x = user_creation.toPandas()["year"].values.tolist()
    y = user_creation.toPandas()["count"].values.tolist()
    plt.title("Wave for user creation for past 10 years")
    plt.plot(x, y)
    plt.ylabel("created accounts")
    plt.xlabel("year")
    # plt.xticks(rotation=40)
    # plt.show()
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query9.png')
#     coorddata = sc.sql("SELECT coordinates FROM totaltweetsdata WHERE coordinates IS NOT NULL")
#     coordDF = coorddata.select(coorddata.coordinates[0], coorddata.coordinates[1])
#     x = coordDF.toPandas()["coordinates[0]"].values.tolist()
#     y = coordDF.toPandas()["coordinates[1]"].values.tolist()
    
#     gmap1 = gmplot.GoogleMapPlotter(31.51073, -96.4247, 0)
#     # gmap1.scatter( x, y, '# FF0000', 
#     #                           size = 30, marker = False )
#     # gmap1.plot(x, y)
#     gmap1.heatmap(x, y)
#     gmap1.draw( "C:\\Users\\gudiy\\OneDrive\\Desktop\\map.html" )

#     # m = Basemap(projection='merc', llcrnrlat=-80, urcrnrlat=80, llcrnrlon=-180, urcrnrlon=180, lat_ts=20, resolution='c')
#     # m.drawcoastlines()
#     # m.drawcountries()
#     # m.fillcontinents(color='#04BAE3', lake_color='#FFFFFF')
#     # m.drawmapboundary(fill_color='#FFFFFF')

#     # for i in range(len(x)):
#     #     x1, y1 = m(x[i], y[i])
#     #     m.plot(x1, y1, 'r.')

#     # plt.title("GPS Coordinates of the Twitter Accounts")

#-------------------------query 10: Top 10 users with friends----------------------------------
def query10() :
    friendsdata = sc.sql("select user.screen_name,user.friends_count AS friendsCount from datatable where (user.id_str,created_at) "+
    "in (select user.id_str, max(created_at) as created_at from datatable group by user.id_str ) ORDER BY friendsCount DESC")
    friendsdata.show(10)
    x = friendsdata.toPandas()["screen_name"].values.tolist()[:10]
    y = friendsdata.toPandas()["friendsCount"].values.tolist()[:10]
    figure = plt.figure()
    axes = figure.add_axes([0.3, 0.1, 0.65, 0.85])
    plt.rcParams.update({'axes.titlesize': 'small'})
    plt.barh(x,y, color = 'blue')
    plt.title("Top 10 People Who Have friends")
    plt.ylabel("User Name")
    plt.xlabel("Number of friends")
    plt.savefig('C:\\Users\\gudiy\\Desktop\\pbproject\\dataanalysis\\static\\images\\query10.png')


if __name__ == "__main__":
    sc = SparkSession.builder.appName("PysparkExample").config("spark.sql.shuffle.partitions", "50").config("spark.driver.maxResultSize", "5g").config("spark.sql.execution.arrow.enabled", "true").getOrCreate()
    print("Spark session start")
    total_data = sc.read.json('C:\\Users\\gudiy\\Desktop\\PB-Visualizations\\data\\tweets_data.txt')
    total_data.registerTempTable("datatable")
    print("table loaded")
    total_tweets_data = sc.sql("SELECT user.name as UserName,user.location as location,text,created_at,user.verified as userVerified,retweet_count,place.country_code as Country,user.location as state,extended_tweet.entities.hashtags.text AS Hashtags,coordinates.coordinates as coordinates from datatable where place.country_code is not null AND (text like '%Corona%' OR text like '%corona%' OR text like '%coronavirus%' OR text like '%Coronavirus%')")
    total_tweets_data.registerTempTable("totaltweetsdata")
    print("tweets filtered")
    # # query1()
    # # query2()
    # # query3()
    # # query4()
    # # query5()
    # query6()
    # # query7()
    query8()
    # query9()
    # # query10()

    sc.stop()
    print("PySpark completed")






