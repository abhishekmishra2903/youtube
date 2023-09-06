# importing necessary libraries

import pandas as pd
import streamlit as st
import pymongo
import mysql.connector

# establishing sql connection

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="5115269000",
auth_plugin='mysql_native_password'
)
mycursor = mydb.cursor()
mycursor.execute("set autocommit=1")
mycursor.execute('USE youtube')

# stocking api keys, and establishing connection

# kindly generate an api_key from your account and put it in the line below.
api_key='***************************************'

from googleapiclient.discovery import build
youtube=build('youtube','v3',developerKey=api_key)

# streamlit page set-up
st.header(':film_frames: Youtube data warehousing and querying')


# displaying the channel details that are existing in sql database

channel_id_list=[]
mycursor.execute("select channel_id,channel_name from channel_data")
for i in mycursor:
    channel_id_list.append({'channel_id':i[0],'channel_name':i[1]})
default=pd.DataFrame(channel_id_list)
st.sidebar.write('Channels present in database')
st.sidebar.dataframe(default)

# taking input of channel id from the user

channel_id_input=st.sidebar.text_input('Enter channel_id to load data into Mongo server',value='UCU5WVGehJ1m-58HpynrFnaA')

# Defining a switch so that the fetching and updation of data on mongo do not 
# happen if the data is already saved once

client = pymongo.MongoClient("mongodb://localhost:27017")
db=client.youtube
collection=db.channel_data
do_mongo=True
x=collection.find()
for i in x:
    if i['channel_id']==channel_id_input:
        do_mongo=False

        
if do_mongo==True:
    
# defining function to collect channel data and return a dictionary of the data

    def get_info(youtube,channel_id_input):
        request=youtube.channels().list(
        part='snippet,contentDetails,statistics,status,brandingSettings,contentOwnerDetails,id,localizations',
        id=channel_id_input)
        response=request.execute()
        data=dict(channel_id=channel_id_input,
                channel_name=response['items'][0]['snippet']['title'],
                channel_views=response['items'][0]['statistics']['viewCount'],
                channel_description=response['items'][0]['snippet']['description'],
                channel_status=response['items'][0]['status']['privacyStatus'])
        return data
 
# calling the function with the input channel id and inserting obtained data in mongodb

    channel_data=get_info(youtube,channel_id_input)
    collection.insert_many([channel_data])
    
# defining function to collect playlist data and return a dictionary of the data
    
    def playlist_info(youtube,channel_ids):
        request=youtube.channels().list(
        part='contentDetails,snippet',
        id=channel_ids)
        response=request.execute()
        
        data=dict(playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                  channel_id=channel_ids,
                  playlist_name=response['items'][0]['snippet']['title'])
        return data

# calling the function with input channel id and inserting obtained data in mongodb
    
    playlist_data=playlist_info(youtube,channel_id_input)
    collection=db.playlist_data
    collection.insert_many([playlist_data])
    
# video list of a playlist is available in playlistItems so we fetch the video_ids
# Later with the help of this list we will be obtaining video details.

    video_ids=[]
    def get_video_ids(youtube,playlist_id):
        request=youtube.playlistItems().list(
        part='contentDetails',
        playlistId=playlist_id,
        maxResults=50)
        response=request.execute()
        
        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
        next_page_token=response.get('nextPageToken')
        more_pages=True
        
        while more_pages:
            if next_page_token is None:
                more_pages=False
            else:
                request=youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId=playlist_id,
                        maxResults=50,
                        pageToken=next_page_token)
                response=request.execute()
                for i in range(len(response['items'])):
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])
                next_page_token=response.get('nextPageToken')
        
    get_video_ids(youtube,playlist_data['playlist_id'])
        
# fetching video data
    
    def get_video_details(youtube,ids):
        video_stats=[]
        
        for i in range(0,len(ids),50):
            request=youtube.videos().list(
            part='contentDetails,snippet, statistics',
            id=','.join(ids[i:i+50]))
            response=request.execute()
            
            for videos in response['items']:
                data=dict(video_id=videos['id'],
                                channel_id=videos['snippet']['channelId'],
                                video_name=videos['snippet']['title'],
                                video_description=videos['snippet']['description'],
                                published_date=videos['snippet']['publishedAt'],
                                view_count=videos['statistics']['viewCount'],
                                like_count=videos['statistics'].get('likeCount'),
                                comment_count=videos['statistics'].get('commentCount'),
                                duration=videos['contentDetails']['duration'],
                                thumbnail=videos['snippet']['thumbnails']['default']['url'],
                                caption_status=videos['contentDetails']['caption'])
                video_stats.append(data)
        return video_stats
    
    video_data=get_video_details(youtube,video_ids)
    
# replacing None with 0 in comment_count and then uploading data into mongodb

    for i in range(len(video_data)):
        if video_data[i]['comment_count']==None:
            video_data[i]['comment_count']=0
    collection=db.video_data
    collection.insert_many(video_data)
    
# fetching comment data
    
    comment_data=[]
    def get_comment_data(youtube,id):
        request=youtube.commentThreads().list(
        part='snippet',
        videoId=id,
        maxResults=100)
        response=request.execute()
        
        for i in range(len(response['items'])):
            data=dict(
            comment_id=response['items'][i]['snippet']['topLevelComment']['id'],
            video_id=response['items'][i]['snippet']['topLevelComment']['snippet'].get('videoId'),
            comment_text=response['items'][i]['snippet']['topLevelComment']['snippet'].get('textDisplay'),
            comment_author=response['items'][i]['snippet']['topLevelComment']['snippet'].get('authorDisplayName'),
            comment_published_date=response['items'][i]['snippet']['topLevelComment']['snippet'].get('publishedAt'))
            comment_data.append(data)
        next_page_token=response.get('nextPageToken')
        more_pages=True
        
        while more_pages:
            if next_page_token is None:
                more_pages=False
            else:
                request=youtube.commentThreads().list(
                part='snippet',
                videoId=id,
                maxResults=100,
                pageToken=next_page_token)
                response=request.execute()
                for i in range(len(response['items'])):
                    data=dict(
                        comment_id=response['items'][i]['snippet']['topLevelComment']['id'],
                        video_id=response['items'][i]['snippet']['topLevelComment']['snippet'].get('videoId'),
                        comment_text=response['items'][i]['snippet']['topLevelComment']['snippet'].get('textDisplay'),
                        comment_author=response['items'][i]['snippet']['topLevelComment']['snippet'].get('authorDisplayName'),
                        comment_published_date=response['items'][i]['snippet']['topLevelComment']['snippet'].get('publishedAt'))
                    comment_data.append(data)
                next_page_token=response.get('nextPageToken')

# from video_ids, removing the ids for which comment is turned off. The passing each 
# video id in the above function to append the comment data into comment_data. Then
# uploading the data into mongodb   

    for i in range(len(video_data)):
        if video_data[i]['comment_count']==0 or video_data[i]['comment_count']=='0':
            video_ids.remove(video_data[i]['video_id'])
    for i in video_ids:
        get_comment_data(youtube,i)
    collection=db.comment_data
    collection.insert_many(comment_data)
    
# storing the video_ids in video_id_list table in sql which will be required for easy
# transfer of data from mongodb to sql 
    
    video_id_sql=[[i] for i in video_ids]
    sql="insert into video_id_list(video_id) values (%s)"
    mycursor.executemany(sql,video_id_sql)

# creating a sql switch just like mongodb, so that repetition can be avoided during re-run.

do_sql=True
for i in channel_id_list:
    if i['channel_id']==channel_id_input:
        do_sql=False
        
if do_sql==True:
    
# defining button to start the transfer process   
    
    sql_switch=st.sidebar.button('Transfer data to SQL server')
    if sql_switch==True:
        
# uploading data in channel_data table        
        
        channel_dict=[]
        collection=db.channel_data
        x=collection.find({'channel_id':channel_id_input})
        for i in x:
            channel_dict.append([i['channel_id'],i['channel_name'],i['channel_views'],i['channel_description'],i['channel_status']])
        sql="insert into channel_data(channel_id,channel_name,channel_views,channel_description,channel_status) values(%s,%s,%s,%s,%s)"
        mycursor.executemany(sql,channel_dict)

# uploading data in playlist_data table
    
        playlist_dict=[]
        collection=db.playlist_data
        x=collection.find({'channel_id':channel_id_input})
        for i in x:
            playlist_dict.append([i['playlist_id'],i['channel_id'],i['playlist_name']])
        sql="insert into playlist_data(playlist_id,channel_id,playlist_name) values(%s,%s,%s)"
        mycursor.executemany(sql,playlist_dict)

# uploading data in video_data table
    
        video_dict=[]
        collection=db.video_data
        x=collection.find({'channel_id':channel_id_input})
        for i in x:
            video_dict.append([i['video_id'],i['channel_id'],i['video_name'], i['video_description'], i['published_date'],i['view_count'],i['like_count'],i['comment_count'] ,i['duration'] ,i['thumbnail'],i['caption_status']])
        
# replacing None with 0
        
        for i in video_dict:
            for k in range(11):
                if i[k]==None:
                    i.insert(k+1,0)
                    i.pop(k)
                    
# to extract relevant datetime format for sql
        
        for i in video_dict:
            s=i[4][:10]+' '+i[4][11:19]
            i.insert(5,s)
            i.pop(4)
            
#convert duration in second

        def to_sec(s):
            
# some entries are in unusual format like P0D,             
            if s[1]!='T':
                return 0
            
# some entries are having hour entries. We are considering hour duration upto 2 digits.
           
            elif (len(s)>3 and s[3]=='H') or (len(s)>4 and s[4]=='H') :
                l1=[*s]
                l1.remove('P')
                l1.remove('T')
                if l1[1]=='H':
                    hour=int(l1[0])
                elif l1[2]=='H':
                    hour=int(s[2:4])
                if 'M' in l1:
                    if l1.index('M')-l1.index('H')==2:
                        minute=int(l1[l1.index('H')+1])
                    elif l1.index('M')-l1.index('H')==3:
                        minute=int(l1[l1.index('H')+1]+l1[l1.index('H')+2])
                if 'M' in l1 and 'S' not in l1:
                    return (hour*60*60)+(minute*60)
                if 'S' in l1 and 'M' in l1:
                    if l1.index('S')-l1.index('M')==2:
                        second=int(l1[l1.index('M')+1])
                    elif l1.index('S')-l1.index('M')==3:
                        second=int(l1[l1.index('M')+1]+l1[l1.index('M')+2])
                    return (hour*60*60)+(minute*60)+second
                elif 'S' in l1 and 'M' not in l1:
                    if l1.index('S')-l1.index('H')==2:
                        second=int(l1[l1.index('H')+1])
                    elif l1.index('S')-l1.index('H')==3:
                        second=int(l1[l1.index('H')+1]+l1[l1.index('H')+2])
                    return (hour*60*60)+second

# for entries without hour value
    
            else:
                if len(s)==4 and s[-1]=='M':
                    return int(s[2:3])*60
                elif len(s)==5 and s[-1]=='M':
                    return int(s[2:4])*60
                elif len(s)==4 and s[-1]=='S':
                    return int(s[2])
                elif len(s)==5 and s[-1]=='S':
                    return int(s[2:4])
                else:
                    if s[3]=='M':
                        minute=int(s[2])
                    else:
                        minute=int(s[2:4])
                    if s[-3]=='M':
                        second=int(s[-2])
                    else:
                        second=int(s[-3:-1])
                    return (minute * 60)+second 
        for i in video_dict:
            i.insert(9,to_sec(i[8]))
            i.pop(8)     
        sql="insert into video_data(video_id,channel_id,video_name, video_description, published_date,view_count,like_count,comment_count ,duration ,thumbnail,caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"      
        mycursor.executemany(sql,video_dict)

# fetching video_ids that we had stored earlier. These are the ids for which relevant
# comment data is available
    
        video_id_list=[]
        mycursor.execute('select * from video_id_list')
        for i in mycursor:
            video_id_list.append(i[0])
        comment_dict=[]
        collection=db.comment_data
        for j in video_id_list:
            x=collection.find({'video_id':j})
            for i in x:
                comment_dict.append([i['comment_id'],i['video_id'],i['comment_text'],i['comment_author'],i['comment_published_date']])
        
# Putting date and time into appropriate sql format        
        
        for i in comment_dict:
            s=i[4][:10]+' '+i[4][11:19]
            i.insert(5,s)
            i.pop(4)
            
        sql="insert into comment_data(comment_id,video_id,comment_text,comment_author,comment_published_date) values(%s,%s,%s,%s,%s)"
        mycursor.executemany(sql,comment_dict)
        
# truncating the video_id_list table as it is needed to be updated with fresh video_ids
# each time we add data from a new channel.      
        
        mycursor.execute("truncate table video_id_list") 

# listing the queries

q0='Select any query'       
q1='What are the names of all the videos and their corresponding channels?'
q2='Which channels have the most number of videos, and how many videos do they have?'
q3='What are the top 10 most viewed videos and their respective channels?'
q4='How many comments were made on each video, and what are their corresponding video names?'
q5='Which videos have the highest number of likes, and what are their corresponding channel names?'
q6='What is the total number of likes and dislikes for each video, and what are their corresponding video names?'
q7='What is the total number of views for each channel, and what are their corresponding channel names?'
q8='What are the names of all the channels that have published videos in the year 2022?'
q9='What is the average duration of all videos in each channel, and what are their corresponding channel names?'
q10='Which videos have the highest number of comments, and what are their corresponding channel names?'
query_list=[q0,q1,q2,q3,q4,q5,q6,q7,q8,q9,q10]

# creating dropdown in streamlit

selection=st.selectbox('select the query',query_list)

# sql for query 1

if selection==q1:
    mycursor.execute("select video_data.video_name, channel_data.channel_name from video_data left join channel_data on video_data.channel_id=channel_data.channel_id")

    demo=[]
    for x in mycursor:
        demo.append(dict(video_name=x[0],channel_name=x[1]))
    
    result1=pd.DataFrame(demo)
    st.dataframe(result1)

# sql for query 2

if selection==q2:
    mycursor.execute("select channel_id from channel_data")
    
# fetching list of channels    
    
    channel_list=[]
    for i in mycursor:
        channel_list.append(i[0])
        
# appending video_count list with video counts and name with names        
        
    video_count=[]
    name=[]
    sql="select count(channel_id) from video_data where video_data.channel_id=(%s)"
    for i in channel_list:
        mycursor.execute(sql,[i])
        for i in mycursor:
            video_count.append(i[0])
    
# Selecting the maximum of video_count and forming dataframe with corresponding data     
    
    sql1="select channel_name from channel_data where channel_id=(%s)"
    mycursor.execute(sql1,[channel_list[video_count.index(max(video_count))]])
    for i in mycursor:
        name.append(i[0])
    result2=pd.DataFrame([{'Channel_name':name[0],'video_count':max(video_count)}])
    st.dataframe(result2)

# sql for query 3

if selection==q3:
    demo3=[]
    mycursor.execute("select channel_data.channel_name,video_data.video_name, video_data.view_count from video_data left join channel_data on channel_data.channel_id=video_data.channel_id order by view_count desc  limit 10")
    for x in mycursor:
        demo3.append(dict(channel_name=x[0],video_name=x[1],view_count=x[2]))
    result3=pd.DataFrame(demo3)
    st.dataframe(result3)
    
# sql for query 4

if selection==q4:
    
# collecting video_ids with comment data    
    
    video_ids=[]
    mycursor.execute("select distinct video_id from comment_data")
    for i in mycursor:
        video_ids.append(i[0])
        
# updating list comment_count_list with comment count of each video.       
        
    sql="select count(video_id) from comment_data where comment_data.video_id=(%s)"
    comment_count_list=[]
    for i in video_ids:
        mycursor.execute(sql,[i])
        for k in mycursor:
            comment_count_list.append(k[0])
            
# forming list of video_name            
            
    video_name=[]
    sql='select video_name from video_data where video_id=(%s)'
    for i in video_ids:
        mycursor.execute(sql,[i])
        for k in mycursor:
            video_name.append(k[0])
            
# forming dictionary from the above two lists and forming dataframe to be displayed            
            
    demo4=[]
    for i in range(len(video_ids)):
        demo4.append(dict(video_names=video_name[i],comment_count=comment_count_list[i]))
    result4=pd.DataFrame(demo4)
    st.dataframe(result4)

# sql for query 5
    
if selection==q5:
    demo5=[]
    mycursor.execute("select video_data.video_name,channel_data.channel_name,video_data.like_count from video_data left join channel_data on channel_data.channel_id=video_data.channel_id order by like_count desc limit 1")
    for i in mycursor:
        demo5.append(dict(video_name=i[0],channel_name=i[1],like_count=i[2]))
        
    result5=pd.DataFrame(demo5)
    st.dataframe(result5)

# sql for query 6

if selection==q6:
    mycursor.execute("select video_name,like_count from video_data")
    demo6=[]
    for i in mycursor:
        demo6.append(dict(video_name=i[0],like_count=i[1]))
    result6=pd.DataFrame(demo6)
    st.dataframe(result6)
    
# sql for query 7

if selection==q7:
    mycursor.execute("select channel_name,channel_views from channel_data")
    demo7=[]
    for i in mycursor:
        demo7.append(dict(channel_name=i[0],channel_views=i[1]))
    result7=pd.DataFrame(demo7)
    st.dataframe(result7)

# sql for query 8

if selection==q8:
    mycursor.execute("select channel_data.channel_name from video_data right join channel_data on channel_data.channel_id=video_data.channel_id where year(published_date) = 2022")
    demo8=[]
    for i in mycursor:
        demo8.append(i[0])
        
# since there can be repition of channel_name if it has multiple videos published in 2022,
# we apply list(set()). Then we convert to dataframe and display.        
        
    dummy=set(demo8)
    demo8=list(dummy)
    demo8_dict=[]
    for i in demo8:
        demo8_dict.append(dict(channel_name=i))
    result8=pd.DataFrame(demo8_dict)
    st.dataframe(result8)

# sql for query 9

if selection==q9:
    mycursor.execute("select channel_data.channel_name, avg(duration) from video_data left join channel_data on channel_data.channel_id=video_data.channel_id group by channel_name")
    demo9=[]
    for i in mycursor:
        demo9.append(dict(channel_name=i[0],avg_video_duration_seconds=i[1]))
    result9=pd.DataFrame(demo9)
    st.dataframe(result9)

# sql for query 10 

if selection==q10:
    demo10=[]
    mycursor.execute("select video_data.video_name,channel_data.channel_name,video_data.comment_count from video_data left join channel_data on channel_data.channel_id=video_data.channel_id order by comment_count desc limit 1")
    for i in mycursor:
        demo10.append(dict(video_name=i[0],channel_name=i[1],comment_count=i[2]))
        
    result10=pd.DataFrame(demo10)
    st.dataframe(result10)
