import pandas as pd
import streamlit as st
import pymongo
import mysql.connector
channel_list=['UCLEL4SHIKwnOkjY0hVcLFeQ']

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

api_key='AIzaSyB7JHLS2gNXvr57RQ0wbnzXGGRoKmY3SvQ'

# streamlit page set-up
st.header('Youtube data warehousing and querying')
channel_id_input=st.text_input('Enter upto 10 comma separated channel_ids',value='UCUKOyIIDIaLLT8T-ce636Qw,UCU5WVGehJ1m-58HpynrFnaA')
channel_list=str(channel_id_input).split(',')
channel_list=str(channel_id_input).split(',')

# fetching channel data

from googleapiclient.discovery import build
youtube=build('youtube','v3',developerKey=api_key)


def get_info(youtube,channel_ids):
    all_data=[]
    request=youtube.channels().list(
    part='snippet,contentDetails,statistics,status,brandingSettings,contentOwnerDetails,id,localizations',
    id=','.join(channel_ids))
    response=request.execute()

    for i in range(len(response['items'])):
        data=dict(channel_id=channel_ids[i],
                channel_name=response['items'][i]['snippet']['title'],
                channel_views=response['items'][i]['statistics']['viewCount'],
                channel_description=response['items'][i]['snippet']['description'],
                channel_status=response['items'][i]['status']['privacyStatus'])
        all_data.append(data)
    return all_data
channel_data=get_info(youtube,channel_list)

# fetching playlist data

def playlist_info(youtube,channel_ids):
    playlist_data=[]
    request=youtube.channels().list(
    part='contentDetails,snippet',
    id=','.join(channel_ids))
    response=request.execute()
    
    for i in range(len(response['items'])):
        data=dict(playlist_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                  channel_id=channel_ids[i],
                  playlist_name=response['items'][i]['snippet']['title'])
        playlist_data.append(data)
    return playlist_data

playlist_data=playlist_info(youtube,channel_list)
df_playlist=pd.DataFrame(playlist_data)

# get list of video ids from playlist ids
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
    
for i in df_playlist['playlist_id'].tolist():
    get_video_ids(youtube,i)
    
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

# fetching comment data

comment_info=[]
def get_comment_data(youtube,id):
    request=youtube.commentThreads().list(
    part='snippet',
    videoId=id,
    maxResults=100)
    response=request.execute()
    
    for i in range(len(response['items'])):
        data=dict(
        comment_id=response['items'][i]['snippet']['topLevelComment']['id'],
        video_id=response['items'][i]['snippet']['topLevelComment']['snippet']['videoId'],
        comment_text=response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
        comment_author=response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
        comment_published_date=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'])
        comment_info.append(data)
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
                    video_id=response['items'][i]['snippet']['topLevelComment']['snippet']['videoId'],
                    comment_text=response['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                    comment_author=response['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    comment_published_date=response['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'])
                comment_info.append(data)
            next_page_token=response.get('nextPageToken')
for i in video_ids:
    get_comment_data(youtube,i)
    
# deleting previous data if any and uploading new data to mongodb altas
    
mongo_switch=st.button('Fetch data into MongoDB')
if mongo_switch==True:
    client = pymongo.MongoClient("mongodb+srv://abhishek:5115269000@abhishekmishra.qsvrhj7.mongodb.net/")
    db=client.youtube
    collection=db.info
    if collection.estimated_document_count()!=0:
        collection.drop()
    collection.insert_many(channel_data)
    collection.insert_many(playlist_data)
    collection.insert_many(video_data)
    collection.insert_many(comment_info)
    
# deleting any pre-existing table to optimise space utilisation

sql_switch=st.button('Transfer data to sql')
if sql_switch==True:
    table_list=[]
    mycursor.execute('show tables')
    for i in mycursor:
        table_list.append(i[0])
    if 'channel_data' in table_list:
        mycursor.execute('drop table channel_data')
    if 'video_data' in table_list:
        mycursor.execute('drop table video_data')
    if 'playlist_data' in table_list:
        mycursor.execute('drop table playlist_data')
    if 'comment_data' in table_list:
        mycursor.execute('drop table comment_data')
        
# creating table with channel data
    
    mycursor.execute("CREATE TABLE channel_data (channel_id VARCHAR(255) ,channel_name VARCHAR(255),channel_views INT,channel_description TEXT, channel_status VARCHAR(255))")
    
    sql="insert into channel_data(channel_id,channel_name,channel_views,channel_description,channel_status) values(%s,%s,%s,%s,%s)"
    
    list1=[]
    for i in channel_data:
        list1.append(tuple(i.values()))
    
    mycursor.executemany(sql,list1)
    
# creating table with playlist data
    
    mycursor.execute("CREATE TABLE playlist_data (playlist_id VARCHAR(255) ,channel_id VARCHAR(255),playlist_name VARCHAR(255))")
    
    sql="insert into playlist_data(playlist_id,channel_id,playlist_name) values(%s,%s,%s)"
    
    list2=[]
    for i in playlist_data:
        list2.append(list(i.values()))
    
    mycursor.executemany(sql,list2)
    
# creating table with video data

    mycursor.execute("CREATE TABLE video_data (video_id VARCHAR(255),channel_id VARCHAR(255),video_name VARCHAR(255),video_description TEXT,published_date DATETIME,view_count INT,like_count INT,comment_count INT,duration INT,thumbnail VARCHAR(255),caption_status VARCHAR(255))")
    sql="insert into video_data(video_id,channel_id,video_name, video_description, published_date,view_count,like_count,comment_count ,duration ,thumbnail,caption_status) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    
    list4=[]
    for i in video_data:
        list4.append(list(i.values()))
        
    # replacing None with 0
    for i in list4:
        for k in range(11):
            if i[k]==None:
                i.insert(k+1,0)
                i.pop(k)
                
    # to extract relevant datetime format for sql
    for i in list4:
        s=i[4][:10]+' '+i[4][11:19]
        i.insert(5,s)
        i.pop(4)
    
    #convert duration in second
    def to_sec(s):
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
    
    for i in list4:
        i.insert(9,to_sec(i[8]))
        i.pop(8)
        
    mycursor.executemany(sql,list4)
    
# creating table with comment data
    mycursor.execute("CREATE TABLE comment_data (comment_id VARCHAR(255) ,video_id VARCHAR(255),comment_text TEXT,comment_author VARCHAR(255), comment_published_date DATETIME)")
    sql="insert into comment_data(comment_id,video_id,comment_text,comment_author,comment_published_date) values(%s,%s,%s,%s,%s)"
    
    list5=[]
    for i in comment_info:
        list5.append(list(i.values())[0:5])
    
    for i in list5:
        s=i[4][:10]+' '+i[4][11:19]
        i.insert(5,s)
        i.pop(4)
    
    mycursor.executemany(sql,list5)
    
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
    
    video_count=[]
    name=[]
    sql="select count(channel_id) from video_data where video_data.channel_id=(%s)"
    for i in channel_list:
        mycursor.execute(sql,[i])
        for i in mycursor:
            video_count.append(i[0])
    
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
    sql="select count(video_id) from comment_data where comment_data.video_id=(%s)"
    comment_count_list=[]
    for i in video_ids:
        mycursor.execute(sql,[i])
        for k in mycursor:
            comment_count_list.append(k[0])
    demo4=[]
    for i in range(len(video_ids)):
        demo4.append(dict(video_names=video_data[i].get('video_name'),comment_count=comment_count_list[i]))
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






