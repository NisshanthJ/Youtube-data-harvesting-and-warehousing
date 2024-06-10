import streamlit as st
from PIL import Image
from streamlit_option_menu import option_menu
import plotly.express as px

import googleapiclient.discovery
from googleapiclient.errors import HttpError


import pandas as pd
import re

import mysql.connector
from sqlalchemy import create_engine


api_service_name = "youtube"
api_version = "v3"
api_Key="API KEY"
youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_Key)


mydb = mysql.connector.connect(host="localhost",user="root",password="")
mycursor = mydb.cursor(buffered=True)
engine = create_engine("mysql+mysqlconnector://root:@localhost/youtube")

mycursor.execute('create database if not exists youtube')
mycursor.execute('use youtube')


with st.sidebar:
    selected =option_menu("Youtube data harvesting and warehousing using SQL and Streamlit",
                        ["Data collection and upload","MYSQL Database","SQL queries output"],
                        menu_icon="menu-up",
                        orientation="vertical")

def channel_details(channel_id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_id)
    response = request.execute()

    for i in response['items']:
        channel_data= dict(
            channel_name=i['snippet']['title'],
            Channel_id=i["id"],
            channel_Description=i['snippet']['description'],
            channel_Thumbnail=i['snippet']['thumbnails']['default']['url'],
            channel_playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
            channel_subscribers=i['statistics']['subscriberCount'],
            channel_video_count=i['statistics']['videoCount'],
            channel_views=i['statistics']['viewCount'],
            channel_publishedat=i['snippet']['publishedAt'])
    return (channel_data)

def playlist_details(channel_id):
    playlist_info=[]
    nextPageToken=None
    try:
        while True:
            request = youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=nextPageToken
                    )
            response = request.execute()
        
            for i in response['items']:
                data=dict(
                    playlist_id=i['id'],
                    playlist_name=i['snippet']['title'],
                    publishedat=i['snippet']['publishedAt'],
                    channel_ID=i['snippet']['channelId'],
                    channel_name=i['snippet']['channelTitle'],
                    videoscount=i['contentDetails']['itemCount'])
                playlist_info.append(data)
                nextPageToken=response.get('nextPageToken')
            if nextPageToken is None:
                break
    except HttpError as e:
        error_message = f"Error retrieving playlists: {e}"   
        st.error(error_message)
    return (playlist_info)

def fetch_video_ids(channel_id):
    response= youtube.channels().list( part="contentDetails",
                                        id=channel_id).execute()
    playlist_videos=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token=None
    
    videos_ids=[]
    
    while True:
        response1=youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_videos,
            maxResults=50,
            pageToken=next_page_token).execute()
        
        for i in range (len(response1['items'])):
            videos_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break
    return (videos_ids)

def video_details(video_IDS):
    video_info=[]
    for video_id in video_IDS:
        response= youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_id).execute()
        
        for i in response['items']:
                data=dict(
                        channel_id=i['snippet']['channelId'],
                        video_id=i['id'],
                        video_name=i['snippet']['title'],
                        video_Description=i['snippet']['description'],
                        Thumbnail=i['snippet']['thumbnails']['default']['url'],
                        Tags=i['snippet'].get('tags'),
                        publishedAt=i['snippet']['publishedAt'],
                        Duration=convert_time_duration(i['contentDetails']['duration']),
                        View_Count=i['statistics']['viewCount'],
                        Like_Count=i['statistics'].get('likeCount'),
                        Favorite_Count=i['statistics'].get('favoriteCount'),
                        Comment_Count=i['statistics']['commentCount'],
                        Caption_Status=i['contentDetails']['caption'] 
                        )
                video_info.append(data)
    return(video_info)

def convert_time_duration(duration): 
        regex = r'PT(\d+H)?(\d+M)?(\d+S)?'
        match = re.match(regex, duration)
        if not match:
                return '00:00:00'
        hours, minutes, seconds = match.groups()
        hours = int(hours[:-1]) if hours else 0
        minutes = int(minutes[:-1]) if minutes else 0
        seconds = int(seconds[:-1]) if seconds else 0
        total_seconds = hours * 3600 + minutes * 60 + seconds
        return '{:02d}:{:02d}:{:02d}'.format(int(total_seconds / 3600), int((total_seconds % 3600) / 60), int(total_seconds % 60))

def comments_details(video_IDS):
    comments_info=[]
    try:
        for video_id in video_IDS:
            request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100)
            response = request.execute()

            for i in response.get('items',[]):
                data=dict(
                            video_id=i['snippet']['videoId'],
                            comment_id=i['snippet']['topLevelComment']['id'],
                            comment_text=i['snippet']['topLevelComment']['snippet']['textDisplay'],
                            comment_author=i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            comment_publishedat=i['snippet']['topLevelComment']['snippet']['publishedAt'])
                comments_info.append(data)
    except HttpError as e:
        if e.resp.status == 403 and e.error_details[0]["reason"] == 'commentsDisabled':
                st.error("comments diabled for some videos")
    return (comments_info)


if selected == "Data collection and upload":
    st.subheader(':green[Data collection and upload]')
    channel_ID = st.text_input("**Enter the channel ID here :**")
    
    if st.button("View details"):
        with st.spinner('Extraction in progress...'):
            try:
                extracted_details = channel_details(channel_id=channel_ID)
                st.write('**:green[Channel Thumbnail]** :')
                st.image(extracted_details.get('channel_Thumbnail'))
                st.write('**:green[Channel Name]** :', extracted_details['channel_name'])
                st.write('**:green[Description]** :', extracted_details['channel_Description'])
                st.write('**:green[Total_Videos]** :', extracted_details['channel_video_count'])
                st.write('**:green[Subscriber Count]** :', extracted_details['channel_subscribers'])
                st.write('**:green[Total Views]** :', extracted_details['channel_views'])
            except HttpError as e:
                if e.resp.status == 403 and e.error_details[0]["reason"] == 'quotaExceeded':
                    st.error(" API Quota exceeded. Please try again later.")
            except:
                st.error("Channel ID is invalid.Kindly check !!!")
            
    
    if st.button("Upload to MYSQL database"):

        with st.spinner('Uploading in progress...'):
            try:
             
                mycursor.execute('''create table if not exists channel( channel_name VARCHAR(100) ,
                                channel_id VARCHAR(50) PRIMARY KEY,channel_Description VARCHAR(1000),channel_Thumbnail VARCHAR(100),
                                channel_playlist_id VARCHAR(50),channel_subscribers BIGINT,channel_video_count BIGINT,
                                channel_views BIGINT,channel_publishedat DATETIME)''')
         
                mycursor.execute('''create table if not exists playlist(playlist_id VARCHAR(50) PRIMARY KEY,playlist_name VARCHAR(100),
                                publishedat DATETIME,channel_id VARCHAR(50),channel_name VARCHAR(100),videoscount BIGINT)''')
                
      
                mycursor.execute('''create table if not exists videos(channel_id VARCHAR(50),video_id VARCHAR(50)primary key,
                                video_name VARCHAR(100),video_Description VARCHAR(500),Thumbnail VARCHAR(100),Tags VARCHAR(250),
                                publishedAt DATETIME,Duration VARCHAR(10),View_Count BIGINT,Like_Count BIGINT,Favorite_Count BIGINT,
                                Comment_Count BIGINT,Caption_Status VARCHAR(10),
                                FOREIGN KEY (channel_id) REFERENCES channel(channel_id))''')
            
                mycursor.execute('''create table if not exists comments(video_id VARCHAR(50),comment_id VARCHAR(50),comment_text TEXT,
                                comment_author VARCHAR(50),comment_publishedat DATETIME,FOREIGN KEY (video_id) REFERENCES videos(video_id))''')
              
                df_channel=pd.DataFrame(channel_details(channel_id=channel_ID),index=[0])
                df_playlist=pd.DataFrame(playlist_details(channel_id=channel_ID))
                df_videos=pd.DataFrame(video_details(video_IDS= fetch_video_ids(channel_id=channel_ID)))
                df_comments=pd.DataFrame(comments_details(video_IDS=fetch_video_ids(channel_id=channel_ID)))
                
                df_channel.to_sql('channel',engine,if_exists='append',index=False)
                df_playlist.to_sql('playlist',engine,if_exists='append',index=False)
                df_videos['Tags'] = df_videos['Tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')
                df_videos.to_sql('videos',engine,if_exists='append',index=False)
                df_comments.to_sql('comments',engine,if_exists='append',index=False)
                mydb.commit()
                st.success('channel information,playlists,videos,comments are uploaded successfully')
            except :
                st.error('channel already uploaded or exist in MYSQL Database')

def fetch_channel_names():
    mycursor.execute("SELECT channel_name FROM channel")
    channel_names = [row[0] for row in mycursor.fetchall()]
    return channel_names

def fetch_channel_data(channel_name):

    mycursor.execute("SELECT * FROM channel WHERE channel_name = %s", (channel_name,))
    out= mycursor.fetchall()
    channel_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    channel_df.index +=1

    mycursor.execute("SELECT * FROM playlist WHERE channel_id = %s", (channel_df['channel_id'].iloc[0],))
    out = mycursor.fetchall()
    playlists_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    playlists_df.index +=1


    mycursor.execute("SELECT * FROM videos WHERE channel_id = %s", (channel_df['channel_id'].iloc[0],))
    out= mycursor.fetchall()
    videos_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    videos_df.index +=1

    mycursor.execute("SELECT * FROM comments WHERE video_id IN (SELECT video_id FROM videos WHERE channel_id = %s)",
                    (channel_df['channel_id'].iloc[0],))
    out = mycursor.fetchall()
    comments_df = pd.DataFrame(out, columns=[i[0] for i in mycursor.description]).reset_index(drop=True)
    comments_df.index +=1

    return channel_df,playlists_df,videos_df,comments_df

if selected =="MYSQL Database":
    st.subheader(':green[MYSQL Database]')
    st.markdown('''The playlist,video,comments and channels details for the respectively selected channel is listed below''')
    try:
        channel_names = fetch_channel_names()
        selected_channel = st.selectbox(':green[Select Channel]', channel_names) 
    
        if selected_channel:
            channel_info,playlist_info,videos_info,comments_info = fetch_channel_data(selected_channel)

        st.subheader(':green[Channel Table]')
        st.write(channel_info)
        st.subheader(':green[Playlists Table]')
        st.write(playlist_info)
        st.subheader(':green[Videos Table]')
        st.write(videos_info)
        st.subheader(':green[Comments Table]')
        st.write(comments_info)
    except:
        st.error('Database is empty ')

def q_1():
    mycursor.execute('''SELECT channel.channel_name,videos.video_name
                        FROM videos 
                        JOIN channel ON channel.Channel_id = videos.Channel_id
                        ORDER BY channel_name''')
    out=mycursor.fetchall()
    Q1= pd.DataFrame(out, columns=['Channel Name','Videos Name']).reset_index(drop=True)
    Q1.index +=1
    st.dataframe(Q1)

def q_2():
    mycursor.execute('''SELECT DISTINCT channel_name,COUNT(videos.video_id) as Total_Videos 
                        FROM channel 
                        JOIN videos on Channel.channel_id = videos.channel_id
                        GROUP BY channel_name 
                        ORDER BY Total_videos DESC''')
    out=mycursor.fetchall()
    Q2= pd.DataFrame(out, columns=['Channel Name','Total Videos']).reset_index(drop=True)
    Q2.index +=1
    st.dataframe(Q2)

def q_3():
    mycursor.execute('''SELECT channel.Channel_name,videos.Video_name, videos.View_Count as Total_Views
                        FROM videos
                        JOIN channel ON channel.Channel_id = videos.Channel_id
                        ORDER BY videos.View_Count DESC
                        LIMIT 10;''')
    out=mycursor.fetchall()
    Q3= pd.DataFrame(out, columns=['Channel Name','Videos Name','Total Views']).reset_index(drop=True)
    Q3.index +=1
    st.dataframe(Q3)

def q_4():
    mycursor.execute('''SELECT videos.video_name,videos.comment_count as Total_Comments
                    FROM videos
                    ORDER BY videos.comment_count DESC''')
    out=mycursor.fetchall()
    Q4= pd.DataFrame(out, columns=['Videos Name','Total Comments']).reset_index(drop=True)
    Q4.index +=1
    st.dataframe(Q4)


def q_5():
    mycursor.execute('''SELECT channel.channel_name,videos.video_name,videos.like_count as Highest_likes FROM videos 
                    JOIN channel ON videos.channel_id=channel.channel_id
                    WHERE like_count=(SELECT MAX(videos.like_count) FROM videos v WHERE videos.channel_id=v.channel_id
                    GROUP BY channel_id)
                    ORDER BY Highest_likes DESC''')
    out=mycursor.fetchall()
    Q5= pd.DataFrame(out, columns=['Channel Name','Videos Name','Likes']).reset_index(drop=True)
    Q5.index +=1
    st.dataframe(Q5)    


def q_6():
    mycursor.execute('''SELECT videos.video_name,videos.like_count as Likes
                    FROM videos
                    ORDER BY videos.like_count DESC''')
    out=mycursor.fetchall()
    Q6= pd.DataFrame(out, columns=['Videos Name','Likes']).reset_index(drop=True)
    Q6.index +=1
    st.dataframe(Q6)


def q_7():
    mycursor.execute('''SELECT channel.channel_name,channel.channel_views as Total_views
                    FROM channel
                    ORDER BY channel.channel_views DESC  ''')
    out=mycursor.fetchall()
    Q7= pd.DataFrame(out, columns=['Channel Name','Total views']).reset_index(drop=True)
    Q7.index +=1
    st.dataframe(Q7)


def q_8():
    mycursor.execute('''SELECT DISTINCT channel.channel_name
                    FROM channel
                    JOIN videos ON  videos.channel_id=channel.channel_id
                    WHERE YEAR(videos.PublishedAt) = 2022 ''')
    out=mycursor.fetchall()
    Q8= pd.DataFrame(out, columns=['Channel Name']).reset_index(drop=True)
    Q8.index +=1
    st.dataframe(Q8)


def q_9():
    mycursor.execute('''SELECT channel.channel_name,
                    TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(videos.Duration)))), "%H:%i:%s") AS Duration
                    FROM videos
                    JOIN channel ON videos.channel_id=channel.channel_id
                    GROUP BY channel_name ''')
    out=mycursor.fetchall()
    Q9= pd.DataFrame(out, columns=['Chanel Name','Duration']).reset_index(drop=True)
    Q9.index +=1
    st.dataframe(Q9)

def q_10():
    mycursor.execute('''SELECT channel.channel_name,videos.video_name,videos.comment_count as Total_Comments
                    FROM videos
                    JOIN channel ON channel.channel_id=videos.channel_id
                    ORDER BY videos.comment_count DESC''')
    out=mycursor.fetchall()
    Q10= pd.DataFrame(out, columns=['Channel Name','Videos Name','Comments']).reset_index(drop=True)
    Q10.index +=1
    st.dataframe(Q10)


if selected == 'SQL queries output':
    st.subheader(':green[SQL queries output]')
    Questions = ['Select your Question',
        '1.What are the names of all the videos and their corresponding channels?',
        '2.Which channels have the most number of videos, and how many videos do they have?',
        '3.What are the top 10 most viewed videos and their respective channels?',
        '4.How many comments were made on each video, and what are their corresponding video names?',
        '5.Which videos have the highest number of likes, and what are their corresponding channel names?',
        '6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
        '7.What is the total number of views for each channel, and what are their corresponding channel names?',
        '8.What are the names of all the channels that have published videos in the year 2022?',
        '9.What is the average duration of all videos in each channel, and what are their corresponding channel names?',
        '10.Which videos have the highest number of comments, and what are their corresponding channel names?' ]
    
    Selected_Question = st.selectbox(' ',options=Questions)
    if Selected_Question =='1.What are the names of all the videos and their corresponding channels?':
        q_1()
    if Selected_Question =='2.Which channels have the most number of videos, and how many videos do they have?':
        q_2()
    if Selected_Question =='3.What are the top 10 most viewed videos and their respective channels?': 
        q_3()
    if Selected_Question =='4.How many comments were made on each video, and what are their corresponding video names?':
        q_4()  
    if Selected_Question =='5.Which videos have the highest number of likes, and what are their corresponding channel names?':
        q_5() 
    if Selected_Question =='6.What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        st.write('**:red[Note]:- Dislike property was made private as of December 13, 2021.**')
        q_6()   
    if Selected_Question =='7.What is the total number of views for each channel, and what are their corresponding channel names?':
        q_7()
    if Selected_Question =='8.What are the names of all the channels that have published videos in the year 2022?':
        q_8()
    if Selected_Question =='9.What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        q_9()
    if Selected_Question =='10.Which videos have the highest number of comments, and what are their corresponding channel names?':
        q_10()

# # Setting up the option "Data Visualization" in streamlit page 
# if selected == "Data Visualization":
#     st.subheader(':blue[Data Visualization]')
#     st.markdown('''you can view statistical analyses of YouTube channels along with visualizations''')

#     Option = st.selectbox(' ',['Select to view ',
#                         '1.Channels with Subscriber Count',
#                         '2.Channels with highest No Of Videos',
#                         '3.Channels with Top 10 viewed videos',
#                         '4.Channels with Total Views',
#                         '5.channels with Average videos duration',
#                         '6.Year wise Performance of each Channel'])
    
#     if Option =='1.Channels with Subscriber Count':
#         with st.spinner('Ploting in progress...'):
#             def plot_ques_1():
#                     mycursor.execute('''SELECT channel_name,channel_subscribers 
#                                     FROM channel
#                                     ORDER BY channel_subscribers DESC''')
#                     out=mycursor.fetchall()
#                     df=pd.DataFrame(out, columns=['Channel Name','Subscribers Count'])
#                     fig = px.bar(df, x='Channel Name', y='Subscribers Count',color='Channel Name',text='Subscribers Count',
#                                 title='Channels with Subscriber Count')
#                     st.plotly_chart(fig, use_container_width=True)
#             plot_ques_1()
    
#     if Option == '2.Channels with highest No Of Videos':
#         def plot_ques_2():
#                 mycursor.execute('''SELECT channel_name,channel_video_count as Total_Videos 
#                             FROM channel 
#                             ORDER BY channel_video_count DESC''')
#                 out=mycursor.fetchall()
#                 df=pd.DataFrame(out, columns=['Channel Name','Total Videos'])
#                 fig =px.bar(df, x='Channel Name', y='Total Videos',color='Channel Name',text='Total Videos',
#                             title='Channels with highest No Of Videos')
#                 st.plotly_chart(fig,use_container_width=True)
#         plot_ques_2()
    
#     if Option =='3.Channels with Top 10 viewed videos':
#         def plot_ques_3():
#             mycursor.execute('''SELECT channel.Channel_name,videos.Video_name, videos.View_Count as Total_Views
#                             FROM videos
#                             JOIN channel ON channel.Channel_id = videos.Channel_id
#                             ORDER BY videos.View_Count DESC
#                             LIMIT 10;''')
#             out=mycursor.fetchall()
#             df=pd.DataFrame(out, columns=['Channel Name','Videos Name','Total Views'])
#             fig=px.bar(df, x='Total Views', y='Videos Name', color='Channel Name',text='Total Views',
#                         orientation='h', title='Top 10 Viewed Videos for Each Channel')
#             st.plotly_chart(fig,use_container_width=True)
#         plot_ques_3()

#     if Option =='4.Channels with Total Views':
#         def plot_ques_4():
#             mycursor.execute('''SELECT channel.channel_name,channel.channel_views as Total_views
#                     FROM channel
#                     ORDER BY channel.channel_views DESC  ''')
#             out=mycursor.fetchall()
#             df= pd.DataFrame(out, columns=['Channel Name','Total Views'])
#             fig=px.bar(df, x='Total Views', y='Channel Name', color='Channel Name',text='Total Views',
#                         title='Channels with Total Views')
#             st.plotly_chart(fig,use_container_width=True)
#         plot_ques_4()

#     if Option =='5.channels with Average videos duration':
#         def plot_ques_5():
#             mycursor.execute('''SELECT channel.channel_name,
#                             TIME_FORMAT(SEC_TO_TIME(AVG(TIME_TO_SEC(TIME(videos.Duration)))), "%H:%i:%s") AS Duration
#                             FROM videos
#                             JOIN channel ON videos.channel_id=channel.channel_id
#                             GROUP BY channel_name ORDER BY Duration ASC ''')
#             out=mycursor.fetchall()
#             df= pd.DataFrame(out, columns=['Channel Name','Average Duration'])
#             fig=px.bar(df, x='Channel Name', y='Average Duration', color='Channel Name',text='Average Duration',
#                         title='Channels with Average Duration')
#             st.plotly_chart(fig,use_container_width=True)
#         plot_ques_5()

#     if Option =='6.Year wise Performance of each Channel':
#         def plot_ques_6():
#             mycursor.execute('''SELECT DISTINCT Year(videos.publishedAt) AS Years, COUNT(videos.video_id) AS Total_videos,
#                             SUM(videos.Like_Count) AS Total_Likes,
#                             SUM(videos.view_count) as Total_views, 
#                             SUM(videos.comment_count) AS Total_comments, channel.channel_name
#                             FROM videos 
#                             LEFT JOIN channel ON videos.channel_id = channel.channel_id 
#                             GROUP BY channel_name, Years''')
#             out=mycursor.fetchall()
#             df= pd.DataFrame(out, columns=['Years','Total videos','Likes','Views','Total Comments','Channel Name'])
#             fig=px.line(df, x='Years', y='Total videos', color='Channel Name',markers=True,
#                         title='Year wise uploaded videos')
#             st.plotly_chart(fig)

#             fig1=px.line(df, x='Years', y='Likes', color='Channel Name',markers=True,
#                         title='Year wise Likes')
#             st.plotly_chart(fig1)

#             fig2=px.line(df, x='Years', y='Views', color='Channel Name',markers=True,
#                         title='Year wise Views')
#             st.plotly_chart(fig2)

#             fig3=px.line(df, x='Years', y='Total Comments', color='Channel Name',markers=True,
#                         title='Year wise Comments')
#             st.plotly_chart(fig3)
#         plot_ques_6()
