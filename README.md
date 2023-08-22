# youtube data visualisation
This project aims at developing a streamlit app which can take youtube channel IDs as input and can facilitate queries like video with most views, average duration of videos of each channel etc.

It's quite interesting to observe that a single .py script fetches relevant data, dumps it at MongoDB database, transfers the data to sql, runs queries and represents result over web-dashboard.

Following are the requirements to run the script:
1) An IDE that supports python.
2) Streamlit package
3) mysql-connector-python and pymongo
4) mysql connection to local/remote host
5) MongoDB server = Atlas/local 

To execute the file we can follow following steps:
1) Copy the script in any python IDE.
2) Set up mysql connection by editing lines from 9-12
3) Provide MongoDB server path at line 177
4) Run the code
5) IDE would provide 'streamlit run <path>' which can be run on cmd or terminal
6) A local host link will be generated which can be used on browser to access the dashboard

Note1: If the youtube channel is popular then it will have a large amount of data, and fetchhing and analysing the same can consume some time accordingly.
Note2: Google api_key expires after a fixed number of requests. If it shows Error stating that request quota has exceeded, change the api_key at line 19.
