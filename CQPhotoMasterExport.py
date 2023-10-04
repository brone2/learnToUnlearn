import cloudinary
import cloudinary.uploader
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import re
import csv
import requests
import sys
import firebase_admin
from firebase_admin import db
import string
import random
import itertools
import requests
# import firebase
from firebase_admin import db
# from pyrebase import pyrebase
import pandas as pd
import os
from datetime import date, timedelta, datetime
# Filter for country
# country_pd = pd.DataFrame(datasets[0])
# country_name = country_pd['country'].iloc[0]
today = date.today()
month_prior = today - timedelta(days=320)
# CLOUDERA SET UP
INSTALLED_APPS = [
    'cloudinary',
]
cloud_key = '296959435248415'
cloud_secret = 'm9BYQw9yHNdvzWK_BDWBiLcfSUY'
cloud_name = 'cornershop'
cloudinary.config(
    cloud_name=cloud_name,
    api_key=cloud_key,
    api_secret=cloud_secret
)
# END CLOUDERA SET UP
# Firebase set up
cred = credentials.Certificate({
    "type": "service_account",
    "project_id": "cqmaster1-6cb9c",
    "private_key_id": "98dc927271eb110c05487e4183931552fcd2cf71",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCshdNn47iw/IqZ\n+2hjABhK6BHYrwDaAYRDctT0XLR5S+O+mpF4WmXkAihf/pjDda1fn/xREb/XP3GM\nHBavGIfajRAd1wcWS1r48ojk8rTDMY+Mu3lPG3p2eMyqRpLXRuzlId2KDGSZejLP\n6NPfaYLn4hXEzBBTzBcIaNnIo2P5WXif9eP9WheGmpW0+Rkf63GJZCTyFpdvgbDw\ncU/FspyQ++8m9/sR/jMnej6X4vz34ynvVKzoj0SSPokGIUrj44YHD5j70IOxplVt\np8DRNzFSn6UShWE0mhqzHxOKbz/tOkaiARIME0i0/8m7c77ImkOKFNLsbXBlnaRx\nypNdgs4ZAgMBAAECggEAObKwF0ijlh1/xSu9p456kWK3xT4SDNcBAwW+jtMKUJ6+\n1hofmJ6zAORv2FrvgD808oww+HP1Mum28eLa+0g69y7COxiU+DHPhP6oJdm99qH2\nnlx8n6ZS+JhB6pibt3+y7pa63tZyNpoVsE7vvQIuk5qbKSwOMdhJw7g9TcrHPrbO\na+o4sniPETpKuLCK232z4f+dfhvwCSaE6KOBLQvWpXKuV/thKkon5+0C7Kg24sFN\nhRkXj2KnQUcdGhQR4KYAYjsbXR6XZVLsbMI8zgCnZelYG2TIzPml5SLtalGO4BpO\n4bzg/onSK7rJtLejgycwPJ47g2yyw/cyNu+NY3A7uQKBgQDl357GBahXxKYQeSkr\nHTzms/2OyVO2/Jy7jWZhvgprQJ1hXPQDRkz393LcdRLsBpZ7Kgn+olAzPkc/F7l+\neDM+w+D9ZvacZaf8WVGs6FYqKm7UrqZtG3p7GmR4mQMYE6DxYeenzis30V011woL\nhf6G3O79nerLscW+hLbazb5h4wKBgQDAIYhsJQtc1NZdERKIpuUGUSS3xvXelzZd\n4pUU8N6N8aE/npfGfKyOlMTYPzpzuEatD4udAYTz5Tx54ACMefx3SZauLFcJMWIM\n7iKfCa63zBMHso9FXqMVCl1erv/Mil8/wkNKYsg34cdIVD5vagIVWjcMQL+6HZ3w\nvXj1YQFg0wKBgBQ7QjYbeV2AKT157G2m/R0w6jgO8BdC1GiNYV1o5HHcFf3juHYx\nUcThOnK8uxWa3tOoN4j5sCVSbeLXq7O90ITNqJek4D+Tkt1a3x1gtXSwl2CQnoUr\nzfPXttAGZ4dO8vNLsp/KPXOEMbfzXSb2fBhSiZY4t9mFo+nItG89fGwRAoGBAJUm\ncv1hFJ0QLQk6g6TJyulIfLSeI6iUwoPHrev/wQV4GrGEsFZJ2rslClrFWt8Souse\nMkfEMi4UOwpxtntXB4KjAfJcQYTFbVSUWRQVNClCp1NGbqpveTKQRUOHntRuZtrc\nN9i7LqsS/t1LaNyc8tkYm7tLWItnCoEk1Y3HrIY5AoGBAOPNvYlyAH1lKru0MNfI\nRFbdFvEYWR+Z9fNHHBFY69ySyLHcDfQDSqFpnhrk2hPb+RFBvAyZpchVH7kNyKg6\nCIE8SWmp5b7oUTGDuUEhqEe8jMsJ27kUKe08Ccnci5/5wGEEoBNQs2J+BMVOveVX\nvLcL4FqeQkv2MJkp57XoA4FA\n-----END PRIVATE KEY-----\n",
    "client_email": "firebase-adminsdk-ozbqn@cqmaster1-6cb9c.iam.gserviceaccount.com",
    "client_id": "108777509811406972776",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-ozbqn%40cqmaster1-6cb9c.iam.gserviceaccount.com"
})
firebaseConfig = {'apiKey': "AIzaSyAWeNj4xEwTOFFDylj0iAA6kACAPWAzvq4",
                  'authDomain': "cqmaster1-6cb9c.firebaseapp.com",
                  'databaseURL': "https://cqmaster1-6cb9c-default-rtdb.firebaseio.com",
                  'projectId': "cqmaster1-6cb9c",
                  'storageBucket': "cqmaster1-6cb9c.appspot.com",
                  'messagingSenderId': "819225670030",
                  'appId': "1:819225670030:web:d268d505f3f675cbaeb4df",
                  'measurementId': "G-ZK9P1HX7N2"}
if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {'databaseURL': 'https://cqmaster1-6cb9c-default-rtdb.firebaseio.com/'})
# END Firebase set up
ref = db.reference()
photo_dict = ref.child('photos').get()
df_prep = [v for v in photo_dict.values()]
df_for_csv = pd.DataFrame(df_prep)
# *******BEGIN EDITING LOOP
# Check if editedPhotoUrl  is already there, if not then perform the edit and save
for index, row in df_for_csv.iterrows():
    if len(str(row['editedPhotoUrl'])) < 10:  # default for editedPhoto is na so doing filter
        photoKey = row['photoKey']
        notEditedPhotoUrl = row['photoUrl']
        # *******Begin Photo Editing
        response = requests.post(
            'https://api.remove.bg/v1.0/removebg',
            data={
                'image_url': notEditedPhotoUrl,
                # 'size': 'preview',
                'size': 'auto',
                'add_shadow': 'false',
                'bg_color': 'white',
                'position': 'center',
                'scale': '90%'
            },
            headers={'X-Api-Key': 'mjikF2eJYKJHgbogHwBgggp5'},
        )
        if response.status_code == requests.codes.ok:
            # print(type((response.content)))
            photo_content = response.content
            photo_upload = cloudinary.uploader.upload(photo_content)
            # edited_photo_url = photo_upload['secure_url']
            # Test on dummy
            edited_photo_url = 'https://storage.googleapis.com/cqmaster1-6cb9c.appspot.com/photos/-NdkdIrvhjR_bHtNj4Sr/1694123342739_editedPhoto.jpg'
            print(photo_upload['secure_url'])
            ref.child('photos').child(photoKey).update({'editedPhotoUrl': edited_photo_url})
            # print(edited_photo_url)
        else:
            print("Error:", response.status_code, response.text)
            ref.child('photos').child(photoKey).update({'editedPhotoUrl': 'unable_to_process'})
            # *******End Photo Editing
            # And begin the more fun things!
# Export the data
photo_dict_for_export = ref.child('photos').get()
df_export_prep = [v for v in photo_dict_for_export.values()]
df_export_prep = pd.DataFrame(df_export_prep)
# Filter for country! Pushing it back!
# if country_name != "ALL":
#   df_export_prep = df_export_prep[df_export_prep['country'] == country_name]
# print(datetime.utcfromtimestamp(df_export_prep['timestamp']).strftime('%Y-%m-%d %H:%M:%S'))
df_export_prep["formattedDate"] = pd.to_datetime(df_export_prep['timestamp'], unit='ms')
df_export_prep["photoKeyFormat"] = "a" + df_export_prep['photoKey']
# df_export_prep["formattedDate"] = datetime.utcfromtimestamp(df_export_prep['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
# Filter past 28 days
df_export_prep = df_export_prep[df_export_prep['formattedDate'].dt.date >= month_prior]
reduced_df = df_export_prep[
    ['scannedBarcode', 'editedPhotoUrl', 'photoUrl', 'formattedDate', 'myName', 'store', 'isDeliItem', 'photoNote',
     'timestamp', 'country', 'adjustedPluCode'].copy()]
sorted_reduced_df = reduced_df.sort_values(by=['timestamp'], ascending=False)
reduced_df = reduced_df.sort_values(by=['timestamp'], ascending=False)
csv_export_path = r"C:\Users\bronf\Documents\1. first_cornershop_thing\python_output.csv"
# sorted_reduced_df = sorted_reduced_df.head(25000)
sorted_reduced_df.to_csv(csv_export_path, index=False)

# print(sorted_reduced_df)
# mode.export_csv(reduced_df)







