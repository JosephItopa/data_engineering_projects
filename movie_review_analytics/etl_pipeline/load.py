import boto3
import os

AWS_KEY_ID='###'  
AWS_SECRET='###'
# instantiate an s3 object using boto3
s3 = boto3.client('s3', region_name = 'us-east-1',\
                  aws_access_key_id = AWS_KEY_ID, aws_secret_access_key = AWS_SECRET)

# create a bucket
bucket = s3.create_bucket(Bucket = 'movie-review-2022')

path = 'rawfile/'
for root, dirs, files in os.walk(path):
    for name in files:
        if name.endswith(".csv"):
            s3.upload_file(Filename = name, Bucket = 'movie-review-2022', Key = 'raw/'+name)
            
            

s3.upload_file(Filename = 'clean-movie-review.csv', Bucket = 'movie-review-2022', Key = 'cleaned/clean-movie-review.csv')