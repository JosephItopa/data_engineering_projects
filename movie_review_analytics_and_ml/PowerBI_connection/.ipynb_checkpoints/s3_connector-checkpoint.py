import boto3
import numpy as np
import pandas as pd

AWS_KEY_ID='AKIAVCI2VIMCOV4GR4FV'  
AWS_SECRET='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'

bucket_name = "movie-review-2022"

s3_object_train = "cleaned/clean-movie-review.csv"
obj_train = s3.get_object(Bucket=bucket_name, Key=s3_object_train)
df_train = pd.read_csv(obj_train['Body'])