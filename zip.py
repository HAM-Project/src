#!/usr/bin/python3
import time
import boto3
import uuid
import sys
import os
from botocore.config import Config

region = 'us-east-1'
queue =  "https://sqs.us-east-1.amazonaws.com/943393141316/samFileToServer"
S3_in = "samfileinput"
S3_out = "compressedbamfile"

if len(sys.argv) >= 2 : #Check if there is a 2nd argument, if there is use the config file.
    fileName = sys.argv[1]
  
    try: #try reading config file
        
        file = open(fileName,"r")
        
        content = file.read()  #need to trim readline, or \n character will be part of string
        file.close()
        text = content.split()
        if len(text) >= 4 :
            region = text[0]
            queue = text[1]
            S3_in = text[2]
            S3_out = text[3]                    
    except:
        region = 'us-east-1'
        queue =  "https://sqs.us-east-1.amazonaws.com/943393141316/samFileToServer"
        S3_in = "samfileinput"
        S3_out = "compressedbamfile"
 
    

my_config = Config(region_name = region)
#Simple program that monitors
sqs_client = boto3.client("sqs", config = my_config)  #need to include config, because when run at startup, config file may not available.
s3_client = boto3.client("s3", config = my_config)

while True:
    response = sqs_client.receive_message(QueueUrl=queue,
                                           MaxNumberOfMessages=1,MessageAttributeNames=['All'],)
    if "Messages" in response :
        message = response["Messages"][0]  #Messages is an array, but we only requested 1 message, so we use [0] to get the messages
        body = message["Body"]
        rhandle = message["ReceiptHandle"]
        sqs_client.delete_message(QueueUrl=queue,ReceiptHandle=rhandle)

        #file loop
        savename = "/home/ec2-user/"+body
        s3_client.download_file(S3_in, body, savename)

        cmd = "samtools view -S -b " + body + " > file.bam"
        shellCommand = os.system(cmd)

        s3_client.upload_file("file.bam",S3_out, str(uuid.uuid1())+"_"+body)
        s3_client.delete_object(Bucket=S3_in, Key=body)

        print(body)
    else:
        time.sleep(1)