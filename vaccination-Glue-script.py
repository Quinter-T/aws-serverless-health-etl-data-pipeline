import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import csv
import io
from datetime import date, timedelta

# Initialize Glue and Boto3 clients
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# SNS client for notifications
sns_client = boto3.client('sns')

# Define your S3 and SNS variables here
S3_BUCKET = 'cameroon-vaccination-data'
S3_KEY = 'vaccination_records.csv'
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:198075950421:PRO-Reminders' # Replace with your ARN

# Read the CSV from S3
try:
    s3_resource = boto3.resource('s3')
    obj = s3_resource.Object(S3_BUCKET, S3_KEY)
    csv_content = obj.get()['Body'].read().decode('utf-8')
except Exception as e:
    print(f"Error reading S3 file: {e}")
    sys.exit(1)

# Process the data
reminders_list = []
reader = csv.reader(io.StringIO(csv_content))
next(reader) # Skip header

today = date.today()

for row in reader:
    baby_name = row[0]
    date_of_birth_str = row[1]
    phone_number = row[2]

    try:
        date_of_birth = date.fromisoformat(date_of_birth_str)
        next_vaccination_date = date_of_birth + timedelta(days=30)

        if today <= next_vaccination_date:
            reminders_list.append({
                'BabyName': baby_name,
                'VaccinationDate': next_vaccination_date.strftime('%Y-%m-%d'),
                'PhoneNumber': phone_number
            })
    except Exception as e:
        print(f"Error processing row for {baby_name}: {e}")
        continue

# Format the email content
if reminders_list:
    email_subject = 'Daily Vaccination Reminders'
    email_body = (
        "Hello Public Relations Officer,\n\n"
        "The following parents need to be reminded of their baby's upcoming vaccination date. "
        "Please send an SMS and call each parent.\n\n"
        "----------------------------------------------------\n"
    )
    for reminder in reminders_list:
        email_body += (
            f"Baby Name: {reminder['BabyName']}\n"
            f"Vaccination Date: {reminder['VaccinationDate']}\n"
            f"Mother's Phone Number: {reminder['PhoneNumber']}\n"
            "----------------------------------------------------\n"
        )
else:
    email_subject = 'No Vaccination Reminders for Today'
    email_body = "Hello Public Relations Officer,\n\nThere are no upcoming vaccination reminders for today."

# Publish the message to SNS
try:
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=email_subject,
        Message=email_body
    )
    print("Email notification sent successfully to the SNS topic.")
except Exception as e:
    print(f"Error sending SNS notification: {e}")
    sys.exit(1)

job.commit()