from django.shortcuts import render
from django.conf import settings
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_replace, explode, split

def index(request):
    # An empty string is created for messages
    upload_message = ""
    file_content = ""
    file_name = ""
    top_words = None

    if request.method == "POST":
        action = request.POST.get('action')

        if action == "upload" and request.FILES["uploaded_file"]:
            uploaded_file = request.FILES["uploaded_file"]
            file_name = uploaded_file.name
            
            # Prepare file to be uploaded to AWS S3
            s3_client = boto3.client('s3',
                                     aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                                     aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
            bucket_name = settings.AWS_STORAGE_BUCKET_NAME
            file_key = file_name

            # Upload file to S3
            try:
                s3_client.upload_fileobj(uploaded_file, bucket_name, file_key)
                upload_message = "The file has been uploaded successfully!"
            except Exception as e:
                upload_message = "An error occurred while uploading the file: {}".format(str(e))
        
        # When the file is loaded and displayed
        if action == "upload":
        # Get filename and content
            if not uploaded_file.name:
                upload_message = "Please select a file!"
            else:
                file_name = uploaded_file.name
                file_key = file_name

                # Read file from S3
                try:
                    s3_client = boto3.client('s3',
                                            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
                                            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
                    response = s3_client.get_object(Bucket=settings.AWS_STORAGE_BUCKET_NAME, Key=file_key)
                    file_content = response['Body'].read().decode('utf-8')
                    
                    # Initialize Spark session
                    spark = SparkSession.builder.appName("ReadAndPrintTxt").getOrCreate()

                    # Define text cleaning function
                    def clean_text(c):
                        return lower(regexp_replace(c, '[^a-zA-Z\s]', '')).alias('cleaned')

                    # Read text file and apply cleaning function
                    df = spark.createDataFrame([(file_content,)], ["value"]).select(clean_text(col("value")))
                    df = df.filter(col("cleaned") != "")

                    # Tokenize words
                    words = df.select(explode(split(col("cleaned"), "\s+")).alias("word"))

                    # Count word occurrences
                    wordCounts = words.groupBy("word").count()

                    # Sort and limit to top 10 words
                    sortedWordCounts = wordCounts.orderBy(col("count").desc()).limit(10)

                    # Collect results
                    top_words = sortedWordCounts.collect()

                    # Stop Spark session
                    spark.stop()

                except Exception as e:
                    upload_message = "An error occurred while reading the file: {}".format(str(e))

    # Import data into Django template
    return render(request, 'index.html', {
        'upload_message': upload_message,
        'file_name': file_name,
        'file_content': file_content,
        'top_words': top_words
    })
