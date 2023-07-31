#################################################
try:
    # import serverless_wsgi
    # from flask import Flask, jsonify
    import json
    import sys
    # import requests
    from collections import namedtuple
    from random import choice
    from csv import writer
    import math
    import re
    import nltk
    from collections import Counter
    # from mysql.connector import Error
    # import pymysql
    import mysql.connector
    # from datetime import date
    from datetime import datetime
    import json
    import pandas as pd
    # import flask
    # import string
    import time
    # import codecs
    # import requests
    # import csv
    # import io
    import boto3
    from datetime import datetime
    # import numpy as np
    from flask import Flask, request, jsonify, render_template
    # from flask_cors import CORS, cross_origin
    from flaskext.mysql import MySQL
    from io import StringIO
    import os
    # import boto.utils
    from smart_open import smart_open
    # import unittest
    import warnings
    warnings.filterwarnings('ignore')
    warnings.simplefilter(action='ignore', category=FutureWarning)
    print("All imports ok ...")
except Exception as e:
    print("Error Imports : {} ".format(e))

sys.path.append(os.path.abspath('/var/task/nltk_data/'))
sys.path.append(os.path.abspath('/var/task/nltk/'))
print("appended")
# nltk.path.append('/var/task/nltk_data')
# nltk.download('stopwords')
nltk.download('stemmers')
print("downloaded the stemmers")
nltk.download('corpora/stopwords')
print("downloaded the stopwords")
nltk.download('corpora/wordnet')
print("downloaded the wordnet")

# nltk.download('corpora/stopwords', download_dir='/var/task/nltk_data')
# print("appended")
# nltk.download('corpora/wordnet', download_dir='/var/task/nltk_data')
# print("appended")
# nltk.download('stemmers', download_dir='/var/task/nltk_data')
# print("appended")
try:
    from nltk.corpus import wordnet as wn
    print("3rd import succeeded")
    # from nltk.stem.porter import PorterStemmer
    from nltk.stem.porter import *
    print("4th import succeeded")
    from nltk.corpus import stopwords
    print("5th import succeeded")
except Exception as e:
    print("Error Imports : {} ".format(e))

# stop = stopwords.words('english')
# WORD = re.compile(r'\w+')
# stemmer = PorterStemmer()
###################################################################################################
# nltk.download('wordnet')
# nltk.download('omw-1.4')
# nltk.download('stopwords')

app = Flask(__name__)
mysql = MySQL()


# ###############################################################
Quote = namedtuple("Quote", ("text", "author"))

quotes = [
    Quote("Talk is cheap. Show me the code.", "Linus Torvalds"),
    Quote("Programs must be written for people to read, and only incidentally for machines to execute.", "Harold Abelson"),
    Quote("Always code as if the guy who ends up maintaining your code will be a violent psychopath who knows where you live",
          "John Woods"),
    Quote("Give a man a program, frustrate him for a day. Teach a man to program, frustrate him for a lifetime.", "Muhammad Waseem"),
    Quote("Progress is possible only if we train ourselves to think about programs without thinking of them as pieces of executable code. ",
          "Edsger W. Dijkstra")
]

########################################## Datashaper Home ################################################################
# @app.route('/')
# def home():
#     return 'Hello'

# @cross_origin(origins='*')
@app.route('/datashaper', methods=['GET'])
def datashaper_home():
    return "This application belongs to Datashaper"

@app.route("/", methods=["GET"])
def get_random_quote2():
    return "Success"

@app.route("/quote", methods=["GET"])
def get_random_quote():
    return jsonify(choice(quotes)._asdict())

@app.route("/quotes", methods=["GET"])
def get_random_quote1():
    return "None"

################################### File Reading ############################################################################
def readCsvFileFromBucket(bucketName, fileName1):
    csvContent = ""
    try:
        bucketFilePath = 's3://{}:{}@{}/{}'.format(
            AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY_ID, bucketName, fileName1)
        csvContent = pd.read_csv(smart_open(
            bucketFilePath, encoding='utf8'), error_bad_lines=False, index_col=False, dtype='unicode')
    #     csvContent = csvContent.dropna()
    except Exception as e:
        print("Fail while reading file from s3")
        print(e)
    return csvContent

#############################################################################################################

def get_cosine(vec1, vec2):
    intersection = set(vec1.keys()) & set(vec2.keys())
    numerator = sum([vec1[x] * vec2[x] for x in intersection])
    sum1 = sum([vec1[x]**2 for x in vec1.keys()])
    sum2 = sum([vec2[x]**2 for x in vec2.keys()])
    denominator = math.sqrt(sum1) * math.sqrt(sum2)
    if not denominator:
        return 0.0
    else:
        result = float(numerator) / denominator
        return result


def text_to_vector(text):
    stop = stopwords.words('english')
    WORD = re.compile(r'\w+')
    stemmer = PorterStemmer()

    words = WORD.findall(text)
    a = []
    for i in words:
        for ss in wn.synsets(i):
            a.extend(ss.lemma_names())
    for i in words:
        if i not in a:
            a.append(i)
    a = set(a)
    w = [stemmer.stem(i) for i in a if i not in stop]
    return Counter(w)


def get_similarity(a, b):
    a = text_to_vector(a.strip().lower())
    b = text_to_vector(b.strip().lower())
    return get_cosine(a, b)


def get_char_wise_similarity(a, b):
    a = text_to_vector(a.strip().lower())
    b = text_to_vector(b.strip().lower())
    s = []
    for i in a:
        for j in b:
            s.append(get_similarity(str(i), str(j)))
    try:
        return sum(s)/float(len(s))
    except:  # len(s) == 0
        return 0

############################################## Fetching Column Names #########################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/fetchColumnNames', methods=['POST'])
def column_names():
    requestBody = request.get_json()
    try:
        print(requestBody, "requestBody")
        sourcefile = requestBody['sourceFile']
        bucketName = requestBody["bucketName"]
        responseData = {}
        inputfile = readCsvFileFromBucket(bucketName, sourcefile)
        responseData["status"] = 200
        responseData["Response"] = list(inputfile.columns)
    except:
        responseData["status"] = 400
        responseData["Response"] = "Cannot Fetch the Details from the Source File at the moment"
    return responseData


# @cross_origin(origins='*')
@app.route('/datashaper/fetchTitleFileColumnNames', methods=['POST'])
def fetch_title_bucket_column_names():
    requestBody = request.get_json()
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        print(requestBody, "requestBody")
        titlefile = requestBody['titlefile']
        bucketName = requestBody["bucketName"]
        if titlefile or request.method == 'POST':
            sql = "SELECT titlefile FROM datashaper_titlebucket WHERE titlebucket=%s LIMIT 1"
            val = (titlefile)
            cursor.execute(sql, val)
            result = cursor.fetchall()
            for row in result:
                existing_title_bucket = row[0]
        titlefile = existing_title_bucket
        responseData = {}
        inputfile = readCsvFileFromBucket(bucketName, titlefile)
        responseData["status"] = 200
        responseData["Response"] = list(inputfile.columns)
    except:
        responseData["status"] = 400
        responseData["Response"] = "Cannot Fetch the Details from the Source File at the moment"
    finally:
        cursor.close()
        conn.close()
    return responseData


# @cross_origin(origins='*')
@app.route('/datashaper/fetchDelTemColumnNames', methods=['POST'])
def fetch_Del_Tem_Column_Names():
    requestBody = request.get_json()
    try:
        delivery_template_input_file = requestBody['delivery_template_input_file']
        bucketName = requestBody["bucketName"]
        responseData = {}
        inputfile = readCsvFileFromBucket(bucketName, delivery_template_input_file)
        responseData["status"] = 200
        responseData["Response"] = list(inputfile.columns)
    except:
        responseData["status"] = 400
        responseData["Response"] = "Cannot Fetch the Details from the Source File at the moment"
    return responseData


# @cross_origin(origins='*')
@app.route('/datashaper/fetchNlpOutputFileColumnNames', methods=['POST'])
def fetch_nlp_output_file_Column_Names():
    requestBody = request.get_json()
    try:
        nlp_output_file = requestBody['nlp_output_file']
        bucketName = requestBody["bucketName"]
        responseData = {}
        inputfile = readCsvFileFromBucket(bucketName, nlp_output_file)
        responseData["status"] = 200
        responseData["Response"] = list(inputfile.columns)
    except:
        responseData["status"] = 400
        responseData["Response"] = "Cannot Fetch the Details from the Source File at the moment"
    return responseData

#####################################################################################################################################

# @cross_origin(origins='*')
@app.route("/datashaper/fetchAllrecords", methods=["GET"])
def fetch_records():
    try:
        fetch_records = []
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT nlp_id, input_file, nlp_output_file, delivery_template_input_file, delivery_template_recipe_name, leadshaper_output_file, created_at, status FROM leadshaper_delivery_template ORDER BY created_at DESC")
        myresult = cursor.fetchall()
        for row in myresult:
            data = {}
            data['nlp_id'] = row[0]
            data['input_file'] = row[1]
            data['nlp_output_file'] = row[2]
            data['delivery_template_input_file'] = row[3]
            data['delivery_template_recipe_name'] = row[4]
            data['leadshaper_output_file'] = row[5]
            data['created_at'] = row[6]
            data['status'] = row[7]
            fetch_records.append(data)
        return jsonify(fetch_records)
    except Exception as e:
        print(e)
#     except Exception as err:
#         logger.error("Exception occurred: %s", err)
#         result = ("Exception in Records: %s", err)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/insertRecord', methods=['POST'])
def insertFunction():
    conn = mysql.connect()
    cursor = conn.cursor()
    try:
        _json = request.json
        input_file = _json['csv_filename1']
        # match_file = _json['csv_filename2']
        delivery_template_input_file = _json['delivery_template_input_file']
        # nlp_output_file = _json['nlp_output_file']
        # recipe_name = _json['recipe_name']
        status = "In progress"
        created_at = datetime.now()
        error_log = "None"
        match_file = ""
        nlp_output_file = ""
        recipe_name = ""
        nlp_output_status = "In progress"
        shaping_status = "Not Started"
        leadshaper_output_status = "Not Started"
        if input_file or match_file or created_at or status or request.method == 'POST':
            sqlQuery = "INSERT INTO leadshaper_delivery_template(input_file, match_file, nlp_output_file, recipe_name, created_at, error_log, status, delivery_template_input_file, nlp_output_status, shaping_status, leadshaper_output_status) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            bindData = (input_file, match_file, nlp_output_file,
                        recipe_name, created_at, error_log, status, delivery_template_input_file, nlp_output_status, shaping_status, leadshaper_output_status)
            cursor.execute(sqlQuery, bindData)
            conn.commit()
            insertRecord = jsonify('record added successfully!')
            id = get_nlp_id()
            id.status_code = 200
            return id
        else:
            return "error"
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


def get_nlp_id():
    get_nlp_id = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT nlp_id FROM leadshaper_delivery_template ORDER BY nlp_id DESC LIMIT 1")
        get_nlp_ids = cursor.fetchall()
        print(get_nlp_ids)
        get_nlp_id['nlp_id'] = get_nlp_ids[0][0]
        titleMatchFetch = jsonify(get_nlp_id)
        titleMatchFetch.status_code = 200
        return titleMatchFetch
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

##################################### NLP Buckets ##################################################################################

# @cross_origin(region='*')
@app.route('/datashaper/addtitlebucket', methods=['POST'])
def insertTitleBucket():
    conn = mysql.connect()
    cursor = conn.cursor()
    response = {}
    try:
        _json = request.json
        titlefile = _json['titlefile']
        titlebucket = _json['titlebucket']
        created_at = datetime.now()
        # Renaming the titlefile
        bucket = default_bucket_name
        client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)

        # # new_name = re.sub(r'[0123456789]','', fileName1)
        # inp = titlefile
        # titlefile1 = inp.split(".")
        # your_string = re.sub(r'[\\!~`@#$%^&*_=}+[{/);/,*?:"<>|(]',"", titlefile1[0])

        # inp = your_string
        # titlefile1 = inp.split(".")
        # titlefile2 = titlefile1[0] + str(datetime.now().strftime('-%m-%d-%Y-%H-%M-%S')) + '.csv'
        # copy_source = {'Bucket': bucket, 'Key': titlefile}
        # client.copy_object(CopySource=copy_source, Bucket=bucket, Key=titlefile2)
        # client.delete_object(Bucket=bucket, Key=titlefile)
        # Up-to here
        if titlefile or titlebucket or created_at or request.method == 'POST':
            sqlQuery = "INSERT INTO datashaper_titlebucket(titlefile, titlebucket, created_at) VALUES(%s, %s, %s)"
            bindData = (titlefile, titlebucket, created_at)
            print(bindData)
            cursor.execute(sqlQuery, bindData)
            conn.commit()
            response['status'] = 200
            response['Response'] = "Successfully Created The Title Bucket"
            return response
        else:
            response['status'] = 405
            response['Response'] = "Cannot Create Title Bucket"
            return response
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(region='*')
@app.route('/datashaper/appenddataToTitleBucket', methods=['POST'])
def appendData():
    conn = mysql.connect()
    cursor = conn.cursor()
    outputData1 = {}
    try:
        _json = request.json
        titlefile = _json['titlebucket']
        bucket_name = _json['bucket_name']
        appending_file = _json['appending_file']
        print(_json, "JSON")
        if titlefile or request.method == 'POST':
            sql = "SELECT titlebucket FROM datashaper_titlebucket WHERE titlefile=%s LIMIT 1"
            val = (titlefile)
            cursor.execute(sql, val)
            result = cursor.fetchall()
            for row in result:
                existing_title_bucket = row[0]
        existing_title_file = titlefile
        client = boto3.client('s3', region_name="us-east-1",
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
        response = client.get_object(
            Bucket=bucket_name, Key=existing_title_file)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df3 = pd.read_csv(response.get("Body"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
        df = pd.DataFrame(df3)
        bytes_to_write = df.to_csv(None, header=True, index=False).encode()
        file_name = appending_file
        current_data = client.get_object(
            Bucket=bucket_name, Key=file_name)['Body'].read()
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")
        appended_data = bytes_to_write + current_data
        # overwrite
        client.put_object(Body=appended_data,
                          Bucket=bucket_name, Key=existing_title_file)
        # outputData1['titlefile'] = existing_title_file
        # outputData1['titlebucket'] = existing_title_bucket
        outputData1['response'] = "Successfully appededed the data to titlebucket"
        outputData1['status'] = 200
        return jsonify(outputData1)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/fetchtitlebucketsbyid', methods=['GET', 'POST'])
def fetchtitlebucketsById():
    conn = mysql.connect()
    cursor = conn.cursor()
    fetch = {}
    try:
        _json = request.json
        titlebucket_id = _json['id']
        print(_json)
        if titlebucket_id or request.method == 'POST' or 'GET':
            cursor.execute(
                "SELECT titlebucket, titlefile, created_at FROM datashaper_titlebucket WHERE id = %s", titlebucket_id)
            result = cursor.fetchall()
            print(result)
            for row in result:
                print(row)
                fetch['titlebucket'] = row[0]
                fetch['titlefile'] = row[1]
                fetch['create at'] = row[2]
                # fetch_title_buckets.append(data)
                # response['status'] = 200
                # response['Response'] = "Successfully fetched the Title Bucket"
                # response['titlebucket'] = fetch
            return jsonify(fetch)
        else:
            return jsonify("cannot fetch the title bucket")
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/deletetitlebucket', methods=['DELETE'])
def deletetitlebucket():
    response = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        titlebucket = _json['titlebucket']
        print(_json, "JSON")
        if titlebucket or request.method == 'DELETE':
            sqlQuery = "DELETE FROM datashaper_titlebucket WHERE titlebucket=%s"
            values = (titlebucket)
            cursor.execute(sqlQuery, values)
            conn.commit()
            response['status'] = 200
            response['Response'] = "Successfully Deleted the Title Bucket"
            return response
        else:
            response['status'] = 405
            response['Response'] = "Cannot Delete the Title Bucket"
    except Exception as e:
        print(e)

    finally:
        cursor.close()
        conn.close()


# @cross_origin(region='*')
@app.route('/datashaper/updatetitlebucket', methods=['PUT', 'POST'])
def updateTitleBucket():
    conn = mysql.connect()
    cursor = conn.cursor()
    response = {}
    try:
        _json = request.json
        titlefile = _json['titlefile']
        titlebucket = _json['titlebucket']
        updated_at = datetime.now()
        bucket = default_bucket_name
        client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
        # inp = titlefile
        # titlefile1 = inp.split(".")
        # new_name = re.sub(r'[0123456789]','', titlefile1[0])
        # your_string = re.sub(r'[\\!~`@#$%-^&*_=}+[{/);/,*?:"<>|(]',"", new_name)

        # inp = your_string
        # titlefile1 = inp.split(".")
        # titlefile2 = titlefile1[0] + str(datetime.now().strftime('-%m-%d-%Y-%H-%M-%S')) + '.csv'
        # copy_source = {'Bucket': bucket, 'Key': titlefile}
        # client.copy_object(CopySource=copy_source, Bucket=bucket, Key=titlefile2)
        # client.delete_object(Bucket=bucket, Key=titlefile)

        if titlefile or updated_at or titlebucket or request.method == 'POST' or 'PUT':
            sqlQuery = "UPDATE datashaper_titlebucket SET titlefile=%s, updated_at=%s WHERE titlebucket=%s"
            values = (titlefile, updated_at, titlebucket)
            print(values)
            # values = (titlefile, titlebucket, updated_at)
            cursor.execute(sqlQuery, values)
            conn.commit()
            response['status'] = 200
            response['Response'] = "Successfully Updated the Title Bucket"
            return response
        else:
            response['status'] = 405
            response['Response'] = "Cannot Update the Title Bucket"
            return response
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

########################################## fetchtitlebuckets ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/fetchtitlebuckets', methods=['GET'])
def fetchtitlebuckets():
    fetch_title_buckets = []
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        sqlQuery = "SELECT id, titlebucket, titlefile, created_at FROM datashaper_titlebucket"
        cursor.execute(sqlQuery)
        result = cursor.fetchall()
        for row in result:
            data = {}
            data['id'] = row[0]
            data['titlebucket'] = row[1]
            data['titlefile'] = row[2]
            data['create at'] = row[3]
            fetch_title_buckets.append(data)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()
    return jsonify(fetch_title_buckets)


# @cross_origin(origins='*')
@app.route('/datashaper/fetchtitlefile', methods=["POST"])
def fetchtitlefile():
    response = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        titlebucket = _json['titlebucket']
        print(_json, "Json")
        sqlQuery = "SELECT titlefile FROM datashaper_titlebucket WHERE titlebucket=%s"
        bindData = (titlebucket)
        cursor.execute(sqlQuery, bindData)
        myresult = cursor.fetchall()
        for row in myresult:
            response['titlefile'] = row[0]
        return jsonify(response)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

#######################################################################################################################################

"""
# @cross_origin(origins='*')
@app.route('/datashaper/jobstatus', methods=['GET'])
def jobstatus():
    response = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        requestBody = request.get_json()
        DatabrewJobName = requestBody['DatabrewJobName']

        client = boto3.client('databrew')
        databrew_client = boto3.client('databrew',
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                       )

        sqlQuery = "SELECT job_name, job_runid FROM datashaper_job"
        cursor.execute(sqlQuery)
        result = cursor.fetchall()
        for row in result:
            job_name = row[0]
            jobRunId = row[1]

            response = databrew_client.describe_job_run(
                Name=DatabrewJobName,
                RunId=jobRunId
            )

            job_status = response["State"]
            print(job_status)
            sql = "UPDATE datashaper_job SET job_status = %s WHERE job_name=%s"
            val = (job_status, job_name)
            cursor.execute(sql, val)
            conn.commit()
            response['status'] = 200
            response['Response'] = "Successfully Updated the job status"
            return response

    except Exception as e:
        print(e)

    finally:
        cursor.close()
        conn.close()
"""

########################################## Employees Column Match ################################################################

def employeesColumnprocess(inputcsvfile, employeesColumn):
    employees_map = ["Just me", "2 to 9", "10 to 49", "50 to 99", "100 to 249", "250 to 499", "500 to 999",
                     "1,000 to 4,999", "5,000 to 9,999", "10,000 to 49,999", "50,000 or more"]
    try:
        # Processing Employee column from input dataset
        emp_df = inputcsvfile[employeesColumn]
        emp_df1 = emp_df.replace('\-*', '', regex=True)
        emp_df1 = list(emp_df1)
        emp_df1 = list(map(int, emp_df1))
        Employee_count = []
        for range in emp_df1:
            if range == 1:
                Employee_count.append(employees_map[0])
            elif range <= 9:
                Employee_count.append(employees_map[1])
            elif range <= 49:
                Employee_count.append(employees_map[2])
            elif range <= 99:
                Employee_count.append(employees_map[3])
            elif range <= 249:
                Employee_count.append(employees_map[4])
            elif range <= 499:
                Employee_count.append(employees_map[5])
            elif range <= 999:
                Employee_count.append(employees_map[6])
            elif range <= 4999:
                Employee_count.append(employees_map[7])
            elif range <= 9999:
                Employee_count.append(employees_map[8])
            elif range <= 49999:
                Employee_count.append(employees_map[9])
            else:
                Employee_count.append(employees_map[10])
        inputcsvfile["Company Employees"] = Employee_count
    except Exception as e:
        print("Fail while reading file from s3")
        print(e)
    return "success"

########################################## Revenue Column Match ################################################################

def value_to_float(x):
    if type(x) == float or type(x) == int:
        return x
    if 'K' in x:
        if len(x) > 1:
            return float(x.replace('K', '')) * 1000
        return 1000.0
    if 'M' in x:
        if len(x) > 1:
            return float(x.replace('M', '')) * 1000000
        return 1000000.0
    if 'B' in x:
        return float(x.replace('B', '')) * 1000000000
    return 0.0


def revenueColumnprocess(inputcsvfile, revenueColumn):
    annual_revenue = ["<$25M", "$25M to $49M", "$50M to $99M",
                      "$100M to $250M", "$250M to $1B", "$1B to $5B", "$5B to $10B", ">$10B"]
    try:
        # Processing Revenue column from input dataset
        Employee_revenue = []
        revenue_df = inputcsvfile[revenueColumn]
        revenue_df1 = revenue_df.replace('\$', '', regex=True)
        revenue_df2 = revenue_df1.apply(value_to_float)
        for range in revenue_df2:
            if range < 25000000:
                Employee_revenue.append(annual_revenue[0])
            elif range <= 49000000:
                Employee_revenue.append(annual_revenue[1])
            elif range <= 99000000:
                Employee_revenue.append(annual_revenue[2])
            elif range <= 250000000:
                Employee_revenue.append(annual_revenue[3])
            elif range <= 1000000000:
                Employee_revenue.append(annual_revenue[4])
            elif range <= 5000000000:
                Employee_revenue.append(annual_revenue[5])
            elif range <= 10000000000:
                Employee_revenue.append(annual_revenue[6])
            else:
                Employee_revenue.append(annual_revenue[7])
        inputcsvfile["Company Revenue"] = Employee_revenue
    except Exception as e:
        print("Fail while reading file from s3")
        print(e)
    return "success"

############################################# Datashaper Job Processing #############################################################

"""
{
   "bucket_name": "leadshaperdev",
   "csv_filename1": "titlematchoutputfile-sas1-11-08-2022-02-53-11.csv",
   "delivery_template_input_file": "sas-delivery-template.csv",
   "matchId": "13",
   "recipe_name": "sas-delTemp-v2.0",
   "inputfile_employeesColumn": "Employees",
   "inputfile_revenueColumn": "revenue",

    # Step 1
    "threshold1": "5",
    "titlefile1": "title",    # csv_filename2
    "inputfile_confidenceColumn1": "Title", # confidenceColumn
    "titlefile1_matchColumn": "LinkedIn Title",
    "del_template_outputColumn1": "Title Match NLP" # outputColumn

    # Step 2
    "threshold2": "5",
    "titlefile2": "industry",
    "inputfile_confidenceColumn2": "Industry",
    "titlefile2_matchColumn": "LinkedIn Title",
    "del_template_outputColumn2": "Industry NLP"

    # Step 3
    "threshold3": "5",
    "titlefile3": "Job_Function_job_role",
    "inputfile_confidenceColumn3": "Title",
    "titlefile3_matchColumn": "LinkedIn Title",
    "del_template_outputColumn3": "Job Function"

    # Step 4
    "threshold4": "5",
    "titlefile4": "employee_level_job_level",
    "inputfile_confidenceColumn4": "Title",
    "titlefile4_matchColumn": "LinkedIn Title",
    "del_template_outputColumn4": "Employee Level"
}
"""

databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                )

def leadshaper_create_dataset(Key, Name, bucket):
    try:
        create_dataset_response = databrew_client.create_dataset(
            Name=Name,
            Format='CSV',
            FormatOptions={
                'Csv': {
                    'Delimiter': ',',
                    'HeaderRow': True
                }
            },
            Input={
                'S3InputDefinition': {
                    'Bucket': bucket,
                    'Key': Key
                },
            }
        )

        DatasetName = create_dataset_response['Name']
    except Exception as e:
        print("dataset name is missing", e)
    return DatasetName


def leadshaper_create_recipe_job(DatasetName, Name, bucket, recipe_name):
    try:
        recipe_job_response = databrew_client.create_recipe_job(
                DatasetName = DatasetName,
                Name = Name,
                LogSubscription = 'DISABLE',
                MaxCapacity = 2,
                MaxRetries = 0,
                Outputs = [
                    {
                        'Format': 'CSV',
                        'Location': {
                        'Bucket': bucket
                        },
                        'Overwrite': True,
                        'FormatOptions': {
                            'Csv': {
                                'Delimiter': ','
                            }
                        },
                    },
                ],
                RecipeReference = {
                    'Name': recipe_name,
                    'RecipeVersion': '1.0'
                },
                RoleArn = role,
                Timeout = 300
            )
        databrewJobName = recipe_job_response["Name"]
        return databrewJobName
    except Exception as e:
        print("recipe_name is missing", e)


def leadshaper_start_job_run(databrewJobName):
    try:
        start_job_response = databrew_client.start_job_run(
                                Name=databrewJobName
                            )

        jobRunId = start_job_response["RunId"]
        return jobRunId
    except Exception as e:
        print("Job Run_id is missing: ", e)


def leadshaper_describe_job_run(databrewJobName, jobRunId):
    try:
        shaperJob = {}
        job_run_response = databrew_client.describe_job_run(
                        Name=databrewJobName,
                        RunId=jobRunId
                    )
        state = job_run_response["State"]
        print(state)
        if state == "SUCCEEDED":
            shaperJob['State'] = state
            shaperJob['recipe_name'] = job_run_response['RecipeReference']['Name']
            shaperJob['ErrorMessage'] = ""
        elif state == "RUNNING":
            shaperJob['State'] = state
            shaperJob['recipe_name'] = job_run_response['RecipeReference']['Name']
            shaperJob['ErrorMessage'] = ""
        elif state == "FAILED":
            shaperJob['State'] = state
            shaperJob['recipe_name'] = job_run_response['RecipeReference']['Name']
            shaperJob['ErrorMessage'] = job_run_response['ErrorMessage']
        elif state == "" or " ":
            shaperJob['State'] = "FAILED"
            shaperJob['recipe_name'] = ""
            shaperJob['ErrorMessage'] = ""
        else:
            return "Something went wrong. Please start your job again"
        return shaperJob
    except Exception as e:
        print("Job run status is not updated yet", e)

############################################## Getting TitleFile ####################################################################

def getTitleFile(titlefile):
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        if titlefile or request.method == 'POST':
            sql = "SELECT titlefile FROM datashaper_titlebucket WHERE titlebucket=%s LIMIT 1"
            val = (titlefile)
            cursor.execute(sql, val)
            result = cursor.fetchall()
            for row in result:
                existing_title_bucket = row[0]
            titlefile1 = existing_title_bucket
            return titlefile1
        elif titlefile == "":
            return ""
        else:
            return None
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

########################################## NLP Matches #############################################################################

def nlpMatch(inputcsvfile, inputfile_confidenceColumn, standard_titles, titlefile_matchColumn, threshold, del_template_outputColumn):
    try:
        if inputfile_confidenceColumn in inputcsvfile.columns:
            print("No error try block execution successfull")
        matchData = []
        accuracy = []
        for matchTitle in inputcsvfile[inputfile_confidenceColumn]:
            result = []
            for title in standard_titles[titlefile_matchColumn]:
                result.append(get_similarity(title, matchTitle)*100)
            standard_titles['match_prob'] = result
            rslt_df = standard_titles[standard_titles['match_prob'] > threshold]
            match_df = rslt_df[rslt_df['match_prob']
                               == rslt_df['match_prob'].max()]
            result = rslt_df['match_prob'].max()
            accuracy.append(result)
            match_result = 'No Match' if match_df.empty else match_df[titlefile_matchColumn].values[0]
            matchData.append(match_result)
        inputcsvfile[del_template_outputColumn] = matchData
        inputcsvfile[del_template_outputColumn]['Accuracy'] = accuracy
    except Exception as e:
        print("Fail while reading file from s3")
        print(e)
    return "Success"

########################################## RUNNING NLP Match ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/runnlpmatch', methods=['POST'])
def runnlpbatchjob():
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        bucketName = _json['bucket_name']
        fileName1 = _json['csv_filename1']
        delivery_template_input_file = _json['delivery_template_input_file']
        matchId = _json['matchId']
        recipe_name = _json['recipe_name']
        employeesColumn = _json['inputfile_employeesColumn']
        revenueColumn = _json['inputfile_revenueColumn']

        # Step1
        threshold1 = _json['threshold1']
        titlefile1 = _json['titlefile1']
        titlefile1 = getTitleFile(titlefile1)
        inputfile_confidenceColumn1 = _json['inputfile_confidenceColumn1']
        titlefile1_matchColumn = _json['titlefile1_matchColumn']
        del_template_outputColumn1 = _json['del_template_outputColumn1']
        # Step 2
        threshold2 = _json['threshold2']
        titlefile2 = _json['titlefile2']
        titlefile2 = getTitleFile(titlefile2)
        # print(titlefile2)
        inputfile_confidenceColumn2 = _json['inputfile_confidenceColumn2']
        titlefile2_matchColumn = _json['titlefile2_matchColumn']
        del_template_outputColumn2 = _json['del_template_outputColumn2']

        # Step 3
        threshold3 = _json['threshold3']
        titlefile3 = _json['titlefile3']
        titlefile3 = getTitleFile(titlefile3)
        inputfile_confidenceColumn3 = _json['inputfile_confidenceColumn3']
        titlefile3_matchColumn = _json['titlefile3_matchColumn']
        del_template_outputColumn3 = _json['del_template_outputColumn3']

        # Step 4
        threshold4 = _json['threshold4']
        titlefile4 = _json['titlefile4']
        titlefile4 = getTitleFile(titlefile4)
        inputfile_confidenceColumn4 = _json['inputfile_confidenceColumn4']
        titlefile4_matchColumn = _json['titlefile4_matchColumn']
        del_template_outputColumn4 = _json['del_template_outputColumn4']

        # print(_json)

        inputcsvfile = readCsvFileFromBucket(bucketName, fileName1)
        standard_titles1 = readCsvFileFromBucket(bucketName, titlefile1)
        standard_titles2 = readCsvFileFromBucket(bucketName, titlefile2)
        standard_titles3 = readCsvFileFromBucket(bucketName, titlefile3)
        standard_titles4 = readCsvFileFromBucket(bucketName, titlefile4)
        #################################################################################################################
        
        employeesColumnProcess = employeesColumnprocess(inputcsvfile, employeesColumn)
        revenueColumnProcess = revenueColumnprocess(inputcsvfile, revenueColumn)
        nlpMatchProcess1 = nlpMatch(inputcsvfile, inputfile_confidenceColumn1, standard_titles1, titlefile1_matchColumn, threshold1, del_template_outputColumn1)
        nlpMatchProcess2 = nlpMatch(inputcsvfile, inputfile_confidenceColumn2, standard_titles2, titlefile2_matchColumn, threshold2, del_template_outputColumn2)
        nlpMatchProcess3 = nlpMatch(inputcsvfile, inputfile_confidenceColumn3, standard_titles3, titlefile3_matchColumn, threshold3, del_template_outputColumn3)
        nlpMatchProcess4 = nlpMatch(inputcsvfile, inputfile_confidenceColumn4, standard_titles4, titlefile4_matchColumn, threshold4, del_template_outputColumn4)
        ####################################################################################################################################
        
        client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                                aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)

        bucket = bucketName
        inp = fileName1
        fileName = inp.split(".")
        # new_name = re.sub(r'[0123456789]','', fileName1)
        your_string = re.sub(r'[\\!~`@#$%^&*_=}+[{/);/,*?:"<>|(-]',"", fileName[0])
        # your_string = your_string + '.csv'
        copy_source = {'Bucket': bucket, 'Key': fileName1}
        client.copy_object(CopySource=copy_source, Bucket=bucket, Key=your_string)
        # client.delete_object(Bucket=bucket, Key=fileName1)
        
        fileName2 = your_string
        # print(fileName1)
        inp = fileName2
        fileName2 = inp.split(".")
        outputFile1 = fileName2[0] + "-" + \
            str(datetime.now().strftime('-%m-%d-%Y-%H-%M-%S')) + '.csv'
        csv_buffer = StringIO()
        inputcsvfile.to_csv(csv_buffer, index=False)
        response = client.put_object(
            Bucket=bucket, Key=outputFile1, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful titlematch output S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful titlematch output S3 put_object response. Status - {status}")
        #########################################################################################################################################
        
        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                )

        Name = "nlpOutput-" + fileName2[0] + \
            str(datetime.now().strftime('%m-%d-%Y-%H-%M-%S'))
        Key = outputFile1
        DatasetName = leadshaper_create_dataset(Key, Name, bucket)
        Name = DatasetName

        # jobName = fileName1[0]
        # Name = fileName1[0] + "-" + \
        #     str(datetime.now().strftime('%m-%d-%Y-%H-%M-%S'))
        JobName = leadshaper_create_recipe_job(DatasetName, Name, bucket, recipe_name)
        databrewJobName = JobName

        job_RunId = leadshaper_start_job_run(databrewJobName)
        jobRunId = job_RunId

        print("the datashaper job has started")
        time.sleep(370)
        print("waiting for the datashaper job to be finished")

        job_run_response = leadshaper_describe_job_run(databrewJobName, jobRunId)
        
        state = job_run_response["State"]
        # recipe_name = job_run_response['RecipeReference']['Name']
        # error_log = job_run_response['ErrorMessage']
        print(state)
        recipe_name = recipe_name
        #########################################################################################################################################
        if state == "SUCCEEDED":
            conn = mysql.connect()
            cursor = conn.cursor()
            outputData = {}
            var = outputFile1
            job_name = databrewJobName
            dataset_name = DatasetName
            # Renaming output file
            outputName = databrewJobName + "_part00000" + '.csv'
            nlp_output_file = databrewJobName + '.csv'
            client = boto3.client('s3', region_name = "us-east-1", 
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
            copy_source = {'Bucket': bucket, 'Key': outputName}
            client.copy_object(CopySource=copy_source, Bucket=bucket, Key=nlp_output_file)
            client.delete_object(Bucket=bucket, Key=outputName)
            client.delete_object(Bucket=bucket, Key=your_string)
            # Here
            run_id = jobRunId
            error_log = "None"
            status = "Start Shaping Step"
            input_file = fileName1
            nlp_output_status = "Completed"
            sql = "UPDATE leadshaper_delivery_template SET nlp_output_status=%s, input_file=%s, match_file=%s, job_name=%s, dataset_name=%s, nlp_output_file=%s, recipe_name=%s, run_id=%s, error_log=%s, status=%s WHERE nlp_id = %s"
            val = (nlp_output_status, input_file, var, job_name, dataset_name, nlp_output_file,
                   recipe_name, run_id, error_log, status, str(matchId))
            cursor.execute(sql, val)
            conn.commit()
            outputData['matchFile'] = var
            outputData['nlp_output_file'] = nlp_output_file
            outputData['jobStatus'] = state
            outputData['status'] = 200
            del standard_titles1
            del standard_titles2
            del standard_titles3
            del standard_titles4
            del inputcsvfile
            return outputData

        elif state == "RUNNING":
            time.sleep(180)
            conn = mysql.connect()
            cursor = conn.cursor()
            outputData2 = {}
            var2 = outputFile1
            # Renaming output file
            outputName1 = databrewJobName + "_part00000" + '.csv'
            nlp_output_file2 = databrewJobName + '.csv'
            client = boto3.client('s3', region_name = "us-east-1", 
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
            copy_source = {'Bucket': bucket, 'Key': outputName1}
            client.copy_object(CopySource=copy_source, Bucket=bucket, Key=nlp_output_file2)
            client.delete_object(Bucket=bucket, Key=outputName1)
            client.delete_object(Bucket=bucket, Key=your_string)
            # Here
            job_name2 = databrewJobName
            dataset_name2 = DatasetName
            run_id2 = jobRunId
            error_log2 = "None"
            input_file = fileName1
            nlp_output_status = "In Progress"
            sql2 = "UPDATE leadshaper_delivery_template SET nlp_output_status=%s, match_file = %s, job_name = %s, dataset_name = %s, nlp_output_file = %s, recipe_name = %s, run_id = %s, error_log = %s, status = %s WHERE nlp_id = %s"
            val2 = (nlp_output_status, input_file, var2, job_name2, dataset_name2, nlp_output_file2,
                    recipe_name, run_id2, error_log2, state, str(matchId))
            cursor.execute(sql2, val2)
            conn.commit()
            outputData2['matchFile'] = var2
            outputData2['nlp_output_file'] = nlp_output_file2
            # outputData1['error_log'] = error_file
            outputData2['jobStatus'] = state
            outputData2['status'] = 200
            del standard_titles1
            del standard_titles2
            del standard_titles3
            del standard_titles4
            del inputcsvfile
            return outputData2

        elif state == "FAILED" or "" or " ":
            conn = mysql.connect()
            cursor = conn.cursor()
            # Storing error logs in S3
            # error_logs = job_run_response['ErrorMessage']
            error_log1 = job_run_response['ErrorMessage']
            error_file1 = "error-log-" + str(datetime.now().strftime('%m-%d-%Y-%H-%M-%S')) + ".txt"
            client = boto3.client('s3', region_name = "us-east-1", 
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
            s3_response = client.put_object(Body=error_log1, Bucket=bucket, Key=error_file1)
            client.delete_object(Bucket=bucket, Key=your_string)
            status = s3_response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful error-log S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful error-log S3 put_object response. Status - {status}")
            # Here
            outputData1 = {}
            var1 = outputFile1
            job_name1 = databrewJobName
            dataset_name1 = DatasetName
            nlp_output_file1 = ""
            run_id1 = jobRunId
            state1 = "Delivery Template job is " + state + ". Please check the Error Logs"
            input_file = fileName1
            nlp_output_status = "FAILED - RUN NLP Step again"
            status1 = "FAILED - RUN NLP Step again"
            sql1 = "UPDATE leadshaper_delivery_template SET nlp_output_status=%s, input_file = %s, match_file = %s, job_name = %s, dataset_name = %s, nlp_output_file = %s, recipe_name = %s, run_id = %s, error_log = %s, status = %s WHERE nlp_id = %s"
            val1 = (nlp_output_status, input_file, var1, job_name1, dataset_name1, nlp_output_file1,
                    recipe_name, run_id1, error_file1, status1, str(matchId))
            cursor.execute(sql1, val1)
            conn.commit()
            outputData1['matchFile'] = var1
            outputData1['nlp_output_file'] = state1
            outputData1['error_log'] = error_file1
            outputData1['jobStatus'] = state
            outputData1['status'] = 200
            del standard_titles1
            del standard_titles2
            del standard_titles3
            del standard_titles4
            del inputcsvfile
            return outputData1
        else:
            status = "The job is {}".format(state)
            return status

    except KeyError as err:
        outputData['Response'] = 'KeyError  given input column "%s" is not present in inputfile' % str(
            err)
        outputData['status'] = 400
    except TypeError as t:
        outputData['Response'] = 'Type Error - reason "%s"' % str(t)
        outputData['status'] = 401
    finally:
        cursor.close()
        conn.close()
    # return outputData

########################################## Show NLP IDs ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/shownlpids', methods=['GET'])
def get_nlpIds():
    response = []
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT nlp_id FROM leadshaper_delivery_template")
        nlp_ids = cursor.fetchall()
        for row in nlp_ids:
            nlp_id = {}
            nlp_id['nlp id'] = row[0]
            response.append(nlp_id)
        response = jsonify(response)
        response.status_code = 200
        return response
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

########################################## GET NLP Output File ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/getnlpOutputFile', methods=['POST'])
def get_nlp_output_file():
    get_nlp_output_file = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        nlp_id = _json['nlp_id']
        query = "SELECT delivery_template_recipe_name, nlp_output_file, delivery_template_input_file, nlp_id FROM leadshaper_delivery_template WHERE nlp_id = %s"
        value = (nlp_id)
        cursor.execute(query, value)
        get_nlp_output_files = cursor.fetchall()
        for row in get_nlp_output_files:
            get_nlp_output_file['delivery_template_recipe_name'] = row[0]
            get_nlp_output_file['nlp_output_file'] = row[1]
            get_nlp_output_file['delivery_template_input_file'] = row[2]
        nlp_output_file = jsonify(get_nlp_output_file)
        nlp_output_file.status_code = 200
        return nlp_output_file
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

########################################## Insert Recipe Name ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/insertRecipeName', methods=['POST'])
def insertRecipeName():
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        delivery_template_recipe_name = _json['recipe_Name']
        nlp_id = _json['nlp_id']
        # _json = request.json
        # recipe_Name = _json['recipe_Name']
        # recipe_Name = "batchonetemp"

        """
        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                       )
        publish_recipe_response = databrew_client.publish_recipe(
                Name = delivery_template_recipe_name
        )
        """

        # recipe_name = publish_recipe_response['Name']
        # recipe_name = jsonify("Recipe successfully generated")
        shaping_status = "Completed"
        status = "Start Lead shaper output Step"
        query = "UPDATE leadshaper_delivery_template SET delivery_template_recipe_name = %s, shaping_status=%s, status=%s WHERE nlp_id = %s"
        value = (delivery_template_recipe_name, shaping_status, status, nlp_id)
        cursor.execute(query, value)
        conn.commit()
        outputData = {}
        outputData['response'] = "Shaping step completed successfully"
        outputData['delivery_template_recipe_name'] = delivery_template_recipe_name
        outputData['status'] = 200
        return outputData
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

########################################## GET Lead Shaper Files ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/getLeadShaperFiles', methods=['POST'])
def get_lead_shaper_files():
    get_lead_shaper_file = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        nlp_id = _json['nlp_id']
        query = "SELECT nlp_output_file, delivery_template_recipe_name, nlp_id FROM leadshaper_delivery_template WHERE nlp_id = %s"
        value = (nlp_id)
        cursor.execute(query, value)
        get_lead_shaper_files = cursor.fetchall()
        for row in get_lead_shaper_files:
            get_lead_shaper_file['nlp_output_file'] = row[0]
            get_lead_shaper_file['delivery_template_recipe_name'] = row[1]
        get_lead_shaper = jsonify(get_lead_shaper_file)
        get_lead_shaper.status_code = 200
        return get_lead_shaper
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

########################################## GET Status for Leadshaper Steps ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/getStatus', methods=['POST'])
def getStatus():
    launchstatus = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        nlp_id = _json['nlp_id']
        query = "SELECT nlp_id, nlp_output_status, nlp_Launch, shaping_status, shaping_Launch, leadshaper_output_status, leadshaper_Launch FROM leadshaper_delivery_template WHERE nlp_id = %s"
        value = (nlp_id)
        cursor.execute(query, value)
        status1 = cursor.fetchall()
        for row in status1:
            launchstatus['Batch_No'] = row[0]
            launchstatus['nlp_output_status'] = row[1]
            launchstatus['nlp_Launch'] = row[2]
            launchstatus['shaping_status'] = row[3]
            launchstatus['shaping_Launch'] = row[4]
            launchstatus['leadshaper_output_status'] = row[5]
            launchstatus['leadshaper_Launch'] = row[6]
        launchstatus = jsonify(launchstatus)
        launchstatus.status_code = 200
        return launchstatus
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/getLeadshaperStepsStatus', methods=['GET', 'POST'])
def getLeadshaperStepsStatus():
    response = []
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        query = "SELECT nlp_id, nlp_output_status,nlp_Launch, shaping_status, shaping_Launch, leadshaper_output_status, leadshaper_Launch FROM leadshaper_delivery_template ORDER BY nlp_id DESC"
        cursor.execute(query)
        status1 = cursor.fetchall()
        for row in status1:
            status = {}
            status['Batch_No'] = row[0]
            status['nlp_output_status'] = row[1]
            status['nlp_Launch'] = row[2]
            status['shaping_status'] = row[3]
            status['shaping_Launch'] = row[4]
            status['leadshaper_output_status'] = row[5]
            status['leadshaper_Launch'] = row[6]
            response.append(status)
        response = jsonify(response)
        response.status_code = 200
        return response
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

########################################## Insert Input ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/insertInput', methods=['POST'])
def insertInput():
    conn = mysql.connect()
    cursor = conn.cursor()
    try:
        _json = request.json
        input_file = _json['csv_filename1']
        Lead_shaper_file = ""
        status = "In progress"
        print(_json, "Json data")
        created_at = datetime.now()
        error_log = "None"
        # dataset_name = ""
        match_file = ""
        recipe_name = ""
        if input_file or match_file or created_at or status or request.method == 'POST':
            sqlQuery = "INSERT INTO leadshaper_delivery_template(input_file, match_file, nlp_output_file, recipe_name, created_at, error_log, status) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            bindData = (input_file, match_file, Lead_shaper_file,
                        recipe_name, created_at, error_log, status)
            cursor.execute(sqlQuery, bindData)
            conn.commit()
            insertRecord = jsonify('record added successfully!')
            id = get_leadshaper_id()
            print(id, "datashaper_id")
            id.status_code = 200
            return id
        else:
            return "error"
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


def get_leadshaper_id():
    get_nlp_id = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT nlp_id FROM leadshaper_delivery_template ORDER BY nlp_id DESC LIMIT 1")
        get_nlp_ids = cursor.fetchall()
        print(get_nlp_ids)
        get_nlp_id['nlp_id'] = get_nlp_ids[0][0]
        titleMatchFetch = jsonify(get_nlp_id)
        titleMatchFetch.status_code = 200
        return titleMatchFetch
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


def updating_del_temp_status(matchId):
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        leadshaper_output_status = "In Progress"
        sql = "UPDATE leadshaper_delivery_template SET leadshaper_output_status=%s WHERE nlp_id = %s"
        val = (leadshaper_output_status, str(matchId))
        cursor.execute(sql, val)
        conn.commit()
        return "Success"
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()

########################################## CREATE Lead Shaper JOB ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/createLeadShaperJob', methods=['POST'])
def createLeadShaperJob():
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        bucketName = _json['bucket_name']
        fileName1 = _json['csv_filename1']
        recipe_name = _json['recipe_name']
        recipe_name1 = _json['recipe_name1']
        matchId = _json['matchId']
        update_status = updating_del_temp_status(matchId)
        print(_json)
        # inputcsvfile = readCsvFileFromBucket(bucketName, fileName1)
        #########################################################################################################################################

        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                )
        client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY_ID,
                            aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)

        bucket = bucketName
        inp = fileName1
        fileName = inp.split(".")
        new_name = re.sub(r'[0123456789]','', fileName[0])
        your_string = re.sub(r'[\\!~`@#$%^&*_=}+[{/);/,*?:"<>|(-]',"", new_name)
        your_string = your_string + '.csv'
        copy_source = {'Bucket': bucket, 'Key': fileName1}
        client.copy_object(CopySource=copy_source, Bucket=bucket, Key=your_string)
        # client.delete_object(Bucket=bucket, Key=fileName1)
        
        fileName2 = your_string
        Key = fileName2
        inp = fileName2
        fileName2 = inp.split(".")

        # datasetName = fileName1[0]
        Name = "leadshaperOutput-" + fileName2[0] + "-" + \
            str(datetime.now().strftime('%m-%d-%Y-%H-%M-%S'))

        DatasetName = leadshaper_create_dataset(Key, Name, bucket)
        Name = DatasetName

        # jobName = fileName1[0]
        # Name = fileName1[0] + "-" + \
        #     str(datetime.now().strftime('%m-%d-%Y-%H-%M-%S'))
        JobName = leadshaper_create_recipe_job(DatasetName, Name, bucket, recipe_name)
        databrewJobName = JobName

        job_RunId = leadshaper_start_job_run(databrewJobName)
        jobRunId = job_RunId

        print("the datashaper job has started")
        time.sleep(370)
        print("waiting for the datashaper job to be finished")

        job_run_response = databrew_client.describe_job_run(
            Name=databrewJobName,
            RunId=jobRunId
        )
        state = job_run_response["State"]
        # recipe_name = job_run_response['RecipeReference']['Name']
        # error_log = job_run_response['ErrorMessage']
        print(state)
        recipe_name = recipe_name
        
        """
        DatasetName = leadshaper_create_dataset(Key, Name, bucket)
        Name = DatasetName

        # jobName = fileName1[0]
        # Name = fileName1[0] + "-" + \
        #     str(datetime.now().strftime('%m-%d-%Y-%H-%M-%S'))
        JobName = leadshaper_create_recipe_job(DatasetName, Name, bucket, recipe_name)
        databrewJobName = JobName

        job_RunId = leadshaper_start_job_run(databrewJobName)
        jobRunId = job_RunId

        print("the datashaper job has started")
        time.sleep(10)
        print("waiting for the datashaper job to be finished")

        job_run_response = leadshaper_describe_job_run(databrewJobName, jobRunId)

        if recipe_name == "" or " ":
            job_run_response = {}
            job_run_response["State"] = "FAILED"
            job_run_response['ErrorMessage'] = " "
        
        # recipe_name = job_run_response['RecipeReference']['Name']
        # error_log = job_run_response['ErrorMessage']
        state = job_run_response["State"]
        print(state)
        recipe_name = recipe_name
        """

        """
        create_dataset_response = databrew_client.create_dataset(
            Name=Name,
            Format='CSV',
            FormatOptions={
                'Csv': {
                    'Delimiter': ',',
                    'HeaderRow': True
                }
            },
            Input={
                'S3InputDefinition': {
                    'Bucket': bucket,
                    'Key': Key
                },
            }
        )

        DatasetName = create_dataset_response['Name']
        # jobName = fileName1[0]
        # Name = fileName1[0] + "-" + str(datetime.now().strftime('%m-%d-%Y-%H-%M-%S'))
        recipe_job_response = databrew_client.create_recipe_job(
            DatasetName=DatasetName,
            Name=DatasetName,
            LogSubscription='DISABLE',
            MaxCapacity=2,
            MaxRetries=0,
            Outputs=[
                {
                    'Format': 'CSV',
                    'Location': {
                        'Bucket': bucket
                    },
                    'Overwrite': True,
                    'FormatOptions': {
                        'Csv': {
                            'Delimiter': ','
                        }
                    },
                },
            ],
            RecipeReference={
                'Name': recipe_name,
                'RecipeVersion': '1.0'
            },
            RoleArn=role,
            Timeout=300
        )
        # print(recipe_job_response["Name"])

        databrewJobName = recipe_job_response["Name"]
        start_job_response = databrew_client.start_job_run(
            Name=databrewJobName
        )
        # print(start_job_response["RunId"])

        jobRunId = start_job_response["RunId"]
        print("the datashaper job has started")
        time.sleep(370)
        print("waiting for the datashaper job to be finished")
        job_run_response = databrew_client.describe_job_run(
            Name=databrewJobName,
            RunId=jobRunId
        )

        state = job_run_response["State"]
        print(state)
        recipe_name = job_run_response['RecipeReference']['Name']
        # error_log = job_run_response['ErrorMessage']
        """
        #########################################################################################################################################
        if state == "SUCCEEDED":
            conn = mysql.connect()
            cursor = conn.cursor()
            outputData = {}
            # var = "None"
            job_name = databrewJobName
            dataset_name = DatasetName
            # Renaming the output file
            outputName1 = databrewJobName + "_part00000" + '.csv'
            leadshaper_output_file = databrewJobName + '.csv'
            client = boto3.client('s3', region_name = "us-east-1", 
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
            copy_source = {'Bucket': bucket, 'Key': outputName1}
            client.copy_object(CopySource=copy_source, Bucket=bucket, Key=leadshaper_output_file)
            client.delete_object(Bucket=bucket, Key=outputName1)
            client.delete_object(Bucket=bucket, Key=your_string)
            # Up-to here
            run_id = jobRunId
            error_log = "None"
            status = "Completed"
            leadshaper_output_status = "Completed"
            # input_file = fileName1
            sql = "UPDATE leadshaper_delivery_template SET leadshaper_output_status=%s, job_name1 = %s, dataset_name1 = %s, leadshaper_output_file = %s, recipe_name1 = %s, run_id1 = %s, error_log = %s, status = %s WHERE nlp_id = %s"
            val = (leadshaper_output_status, job_name, dataset_name, leadshaper_output_file,
                   recipe_name, run_id, error_log, status, str(matchId))
            cursor.execute(sql, val)
            conn.commit()
            # outputData['matchFile'] = var
            outputData['leadshaper_output_file'] = leadshaper_output_file
            outputData['jobStatus'] = state
            outputData['inputFile'] = fileName1
            outputData['status'] = 200
            # return outputData
        elif state == "RUNNING":
            conn = mysql.connect()
            cursor = conn.cursor()
            time.sleep(180)
            outputData2 = {}
            # var2 = "None"
            # Renaming output file
            outputName = databrewJobName + "_part00000" + '.csv'
            leadshaper_output_file2 = databrewJobName + '.csv'
            client = boto3.client('s3', region_name = "us-east-1", 
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
            copy_source = {'Bucket': bucket, 'Key': outputName}
            client.copy_object(CopySource=copy_source, Bucket=bucket, Key=leadshaper_output_file2)
            client.delete_object(Bucket=bucket, Key=outputName)
            client.delete_object(Bucket=bucket, Key=your_string)
            # Up-to here
            job_name2 = databrewJobName
            dataset_name2 = DatasetName
            run_id2 = jobRunId
            error_log2 = "None"
            # input_file = fileName1
            leadshaper_output_status = "In Progress"
            sql2 = "UPDATE leadshaper_delivery_template SET leadshaper_output_status =%s, job_name1 = %s, dataset_name1 = %s, leadshaper_output_file = %s, recipe_name1 = %s, run_id1 = %s, error_log = %s, status = %s WHERE nlp_id = %s"
            val2 = (leadshaper_output_status, job_name2, dataset_name2, leadshaper_output_file2,
                    recipe_name, run_id2, error_log2, state, str(matchId))
            cursor.execute(sql2, val2)
            conn.commit()
            # outputData2['matchFile'] = var2
            outputData2['leadshaper_output_file'] = leadshaper_output_file2
            # outputData1['error_log'] = error_file
            outputData2['jobStatus'] = state
            outputData2['inputFile'] = fileName1
            outputData2['status'] = 200
            return outputData2
        elif state == "FAILED" or "" or " ":
            conn = mysql.connect()
            cursor = conn.cursor()
            # Storing the error logs in S3
            error_log1 = job_run_response['ErrorMessage']
            error_file1 = "error-log-" + str(datetime.now().strftime('%m-%d-%Y-%H-%M-%S')) + ".txt"
            client = boto3.client('s3', region_name = "us-east-1", 
                                    aws_access_key_id=AWS_ACCESS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
            s3_response = client.put_object(Body=error_log1, Bucket=bucket, Key=error_file1)
            client.delete_object(Bucket=bucket, Key=your_string)
            status = s3_response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 200:
                print(f"Successful error-log S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful error-log S3 put_object response. Status - {status}")
            # Up-to here
            outputData1 = {}
            # var1 = "None"
            job_name1 = databrewJobName
            dataset_name1 = DatasetName
            leadshaper_output_file1 = ""
            run_id1 = jobRunId
            state1 = "Delivery Template job is " + state + \
                ". Please check the Error Logs in Dashboard"
            # input_file = fileName1
            leadshaper_output_status = "FAILED - RUN Lead shaper output Step Again"
            status1 = "FAILED - RUN Lead shaper output Step Again"
            sql1 = "UPDATE leadshaper_delivery_template SET leadshaper_output_status=%s, job_name1 = %s, dataset_name1 = %s, leadshaper_output_file = %s, recipe_name1 = %s, run_id1 = %s, error_log = %s, status = %s WHERE nlp_id = %s"
            val1 = (leadshaper_output_status, job_name1, dataset_name1, leadshaper_output_file1,
                    recipe_name, run_id1, error_file1, status1, str(matchId))
            # cursor.execute('set GLOBAL max_allowed_packet=67108864')
            # print(mysql.connector.__version__)
            cursor.execute(sql1, val1)
            conn.commit()
            # outputData1['matchFile'] = var1
            outputData1['leadshaper_output_file'] = state1
            outputData1['error_log'] = error_file1
            outputData1['jobStatus'] = state
            outputData1['inputFile'] = fileName1
            outputData1['status'] = 200
            return outputData1
        else:
            status = "The job is {}".format(state)
            return status
    except KeyError as err:
        outputData['Response'] = 'KeyError  given input column "%s" is not present in inputfile' % str(err)
        outputData['status'] = 400
    except TypeError as t:
        outputData['Response'] = 'Type Error - reason "%s"' % str(t)
        outputData['status'] = 401
    finally:
        cursor.close()
        conn.close()
    return outputData

########################################## Listing Recipes ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/listrecipes', methods=['GET'])
def list_recipes():
    try:
        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                       )
        response = databrew_client.list_recipes(
            MaxResults=100,
            RecipeVersion='LATEST_WORKING'
        )
        recipe_names = []
        for row in response["Recipes"]:
            data = {}
            data['recipe'] = row['Name']
            recipe_names.append(data)
        # return jsonify(str(recipe_names))
    except Exception as e:
        print(e)
    finally:
        return jsonify(recipe_names)

########################################## Listing Recipes Screen ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/showRecipes', methods=['GET'])
def recipes():
    try:
        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                       )

        default_bucket_name = "leadshaperdev"
        response = databrew_client.list_recipes(
            MaxResults=1,
            RecipeVersion='LATEST_WORKING'
        )
        # recipe_names = []
        for row in response["Recipes"]:
            recipe_names = []
            data = {}
            recipe = {}
            recipes = []
            data['recipe_name'] = row['Name']
            data['Created Date'] = row['CreateDate']
            recipe_name = row['Name']
            recipe_file = recipe_name + ".json"
            data['download recipe'] = recipe_file
            recipe_names.append(data)
            recipe['recipe'] = row['Steps']
            recipes.append(recipe)
            print(recipes)
            recipe = row['Steps']
        client = boto3.client('s3', region_name="us-east-1", aws_access_key_id=AWS_ACCESS_KEY_ID,
                                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)
        # for row in recipes:
            # print(row)
        s3_response = client.put_object(
                        Body=json.dumps(recipe),
                        Bucket=default_bucket_name,
                        Key=recipe_file
                    )
        status = s3_response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful S3 put_object response. Status - {status}")
    except Exception as e:
        print("Exception occurred: ", e)
    return jsonify(str(recipe_names), row)
    # finally:
        # return jsonify(str(recipe_names))

########################################## Delete Dataset ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/deletedataset', methods=['POST'])
def delete_dataset():
    try:
        _json = request.json
        datasetName = _json['dataset_name']
        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                       )
        response = databrew_client.delete_dataset(
            Name=datasetName
        )
        name = response["Name"]
        datasetName = jsonify(name)
    except Exception as e:
        print(e)
    finally:
        return datasetName

########################################## Delete JOB ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/deletejob', methods=['POST'])
def delete_job():
    try:
        _json = request.json
        jobName = _json['job_name']
        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                       )
        response = databrew_client.delete_job(
            Name=jobName
        )
        name = response["Name"]
        jobName = jsonify(name)
    except Exception as e:
        print(e)
    finally:
        return jobName


# @cross_origin(origins='*')
@app.route('/datashaper/createShapingRecipe', methods=['POST'])
def createShapingRecipe():
    try:
        _json = request.json
        jobName = _json['job_name']
        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                       )
        create_recipe_response = databrew_client.create_recipe(
            Description = 'string',
            Name = 'string',
            Steps = [
                {
                'Action': {
                    'Operation': 'string',
                    'Parameters': {
                        'string': 'string'
                    }
                },
                'ConditionExpressions': [
                        {
                        'Condition': 'string',
                        'Value': 'string',
                        'TargetColumn': 'string'
                        },
                    ]
                },
            ]
        )
        name = create_recipe_response["Name"]
        recipeName = jsonify(name)
    except Exception as e:
        print(e)
    finally:
        return recipeName


# @cross_origin(origins='*')
@app.route('/datashaper/publishShapingRecipe', methods=['POST'])
def publishShapingRecipe():
    try:
        _json = request.json
        recipe_Name = _json['recipe_Name']
        # recipe_Name = "batchonetemp"
        databrew_client = boto3.client('databrew', region_name="us-east-1",
                                       aws_access_key_id=AWS_ACCESS_KEY_ID,
                                       aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID
                                       )
        publish_recipe_response = databrew_client.publish_recipe(
                Name = recipe_Name
        )

        recipe_name = publish_recipe_response['Name']
        recipe_name = jsonify("Recipe successfully generated")
    except Exception as e:
        print(e)
    finally:
        return recipe_name


"""
create_recipe_response = databrew_client.create_recipe(
    Description = 'string',
    Name = 'string',
    Steps = [
        {
            'Action': {
                'Operation': 'string',
                'Parameters': {
                    'string': 'string'
                }
            },
            'ConditionExpressions': [
                {
                    'Condition': 'string',
                    'Value': 'string',
                    'TargetColumn': 'string'
                },
            ]
        },
    ]
)

Name = create_recipe_response['Name']
publish_recipe_response = databrew_client.publish_recipe(
    Name = Name
)

recipe_name = publish_recipe_response['Name']
"""

########################################## Datashaper Campaign Details ################################################################

# @cross_origin(origins='*')
@app.route('/datashaper/campaign', methods=['POST'])
def insertDatashaperCampaignDetails():
    try:
        _json = request.json
        _campaign_id = _json['campaign_id']
        _job_name = _json['job_name']
        _start_time = _json['start_time']
        _tool_assignee_user_email = _json['tool_assignee_user_email']
        _tool_campg_id = _json['tool_campg_id']
        if _campaign_id or _job_name or _start_time or _tool_assignee_user_email or _tool_campg_id or request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor()
            sqlQuery = "INSERT INTO datashaper_campaign(campaign_id, job_name, start_time, tool_assignee_user_email, tool_campg_id) VALUES(%s, %s, %s, %s, %s)"
            bindData = (_campaign_id, _job_name, _start_time,
                        _tool_assignee_user_email, _tool_campg_id)
            cursor.execute(sqlQuery, bindData)
            conn.commit()
            insertRecord = jsonify('datashaperCampId added successfully!')
            insertRecord.status_code = 200
            return insertRecord
        else:
            return showMessage()

    except Exception as e:
        print(e)

    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/getAllDatashaperRecords', methods=['GET'])
def getDatashaperRecords():
    try:
        datashaper_data = []
        conn = mysql.connect()
        cursor = conn.cursor()
        cursor.execute("SELECT rowid, tool_campg_id, job_name, tool_assignee_user_email, start_time, campaign_id  FROM datashaper_campaign")
        campRows = cursor.fetchall()
        for row in campRows:
            new = {}
            new['rowid'] = row[0]
            new['tool_campg_id'] = row[1]
            new['job_name'] = row[2]
            new['tool_assignee_user_email'] = row[3]
            new['start_time'] = row[4]
            new['campaign_id'] = row[5]
            datashaper_data.append(new)
        getRecord = jsonify(datashaper_data)
        getRecord.status_code = 200
        return getRecord
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/getDatashaperRecordByJobName', methods=['GET'])
def getDatashaperRecordByJobName():
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json()
        job_name = _json['job_name']
        cursor.execute(
            "SELECT rowid, tool_campg_id, job_name, tool_assignee_user_email, start_time, campaign_id FROM datashaper_campaign WHERE job_name =%s", job_name)
        campRow = cursor.fetchall()
        for row in campRow:
            new = {}
            new['rowid'] = row[0]
            new['tool_campg_id'] = row[1]
            new['job_name'] = row[2]
            new['tool_assignee_user_email'] = row[3]
            new['start_time'] = row[4]
            new['campaign_id'] = row[5]
        getRecord = jsonify(new)
        getRecord.status_code = 200
        return getRecord
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/updatebyJobName', methods=['POST'])
def updateDatashaperRecordByJobName():
    try:
        _json = request.json
        _rowid = _json['rowid']
        _tool_campg_id = _json['tool_campg_id']
        _job_name = _json['job_name']
        _tool_assignee_user_email = _json['tool_assignee_user_email']
        _start_time = _json['start_time']
        _campaign_id = _json['campaign_id']
        if _rowid and _tool_campg_id and _job_name and _tool_assignee_user_email and _start_time and _campaign_id and request.method == 'POST':
            conn = mysql.connect()
            cursor = conn.cursor()
            sql = "UPDATE datashaper_campaign SET tool_campg_id=%s, job_name=%s, tool_assignee_user_email=%s, start_time=%s, campaign_id=%s WHERE job_name=%s"
            data = (_rowid, _tool_campg_id, _job_name,
                    _tool_assignee_user_email, _start_time, _campaign_id)
            print("data is: ", data)
            cursor.execute(sql, data)
            conn.commit()
            updateRecord = jsonify('Datashaper Record updated successfully!')
            updateRecord.status_code = 200
            return updateRecord
        else:
            return showMessage()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/deletebyJobName', methods=['DELETE'])
def deleteDatashaperRecordByJobName():
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        job_name = _json['job_name']
        sqlQuery = "DELETE FROM datashaper_campaign WHERE job_name=%s"
        bindData = (job_name)
        cursor.execute(sqlQuery, bindData)
        conn.commit()
        resp = jsonify('Datashaper Record deleted successfully!')
        resp.status_code = 200
        return resp
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/jobrundescriptionOrErrorlog', methods=["GET"])
def jobrundescriptionOrErrorlog():
    response = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        job_name = _json['job_name']
        print(_json, "Json")
        sqlQuery = "SELECT job_run_description FROM datashaper_job_run WHERE job_name=%s"
        bindData = (job_name)
        cursor.execute(sqlQuery, bindData)
        myresult = cursor.fetchall()
        for row in myresult:
            response['job_run_description'] = row[0]
        return jsonify(response)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# @cross_origin(origins='*')
@app.route('/datashaper/datashaperjobstatus', methods=["GET"])
def datashaper_job_status():
    response = {}
    try:
        conn = mysql.connect()
        cursor = conn.cursor()
        _json = request.json
        job_name = _json['job_name']
        print(_json, "Json")
        sqlQuery = "SELECT datashaper_job_status FROM datashaper_job_run WHERE job_name=%s"
        bindData = (job_name)
        cursor.execute(sqlQuery, bindData)
        myresult = cursor.fetchall()
        for row in myresult:
            response['datashaper_job_status'] = row[0]
        return jsonify(response)
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


# # @cross_origin(origins='*')
@app.errorhandler(404)
def showMessage(error=None): 
    message = {
        'status': 404,
        'message': 'Record not found: ' + request.url,
    }
    respone = jsonify(message)
    respone.status_code = 404
    return respone

# def handler(event, context):
    # return serverless_wsgi.handle_request(app, event, context)


# # Lead Shaper server
# if __name__ == "__main__":
#     app.run(host="0.0.0.0", port=5000, debug=True)
