import json
import os
import boto3
import csv
from builtins import str
from werkzeug.security import generate_password_hash, check_password_hash
from flask_api import status
import datetime
from flask_httpauth import HTTPBasicAuth
from flask import request, jsonify
import random
from jproperties import Properties
import logging
from csv import writer
import math
import re
from nltk.corpus import wordnet as wn
from nltk.stem.porter import *
from nltk.corpus import stopwords
from collections import Counter
from mysql.connector import Error
import pymysql
import mysql.connector
from datetime import date
import nltk
from datetime import datetime, timedelta
import json
import flask
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS, cross_origin
from flaskext.mysql import MySQL
import boto3
from io import StringIO
import os
import boto.utils
from smart_open import smart_open
import unittest
import warnings
from  jsonmerge import merge
warnings.filterwarnings('ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)
# from c3papplication.common import Connections
# from c3papplication.discovery import c3p_snmp_disc_rec_new as CSDRN
auth = HTTPBasicAuth()
# app = Flask(__name__) #creating the Flask class object


users = {
    "root": generate_password_hash("hello"),
    "c3p": generate_password_hash("c3p")
}
#mydb = Connections.create_connection()
#mycursor = mydb.cursor(buffered=True)
filename = ""
configs = Properties()
with open('c3papplication/conf/app-config.properties', 'rb') as config_file:
  configs.load(config_file)
logger = logging.getLogger(__name__)


""" Authentification using Username and Password """
@auth.verify_password
def verify_password(username, password):
    if username in users and \
            check_password_hash(users.get(username), password):
        return username, status.HTTP_202_ACCEPTED
    else:
        return "NOT", status.HTTP_401_UNAUTHORIZED


# from c3papplication.common import Connections
logger = logging.getLogger(__name__)
configs = Properties()
with open('c3papplication/conf/app-config.properties', 'rb') as config_file:
  configs.load(config_file)

C3P_Application = (configs.get("C3P_Application").data).strip()

def lambda_handler(event, context):

    bucket = ""
    file_key = "Hotel_details.csv"
    s3 = boto3.client('s3')
 
    data = s3.get_object(Bucket=bucket, Key=file_key)['Body'].read().decode('utf-8').splitlines()
    lines = csv.reader(data)
    headers = next(lines)

    print('headers: %s' %(headers))

    for line in lines:
        # print complete line of city names
        print(line[4])
    return "Success"