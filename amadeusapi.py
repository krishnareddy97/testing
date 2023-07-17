from amadeus import Client, ResponseError
import logging
from amadeus import Location
# All imports
from csv import writer
import re
import mysql.connector
from datetime import datetime
import pandas as pd
import boto3
from datetime import datetime
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS, cross_origin
from flaskext.mysql import MySQL
from io import StringIO
from smart_open import smart_open
import unittest
import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter(action='ignore', category=FutureWarning)

################################### Logging ####################################################
logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)

# DEBUG: Detailed information, typically of interest only when diagnosing problems.
# INFO: Confirmation that things are working as expected.
# WARNING: An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.
# ERROR: Due to a more serious problem, the software has not been able to perform some function.
# CRITICAL: A serious error, indicating that the program itself may be unable to continue running.
# logging.basicConfig(level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')
"""
import logging
a = 10
b = 0
try:
  c = a / b
except Exception as e:
  logging.error("Exception Occurred", exc_info=True)  ## At default it is True

import logging
# custom logger
logger = logging.getLogger(__name__)

# Create handlers
c_handler = logging.StreamHandler()
f_handler = logging.FileHandler('file.log')
c_handler.setLevel(logging.WARNING)
f_handler.setLevel(logging.ERROR)

# Create formatters and add it to handlers
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
f_handler.setFormatter(f_format)

# Add handlers to the logger
logger.addHandler(c_handler)
logger.addHandler(f_handler)

logger.warning('This is a warning')
logger.error('This is an error')
logger.critical('This is an error')
"""
################################ Flask application ###################################################################
app = Flask(__name__)
# mysql = MySQL()

################################ Global Variable Declaration ########################################################
outputData = {}

################################### File Reading ############################################################################
# Function for reading the uploaded CSV input file for Natural Language Processing (NLP)


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


@cross_origin(origins='*')
@app.route('/amadeus/hotelsManagement', methods=['GET', 'POST'])
def amadeus_hotels():
    outputData = {}
    # conn = mysql.connect()
    # cursor = conn.cursor()
    try:
        _json = request.json
        cityCode = _json['cityCode']
        cityName = _json['cityName']
        countryCode = _json['countryCode']
        countryName = _json['countryName']
        radius = _json['radius']

        amadeus = Client(            
		1# logger=logger
            # log_level='debug'
        )

        bucketName = "curateddata-glue"

        # response = amadeus.shopping.flight_offers_search.get(
        #     originLocationCode='MAD',
        #     destinationLocationCode='ATH',
        #     departureDate='2022-11-01',
        #     adults=1)

        # response = amadeus.reference_data.locations.hotels.by_city.get(cityCode=cityCode)

        response = amadeus.reference_data.locations.hotels.by_city.get(
            cityCode=cityCode, radius=radius, radiusUnit="KM",
            amenities="SWIMMING_POOL,SPA,FITNESS_CENTER,AIR_CONDITIONING,RESTAURANT,PARKING,PETS_ALLOWED,AIRPORT_SHUTTLE,BUSINESS_CENTER,DISABLED_FACILITIES,WIFI,MEETING_ROOMS,NO_KID_ALLOWED,TENNIS,GOLF,KITCHEN,ANIMAL_WATCHING,BABY-SITTING,BEACH,CASINO,JACUZZI,SAUNA,SOLARIUM,MASSAGE,VALET_PARKING,BAR%20or%20LOUNGE,KIDS_WELCOME,NO_PORN_FILMS,MINIBAR,TELEVISION,WI-FI_IN_ROOM,ROOM_SERVICE,GUARDED_PARKG,SERV_SPEC_MENU",
            ratings="1,2,3,4,5", hotelSource="ALL")

        # response = amadeus.reference_data.locations.hotels.by_city.get(
        #     cityCode=cityCode,
        #     amenities="SWIMMING_POOL,SPA,FITNESS_CENTER,AIR_CONDITIONING,RESTAURANT,PARKING,PETS_ALLOWED,AIRPORT_SHUTTLE,BUSINESS_CENTER,DISABLED_FACILITIES,WIFI,MEETING_ROOMS,NO_KID_ALLOWED,TENNIS,GOLF,KITCHEN,ANIMAL_WATCHING,BABY-SITTING,BEACH,CASINO,JACUZZI,SAUNA,SOLARIUM,MASSAGE,VALET_PARKING,BAR%20or%20LOUNGE,KIDS_WELCOME,NO_PORN_FILMS,MINIBAR,TELEVISION,WI-FI_IN_ROOM,ROOM_SERVICE,GUARDED_PARKG,SERV_SPEC_MENU",
        #     ratings="1,2,3,4,5", hotelSource="ALL")

        client = boto3.client('s3', region_name="us-east-1",
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)

        df2 = pd.DataFrame(response.data)

        bucket = bucketName
        outputFile = "amadeus" + '.csv'
        csv_buffer = StringIO()
        df2.to_csv(csv_buffer, index=False)

        response = client.put_object(
            Bucket=bucket, Key=outputFile, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(
                f"Successful output S3 put_object response. Status - {status}")
        else:
            print(
                f"Unsuccessful output S3 put_object response. Status - {status}")

        # Reading the input dataset
        response = client.get_object(Bucket=bucket, Key="amadeus.csv")
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df = pd.read_csv(response.get("Body"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")

        df1 = df.rename(columns={'iataCode': 'cityName',
                        'name': 'hotelName', 'address': 'country'})

        df1['geoCode'] = df1['geoCode'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        df1['geoCode'] = df1['geoCode'].str.replace('latitude:', 'latitude: ')
        df1['geoCode'] = df1['geoCode'].str.replace('longitude:', 'longitude: ')
        df1['geoCode'] = df1['geoCode'].str.replace(',', ', ')

        df1['country'] = df1['country'].str.replace('countryCode:', '')
        df1['country'] = df1['country'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        df1['country'] = df1['country'].str.replace(countryCode, countryName)

        # df1['distance']= df1['distance'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        # df1['distance']= df1['distance'].str.replace('value:', '')
        # df1['distance']= df1['distance'].str.replace(',', ' ')
        # df1['distance']= df1['distance'].str.replace('unit:', '')

        df1['cityName'] = df1['cityName'].str.replace(cityCode, cityName)

        df1['amenities'] = df1['amenities'].str.replace(
            r'[^A-Za-z0-9,.:]+', '')
        df1['amenities'] = df1['amenities'].str.replace(',', ', ')
        df1 = df1.fillna(0)

        csv_buffer = StringIO()
        df1.to_csv(csv_buffer, index=False)

        outputFile = "amadeus" + "-" + cityName + \
            str(datetime.now().strftime('-%m-%d-%Y-%H-%M-%S')) + '.csv'
        KEY = "amadeus/US/" + outputFile

        response = client.put_object(
            Bucket=bucket, Key=KEY, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(
                f"Successful output S3 put_object response. Status - {status}")
        else:
            print(
                f"Unsuccessful output S3 put_object response. Status - {status}")
        outputData['output_file'] = outputFile

        # response = amadeus.reference_data.locations.get(
        #     keyword='LON',
        #     subType=Location.ANY
        # )
        # print(response.body) #=> The raw response, as a string
        # print(response.result) #=> The body parsed as JSON, if the result was parsablef
        # print(response.data) #=> The list of locations, extracted f--rom the JSON

    except KeyError as err:
        outputData['Response'] = 'KeyError given input column "%s" is not present in inputfile' % str(
            err)
        outputData['status'] = 400
    except TypeError as t:
        outputData['Response'] = 'Type Error - reason "%s"' % str(t)
        outputData['status'] = 401

    finally:
        return outputData
        # cursor.close()
        # conn.close()


@cross_origin(origins='*')
@app.route('/amadeus/hotelListByCity', methods=['GET', 'POST'])
def hotelListByCity():
    outputData = {}
    # conn = mysql.connect()
    # cursor = conn.cursor()
    try:
        _json = request.json
        cityCode = _json['cityCode']
        cityName = _json['cityName']
        countryCode = _json['countryCode']
        countryName = _json['countryName']
        # radius = _json['radius']

        amadeus = Client(
            # logger=logger
            # log_level='debug'
        )

        bucketName = "curateddata-glue"

        # response = amadeus.shopping.flight_offers_search.get(
        #     originLocationCode='MAD',
        #     destinationLocationCode='ATH',
        #     departureDate='2022-11-01',
        #     adults=1)

        response = amadeus.reference_data.locations.hotels.by_city.get(
            cityCode=cityCode)

        # response = amadeus.reference_data.locations.hotels.by_city.get(
        #     cityCode=cityCode, radius=100, radiusUnit="KM",
        #     amenities="SWIMMING_POOL,SPA,FITNESS_CENTER,AIR_CONDITIONING,RESTAURANT,PARKING,PETS_ALLOWED,AIRPORT_SHUTTLE,BUSINESS_CENTER,DISABLED_FACILITIES,WIFI,MEETING_ROOMS,NO_KID_ALLOWED,TENNIS,GOLF,KITCHEN,ANIMAL_WATCHING,BABY-SITTING,BEACH,CASINO,JACUZZI,SAUNA,SOLARIUM,MASSAGE,VALET_PARKING,BAR%20or%20LOUNGE,KIDS_WELCOME,NO_PORN_FILMS,MINIBAR,TELEVISION,WI-FI_IN_ROOM,ROOM_SERVICE,GUARDED_PARKG,SERV_SPEC_MENU",
        #     ratings="1,2,3,4,5", hotelSource="ALL")

        client = boto3.client('s3', region_name="us-east-1",
                              aws_access_key_id=AWS_ACCESS_KEY_ID,
                              aws_secret_access_key=AWS_SECRET_ACCESS_KEY_ID)

        df2 = pd.DataFrame(response.data)

        bucket = bucketName
        outputFile = "amadeus" + '.csv'
        csv_buffer = StringIO()
        df2.to_csv(csv_buffer, index=False)

        response = client.put_object(Bucket=bucket, Key=outputFile, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful output S3 put_object response. Status - {status}")
        else:
            print(f"Unsuccessful output S3 put_object response. Status - {status}")

        # Reading the input dataset
        response = client.get_object(Bucket=bucket, Key="amadeus.csv")
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df = pd.read_csv(response.get("Body"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")

        df1 = df.rename(columns={'iataCode': 'cityName', 'name': 'hotelName', 'address': 'country'})

        df1['geoCode'] = df1['geoCode'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        df1['geoCode'] = df1['geoCode'].str.replace('latitude:', 'latitude: ')
        df1['geoCode'] = df1['geoCode'].str.replace('longitude:', 'longitude: ')

        df1['country'] = df1['country'].str.replace('countryCode:', '')
        df1['country'] = df1['country'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        df1['country'] = df1['country'].str.replace(countryCode, countryName)
        # df1['country']= df1['country'].str.replace('IN', 'India')

        # df1['distance']= df1['distance'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        # df1['amenities']= df1['amenities'].str.replace(r'[^A-Za-z0-9,.:]+', '')

        df1['cityName'] = df1['cityName'].str.replace(cityCode, cityName)
        # df1['cityName']= df1['cityName'].str.replace('GOI', 'Goa')
        # df1['cityName']= df1['cityName'].str.replace('MAA', 'Chennai')
        # df1['cityName']= df1['cityName'].str.replace('BOM', 'Mumbai')
        # df1['cityName']= df1['cityName'].str.replace('DEL', 'Delhi')

        # df1['distance']= df1['distance'].str.replace('value:', '')
        # df1['distance']= df1['distance'].str.replace(',', ' ')
        # df1['distance']= df1['distance'].str.replace('unit:', '')
        df1['geoCode'] = df1['geoCode'].str.replace(',', ', ')
        # df1['amenities']= df1['amenities'].str.replace(',', ', ')
        df1 = df1.fillna(0)

        csv_buffer = StringIO()
        df1.to_csv(csv_buffer, index=False)

        outputFile = "amadeus" + "-" + cityName + \
            str(datetime.now().strftime('-%m-%d-%Y-%H-%M-%S')) + '.csv'
        KEY = "amadeus/US/" + outputFile
        # KEY = "amadeus/India/" + outputFile

        response = client.put_object(
            Bucket=bucket, Key=KEY, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(
                f"Successful output S3 put_object response. Status - {status}")
        else:
            print(
                f"Unsuccessful output S3 put_object response. Status - {status}")
        outputData['output_file'] = outputFile
        print(outputFile)
        # response = amadeus.reference_data.locations.get(
        #     keyword='LON',
        #     subType=Location.ANY
        # )
        # print(response.body) #=> The raw response, as a string
        # print(response.result) #=> The body parsed as JSON, if the result was parsablef
        # print(response.data) #=> The list of locations, extracted f--rom the JSON
        return outputData
    except KeyError as err:
        outputData['Response'] = 'KeyError given input column "%s" is not present in inputfile' % str(
            err)
        outputData['status'] = 400
        return outputData
    except TypeError as t:
        outputData['Response'] = 'Type Error - reason "%s"' % str(t)
        outputData['status'] = 401
        return outputData
    # finally:
    #     cursor.close()
    #     conn.close()
        # return outputData


@cross_origin(origins='*')
@app.route('/amadeus/amadeushoteloffers', methods=['GET', 'POST'])
def amadeushoteloffers():
    outputData = {}
    # conn = mysql.connect()
    # cursor = conn.cursor()
    try:
        _json = request.json
        hotelId = _json['hotelId']
        adults = _json['adults']
        # checkInDate = _json['checkInDate']
        # roomQuantity = _json['roomQuantity']
        # paymentPolicy = _json['paymentPolicy']
        # radius = _json['radius']

        """
        {
            "hotelId": "MCLONGHM",
            "adults": "1",
            "checkInDate": "2023-04-22",
            "roomQuantity": "1"
        }
        """

        amadeus = Client(
            # logger=logger
            # log_level='debug'
        )

        bucketName = "curateddata-glue"

        # Get list of available offers by hotel ids
        response = amadeus.shopping.hotel_offers_search.get(hotelIds=hotelId, adults=adults)
        # response = amadeus.shopping.hotel_offers_search.get(hotelIds=hotelId, adults=adults,
        #                         checkInDate=checkInDate, roomQuantity=roomQuantity, paymentPolicy="NONE", bestRateOnly="true")
        # https://test.api.amadeus.com/v3/shopping/hotel-offers?hotelIds=MCLONGHM&adults=1&checkInDate=2023-04-22&roomQuantity=1&paymentPolicy=NONE&bestRateOnly=true

        # Check conditions of a specific offer
        # amadeus.shopping.hotel_offer_search('').get()

        df2 = pd.DataFrame(response.data)

        bucket = bucketName
        outputFile = "amadeushoteloffers" + '.csv'
        KEY1 = "amadeushoteloffers/" + outputFile
        csv_buffer = StringIO()
        df2.to_csv(csv_buffer, index=False)

        response = client.put_object(
            Bucket=bucket, Key=KEY1, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(
                f"Successful output S3 put_object response. Status - {status}")
        else:
            print(
                f"Unsuccessful output S3 put_object response. Status - {status}")

        # Reading the input dataset
        response = client.get_object(Bucket=bucket, Key=KEY1)
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(f"Successful S3 get_object response. Status - {status}")
            df1 = pd.read_csv(response.get("Body"))
        else:
            print(f"Unsuccessful S3 get_object response. Status - {status}")

        # df1 = df.rename(columns={'iataCode': 'cityName',
        #                 'name': 'hotelName', 'address': 'country'})

        # df1['geoCode'] = df1['geoCode'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        # df1['geoCode'] = df1['geoCode'].str.replace('latitude:', 'latitude: ')
        # df1['geoCode'] = df1['geoCode'].str.replace('longitude:', 'longitude: ')

        # df1['country'] = df1['country'].str.replace('countryCode:', '')
        # df1['country'] = df1['country'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        # df1['country'] = df1['country'].str.replace(countryCode, countryName)
        # df1['country']= df1['country'].str.replace('US', 'United States')

        # df1['distance']= df1['distance'].str.replace(r'[^A-Za-z0-9,.:]+', '')
        # df1['amenities']= df1['amenities'].str.replace(r'[^A-Za-z0-9,.:]+', '')

        # df1['cityName'] = df1['cityName'].str.replace(cityCode, cityName)
        # # df1['cityName']= df1['cityName'].str.replace('GOI', 'Goa')

        # df1['distance']= df1['distance'].str.replace('value:', '')
        # df1['distance']= df1['distance'].str.replace(',', ' ')
        # df1['distance']= df1['distance'].str.replace('unit:', '')
        # df1['geoCode'] = df1['geoCode'].str.replace(',', ', ')
        # df1['amenities']= df1['amenities'].str.replace(',', ', ')
        df1 = df1.fillna(0)

        csv_buffer = StringIO()
        df1.to_csv(csv_buffer, index=False)

        outputFile = "amadeushoteloffers" + "-" + \
            str(datetime.now().strftime('-%m-%d-%Y-%H-%M-%S')) + '.csv'
        KEY = "amadeushoteloffers/" + outputFile
        # KEY = "amadeus/India/" + outputFile

        response = client.put_object(
            Bucket=bucket, Key=KEY, Body=csv_buffer.getvalue())
        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status == 200:
            print(
                f"Successful output S3 put_object response. Status - {status}")
        else:
            print(
                f"Unsuccessful output S3 put_object response. Status - {status}")
        outputData['output_file'] = outputFile
        print(outputFile)
        # print(response.body) #=> The raw response, as a string
        # print(response.result) #=> The body parsed as JSON, if the result was parsablef
        # print(response.data) #=> The list of locations, extracted f--rom the JSON
        return outputData
    except KeyError as err:
        outputData['Response'] = 'KeyError given input column "%s" is not present in inputfile' % str(
            err)
        outputData['status'] = 400
        return outputData
    except TypeError as t:
        outputData['Response'] = 'Type Error - reason "%s"' % str(t)
        outputData['status'] = 401
        return outputData
    # finally:
    #     cursor.close()
    #     conn.close()
    # return outputData


@cross_origin(origins='*')
@app.route('/amadeus/hotelbooking', methods=['GET', 'POST'])
def hotelbooking():
    outputData = {}
    try:
        # _json = request.json
        # hotelId = _json['hotelId']
        # adults = _json['adults']


        amadeus = Client(
            # logger=logger
            # log_level='debug'
        )

        offerId = "NRPQNQBOJM"
        guests = [
        {
        "name": {
          "title": "MR",
          "firstName": "BOB",
          "lastName": "SMITH"
        },
        "contact": {
          "phone": "+33679278416",
          "email": "bob.smith@email.com"
                }
            }
        ]
        payments = [
            {
        "method": "creditCard",
        "card": {
          "vendorCode": "VI",
          "cardNumber": "0000000000000000",
          "expiryDate": "2026-01"
                    }
                }
            ]

        hotel_booking = amadeus.booking.hotel_bookings.post(offerId, guests, payments)
        print(hotel_booking.data)

    except KeyError as err:
        outputData['Response'] = 'KeyError given input column "%s" is not present in inputfile' % str(
            err)
        outputData['status'] = 400
        return outputData
    except TypeError as t:
        outputData['Response'] = 'Type Error - reason "%s"' % str(t)
        outputData['status'] = 401
        return outputData


# Brilli server
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

# write the code for hello world
