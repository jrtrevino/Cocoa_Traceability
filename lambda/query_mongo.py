import json
import re
from pymongo import MongoClient

# Below are the strings required to connect to MongoDB.
# MongoDB is hosted on an EC2 instance. Be aware that the
# Hostname address of the EC2 may change if rebooted/stopped/etc.
ip = 'ec2-54-212-157-127.us-west-2.compute.amazonaws.com'
port = '27017'
connection_string = f'mongodb://{ip}:{port}/'


def lambda_handler(event, context):
    print(f"Printing event: {event}")
     # gather our queryStringParameters required for MongoDB querying.
    shape = event.get('queryStringParameters') and  event['queryStringParameters'].get('shape')
    center =  event.get('queryStringParameters') and event['queryStringParameters'].get('center')
    radius =  event.get('queryStringParameters') and event['queryStringParameters'].get('radius')
    bottom_left =  event.get('queryStringParameters') and event['queryStringParameters'].get('bottomLeft')
    top_right =  event.get('queryStringParameters') and event['queryStringParameters'].get('topRight')

    try:
        client = MongoClient(connection_string)
        db = client['geospatial'] if shape else client['farms']
        # print(db.geospatial.find_one())
    except Exception as e:
        print(e)
        return generate_response(500, "Could not connect to MongoDB.")

   
    # query MongoDB according to shape and coordinate points.
    # rectangle queries require two point pairs: bottom left and top right.
    # These two pairs designate the corners of the rectangle.
    # Spherical/Circular queries only require a center point and radius.
    # All coordinate pairs are in the format (lat,long)

    if not shape:
        # shape was not provided, return an error.
        # we can change default behavior later.
        return generate_response(400, 'Please provide a shape for geospatial queries.')

    elif shape == 'rectangle' and (not bottom_left or not top_right):
        # rectangle shape was provided, but required coordinate points were not.
        return generate_response(400, 'Please provide a bottom-left and top-right coordinate point pair for rectangular queries.')

    elif shape == 'circle' and (not radius or not center):
        # circle shape provided but no center point and/or radial distance.
        return generate_response(400, 'Please provide a center coordinate point pair and a radial distance.')

    elif shape != 'rectangle' and shape != 'circle':
        # the off chance the user does not provided a supported shape
        return generate_response(400, f'Sorry, shapes provided must be a circle or a rectangle. You provided: {shape}')

    # By now, we should have all of the required information to query MongoDB.
    response = query_mongo(db, event['queryStringParameters'])
    return response


"""
Queries MongoDB using a geospatial query. This could either be a rectangle or a sphere.
Notice that a provided coordinate point pair must contain a delimiter symbol ',' to parse the pair.
    Example: -70,80 would represent the coordinate point (-70 latitude, 80 longitude). Failure for the request
    to provide this results in a 400 response.

query_info is a dictionary containing a few of the following keys:
    shape: dictates the shape to query with.
    bottom_left & top_right: dictates the pairs representing the edges of a rectangle.
    radius: the radius of the circle.
    center: the center of the circle.

Returns: a JSON object containing documents that are within the provided shape's boundaries.
         This may also return an error (status code 400) if the coordinate points are not provided correctly.
"""


def query_mongo(db, query_info):
    shape = query_info.get('shape')
    if shape == 'rectangle':
        print("Building query for a rectangle.")
        bottom_left = query_info.get('bottomLeft').split(',')
        top_right = query_info.get('topRight').split(',')
        if len(bottom_left) < 2 or len(top_right) < 2:
            return generate_response(400, 'Please input coordinate points correctly with delimiter: `,`')
        query = {"coordinates": {"$within": {
            "$box": [[float(bottom_left[0]), float(bottom_left[1])], [float(top_right[0]), float(top_right[1])]]}}}
        print(f"Constructed query: {query}")
        query_response = db.geospatial.find(query)
        
    elif shape == 'circle':
        return generate_response(500, "Sorry, circular queries are unavailable at the moment.")
    
    return parse_response(query_response)


def parse_response(mongo_cursor):
    body = []
    for doc in mongo_cursor:
        body.append(doc)
    return generate_response(200, body)

def generate_response(status_code, body):
    return {
        "isBase64Encoded": False,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'statusCode': status_code,
        'body': json.dumps(body, default=str)
    }

