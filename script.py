import mysql.connector
import config
import yelp_key
import requests
import json

# Establish connection to database
cnx = mysql.connector.connect(
    host = config.host,
    user = config.user,
    password = config.passwd)

c = cnx.cursor()

key = yelp_key.key

count = 0
#Business Search Endpoint
url = 'https://api.yelp.com/v3/businesses/search'

term = 'coffee'
location = "56 Bogart St, Brooklyn, NY 11206"
latitude = 40.710946
longitude = -73.951152
sort_by = 'distance'


headers = {
            'Authorization': 'Bearer {}'.format(key),
           }

url_params = {  'term': term.replace(' ', '+'),
                'location': location.replace(' ', '+'),
                'limit' : 50,
                'latitude' : 40.710946,
                'longitude' : -73.951152,
                'offset': count, 
                'radius' : 5500,
                'sort_by' : sort_by.replace(' ', '+')
              }

def business_search(url, headers, params):
    response = requests.get(url, headers = headers, params = url_params)
    business = json.loads(response.text)
    return business

def parse_business(business_json):
    parsed_businesses = []
    for biz in business_json['businesses']:
        try:
            tup = (biz['id'], biz['name'], biz['rating'],len(biz['price']),
                   ' '.join(biz['location']['display_address']),
                   biz['display_phone'], biz['review_count'],
                   biz['categories'][0]['title'])
            parsed_businesses.append(tup)
        except:
            tup = (biz['id'], biz['name'], biz['rating'], 0,
                       ' '.join(biz['location']['display_address']),
                     biz['display_phone'], biz['review_count'],
                       biz['categories'][0]['title'])
            parsed_businesses.append(tup)
            
    return parsed_businesses

def get_review_url(biz_data):
    review_url = []
    for i in range(len(biz_data)):
        review_url.append(f'https://api.yelp.com/v3/businesses/{biz_data[i][0]}/reviews')
    return review_url

def review_call(review_urls, headers):
    review_data = []
    for urls in review_urls:
        response = requests.get(urls, headers = headers)
        review = json.loads(response.text)
        review_data.append(review)
    return review_data

def get_business_ids(business_data):
    ids = []
    for i in range(len(business_data)):
        ids.append(business_data[i][0])
    return ids

def add_business_ids(business_ids, review_data):
    id_plus_reviews = []
    for i in range(len(review_data)):
        join = (biz_ids[i], review_data[i])
        id_plus_reviews.append(list(join))
        
    join_dict = list(map(lambda x: {'business_id': x[0], 'reviews' : x[1]['reviews']}, id_plus_reviews))
    return join_dict   

def parse_reviews(review_dict):
    parsed_reviews = [] 
    for x in review_dict:
        try:
            if len(x['reviews']) == 3:
                tup = (x['business_id'], x['reviews'][0]['id'], x['reviews'][0]['time_created'],
                        x['reviews'][0]['rating'],x['reviews'][0]['user']['id'],
                        x['reviews'][0]['user']['name'], x['reviews'][0]['text'], 

                        x['business_id'], x['reviews'][1]['id'], x['reviews'][1]['time_created'],
                        x['reviews'][1]['rating'], x['reviews'][1]['user']['id'],
                        x['reviews'][1]['user']['name'], x['reviews'][1]['text'],

                        x['business_id'], x['reviews'][2]['id'], x['reviews'][2]['time_created'],
                        x['reviews'][2]['rating'], x['reviews'][2]['user']['id'],
                        x['reviews'][2]['user']['name'], x['reviews'][2]['text'])
            elif len(x['reviews']) == 2:
                tup = (x['business_id'], x['reviews'][0]['id'], x['reviews'][0]['time_created'],
                        x['reviews'][0]['rating'],x['reviews'][0]['user']['id'],
                        x['reviews'][0]['user']['name'], x['reviews'][0]['text'], 

                        x['business_id'], x['reviews'][1]['id'], x['reviews'][1]['time_created'],
                        x['reviews'][1]['rating'], x['reviews'][1]['user']['id'],
                        x['reviews'][1]['user']['name'], x['reviews'][1]['text'])
            else:
                tup = (x['business_id'], x['reviews'][0]['id'], x['reviews'][0]['time_created'],
                        x['reviews'][0]['rating'],x['reviews'][0]['user']['id'],
                        x['reviews'][0]['user']['name'], x['reviews'][0]['text'])          
                      
            parsed_reviews.append(tup)
        except Exception as e:
            print(e)
    return parsed_reviews