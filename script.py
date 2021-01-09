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
        
    join_dict = list(map(lambda x: {'business_id': x[0], 'reviews' : x[1]}, id_plus_reviews))
    return join_dict   

def parse_reviews(reviews_with_business_id):
    parsed_reviews = []
    
    for x in reviews_with_business_id:

        if 'error' in x['reviews'].keys():
            tup = (x['business_id'], x['reviews']['error']['code'])
            parsed_reviews.append(tup)
        else:
            try:
                for i in range(len(x['reviews'])):
                    tup = (x['business_id'], x['reviews']['reviews'][i]['id'],
                           x['reviews']['reviews'][i]['time_created'],
                           x['reviews']['reviews'][i]['rating'],
                           x['reviews']['reviews'][i]['user']['id'],
                           x['reviews']['reviews'][i]['user']['name'],
                           x['reviews']['reviews'][i]['text'])

                    parsed_reviews.append(tup)
                    i+1
            except Exception as e:
                print(e,': missing reviews')
    return parsed_reviews
            

def cafes_insert(c, cnx, cafe_details):
    
    c.executemany('''INSERT INTO coffee.cafes
                    (ID, NAME, RATING, PRICE, ADDRESS, PHONE, REVIEW_COUNT, CATEGORIES)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);''', cafe_details)
    cnx.commit()

def review_insert(c, cnx, reviews):
    
    c.executemany('''INSERT INTO coffee.reviews (CAFE_ID, REVIEW_ID, REVIEW_DATE, RATING, USER_ID, USER_NAME, REVIEW)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)''', reviews)
    cnx.commit()    


# c.execute('''USE coffee''')

c.execute('''CREATE TABLE cafes(

                ID VARCHAR(100) PRIMARY KEY,
                NAME VARCHAR(100),
                RATING FLOAT,
                PRICE INT,
                ADDRESS VARCHAR(160),
                PHONE VARCHAR(100),
                REVIEW_COUNT VARCHAR(100),
                CATEGORIES VARCHAR(200));'''
         )

c.execute(                     
            '''CREATE TABLE reviews(
            
                CAFE_ID VARCHAR(100),
                REVIEW_ID VARCHAR(100) PRIMARY KEY,
                REVIEW_DATE DATETIME,
                RATING FLOAT,
                USER_ID VARCHAR(100),
                USER_NAME VARCHAR(100),
                REVIEW VARCHAR(1000),
                CONSTRAINT FOREIGN KEY (CAFE_ID) REFERENCES cafes(ID)
                                    ); 
                
                                   '''
         )

count = 0 
#4800 cafes total. Yelp restricts business endpoint daily pull to 1000, throwing error if limit is reached
while count < 1000:
    url_params['offset'] = count
    business_data = business_search(url, headers = headers, params = url_params)
    parsed_businesses = parse_business(business_data)
    review_urls = get_review_url(parsed_businesses)
    review_raw = review_call(review_urls, headers)
    business_ids = get_business_ids(parsed_businesses)
    reviews_with_business_id = add_business_ids(business_ids, review_raw)
    parsed_reviews = parse_reviews(reviews_with_business_id)
    cafes_insert(c, cnx, parsed_businesses)
    review_insert(c, cnx, parsed_reviews)
    count += 50
    if count > 1000:
        break
