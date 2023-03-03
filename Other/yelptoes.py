#Business Search      URL -- 'https://api.yelp.com/v3/businesses/search'
#Business Match       URL -- 'https://api.yelp.com/v3/businesses/matches'
#Phone Search         URL -- 'https://api.yelp.com/v3/businesses/search/phone'

#Business Details     URL -- 'https://api.yelp.com/v3/businesses/{id}'
#Business Reviews     URL -- 'https://api.yelp.com/v3/businesses/{id}/reviews'

#Businesses, Total, Region

# Import the modules
import requests
import json
import time
import calendar
import datetime
# Define a business ID
business_id = '4AErMBEoNzbk7Q8g45kKaQ'
unix_time = 1546047836
filename="/Users/sakshikulkarni/Desktop/final_nb.json"
# Define my API Key, My Endpoint, and My Header
API_KEY = 'ViYxOHxzCLRrO-zEzJIkak-LPmRl6Mw0uTu2EjcwHk_yKMgRZiX1U5yIu_QGR4TRGsZ8ma5hPjLwvDh_rdeD9-HQ1Nes7BCaQad5ACgGjCxYAQDsJaAYP9g9ENbjY3Yx'
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
HEADERS = {'Authorization': 'bearer %s' % API_KEY}
d={}
d['business']=[]
final_f=[]
with open(filename, "w") as file_object:
        file_object.write(json.dumps(""))

    
# Define my parameters of the search
# BUSINESS SEARCH PARAMETERS - EXAMPLE
cuisines=["mexican", "italian", "indian","thai", "chinese","american", "french","Mediterranean", "Greek"]
j=0
for c in cuisines:
  
    for i in range(50,1000, 50):
        print(i)
        
        PARAMETERS = {'term': c+' ' +'restaurants',
                    'limit': 50,
                    'offset': i,
                    'location': 'Manhatten'}

        response = requests.get(url = ENDPOINT,
                                params = PARAMETERS,
                                headers = HEADERS)

        # Conver the JSON String
        business_data = response.json()
        #print(business_data)
        data=business_data['businesses']
        for i in data:
        # print(i)
        
            j+=1
           
            temp = { "_index": "restaurants_list", "_id" :str(j)}

            a={ "index" : temp }
            #s=eval(a)
            b={'id': i['id'], 'category': c }
            with open(filename, "a") as file_object:
                file_object.write(json.dumps(a))
                file_object.write("\n")
                file_object.write(json.dumps(b))
                file_object.write("\n")
                file_object.close()
            j+=1

            
       


#Command for upload to ES            
# curl -XPOST -u 'DiningES:DiningES@123' 'https://search-restaurants-zggsreqybwzmor3yxdgzwv7tpy.us-east-1.es.amazonaws.com/_bulk' --data-binary @final_nb.json -H 'Content-Type: application/json'