import csv
import requests
import json

#write data to csv file
def write_to_csv(list_input,store):
    try:
        with open("locations_info_"+store+".csv", "a") as fopen: 
            w = csv.DictWriter(fopen, list_input.keys())
            if fopen.tell() == 0:
                w.writeheader()
            w.writerow(list_input)
    except:
        return False

def iterate_data(url):
    response = requests.request("GET", url)
    data  = json.loads(response.text)
    if data['local_results']:
        formated_data = format_data_and_insert(data['local_results'])
    if "next_link" in data['serpapi_pagination'].keys():
        while data['serpapi_pagination']['next_link']:
            print(data['serpapi_pagination']['next_link']+'&api_key=fe06990fa0ad413bdbcc206ac857bd2b8bf8c220d69f552d4729f9fc3dbbb5a4')
            iterate_data(data['serpapi_pagination']['next_link']+'&api_key=fe06990fa0ad413bdbcc206ac857bd2b8bf8c220d69f552d4729f9fc3dbbb5a4')
    else:
        return 'ALL done'

def format_data_and_insert(input):
    for locations in input:
        latitude = longitude =address = ''
        if locations['gps_coordinates']['latitude']:
            latitude= locations['gps_coordinates']['latitude']
        if locations['gps_coordinates']['longitude']:
            longitude = locations['gps_coordinates']['longitude']
        if "address" in locations.keys():
            if locations['address']:
                address = locations['address']
        location_array= {'Position':locations['position'],'Title':locations['title'],'PlaceId':locations['place_id'],'Address':address,'Latitude':latitude,'Longitude':longitude}
        write_to_csv(location_array,'Netto')
    return 'done_format_and_insert'

start_url = "https://serpapi.com/search?q=Netto&google_domain=google.de&location=germany&gl=de&engine=google&api_key=fe06990fa0ad413bdbcc206ac857bd2b8bf8c220d69f552d4729f9fc3dbbb5a4&num=100&start=0&tbm=lcl"
iterate_data(start_url)
