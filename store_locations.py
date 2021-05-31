# ----------------------------------------------------------------------------------------------------------------------
import csv
import json
import requests

# ----------------------------------------------------------------------------------------------------------------------
def writeToCsv(listInput, store):
    """
    Write data to csv file.
    """

    try:
        with open("locations_info_" + store + ".csv", 'a') as fileHandler:
            writer = csv.DictWriter(fileHandler, listInput.keys())
            if fileHandler.tell() == 0:
                writer.writeheader()
            writer.writerow(listInput)
    except:
        return False

# ----------------------------------------------------------------------------------------------------------------------
def iterateData(url):
    """
    To do: Specify function behaviour.
    """

    response = requests.request("GET", url)
    data = json.loads(response.text)
    if data["local_results"]:
        formattedData = formatDataAndInsert(data["local_results"])
    if "next_link" in data["serpapi_pagination"].keys():
        while data["serpapi_pagination"]["next_link"]:
            print(data["serpapi_pagination"]["next_link"] + "&api_key=fe06990fa0ad413bdbcc206ac857bd2b8bf8c220d69f552d4729f9fc3dbbb5a4")
            iterateData(data["serpapi_pagination"]["next_link"] + "&api_key=fe06990fa0ad413bdbcc206ac857bd2b8bf8c220d69f552d4729f9fc3dbbb5a4")
    else:
        return "ALL done"

# ----------------------------------------------------------------------------------------------------------------------
def formatDataAndInsert(inputLocations):
    """
    To do: Specify function behaviour.
    """

    for locations in inputLocations:
        latitude = ''
        longitude = ''
        address = ''
        if locations["gps_coordinates"]["latitude"]:
            latitude = locations["gps_coordinates"]["latitude"]
        if locations["gps_coordinates"]["longitude"]:
            longitude = locations["gps_coordinates"]["longitude"]
        if "address" in locations.keys():
            if locations["address"]:
                address = locations["address"]
        locationArray = {"Position": locations["position"], "Title": locations["title"],
                         "PlaceId": locations["place_id"], "Address": address, "Latitude": latitude,
                         "Longitude": longitude}
        writeToCsv(locationArray, "Netto")
    return "done_format_and_insert"

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    startUrl = "https://serpapi.com/search?q=Netto&google_domain=google.de&location=germany&gl=de&engine=google&api_key=fe06990fa0ad413bdbcc206ac857bd2b8bf8c220d69f552d4729f9fc3dbbb5a4&num=100&start=0&tbm=lcl"
    iterateData(startUrl)

# ----------------------------------------------------------------------------------------------------------------------
