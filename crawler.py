# ----------------------------------------------------------------------------------------------------------------------
from bs4 import BeautifulSoup
import requests
import csv
import time
from datetime import datetime
import datetime

# ----------------------------------------------------------------------------------------------------------------------
# Write data to csv file.


def writeToCsv(listInput,filename):
    try:
        with open(filename, 'a') as fopen:
            w = csv.DictWriter(fopen, listInput.keys())
            if fopen.tell() == 0:
                w.writeheader()
            w.writerow(listInput)
    except:
        return False

# ----------------------------------------------------------------------------------------------------------------------


def getTableData(nutrientTable):
    t_headers = []
    i = 0
    for th in nutrientTable.find_all("th"):
        t_headers.append(th.text.replace('\n', ' ').strip())
    table_data = []
    # Find all tr's from table's tbody
    for tr in nutrientTable.find_all("tr"):
        t_row = {}
        for td, th in zip(tr.find_all("td"), t_headers):
            table_data.append(td.text)
    return table_data
# ----------------------------------------------------------------------------------------------------------------------


def checkIfProductAlreadyPresentInFile(file,searchString):
    with open(file, 'rb') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row[0] == searchString:
                return False
            else:
                return True
#--------------------------------------------------------------------------------------------------------------------
def get_timestamp():
    ts = time.time()
    st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    return st

# ----------------------------------------------------------------------------------------------------------------------
#url = "www.edeka24.de/3-Bears-Dreierlei-Beere-Porridge-400-g.html"
# "www.edeka24.de/Lebensmittel/Essig-Oel/Alnatura-Bio-Aceto-Balsamico-500ML.html"


def getAllProductInfo(url):
    print(url)
    file='productInfo'+datetime.datetime.now(). strftime("%Y_%m_%d")+ '.csv'
    with open(url, 'r') as f:
        contents = f.read()
        categories = priceNote= productNote=image=''
        b = []
        feature = "NIL"
        
        soup = BeautifulSoup(contents, 'lxml')
        checkIfItsProductPage= soup.find("div", class_="col-sm-6 detail-description")
        if checkIfItsProductPage:
            listTitle = soup.find("div", class_="col-sm-6 detail-description").h1.text
            productName = listTitle.strip()
            category = soup.find("div", class_="breadcrumb")
            list = category.find_all('li')
            for li in list:
                b.append(li.a.text)
            categories = '|'.join(map(str, b))
            if soup.find("img", class_="img-responsive jq-img-zoom"):
                image = soup.find("img", class_="img-responsive jq-img-zoom")["src"]
            price = soup.find("div", class_="price").text
            if soup.find("p", class_="price-note"):
                priceNote = soup.find("p", class_="price-note").text
            if soup.find("p", class_="product-note"):
                productNote = soup.find("p", class_="product-note").text
                productNote = productNote.strip()
            characteristics = soup.find("ul", class_="characteristics clearfix")
            if characteristics:
                feature = characteristics.text
            nutrientTable = soup.find("table", class_="table-striped")
            if nutrientTable:
                nutrientInfo = getTableData(nutrientTable)
            else:
                nutrientInfo = ["NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL"]
            print(nutrientInfo)
            if(len(nutrientInfo)<8):
                nutrientInfo = ["NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL"]
            timestamp = get_timestamp()
            finalData = {"Product Name": productName, "Category": categories, "Image": image, "Price": price,
                        "ProductNote":productNote,
                        "Price Note": priceNote, "Feature": feature, "Brennwert in kJ": nutrientInfo[0],
                        "Brennwert in kcal": nutrientInfo[1], "Fett in g": nutrientInfo[2],
                        "davon gesättigte Fettsauren in g": nutrientInfo[3], "Kohlenhydrate in g": nutrientInfo[4],
                        "davon Zucker in g": nutrientInfo[5], "Eiweiß in g": nutrientInfo[6], "Salz in g": nutrientInfo[7],
                        "Timestamp": timestamp}
            writeToCsv(finalData,file)
    print("Done")

# ----------------------------------------------------------------------------------------------------------------------
