# ----------------------------------------------------------------------------------------------------------------------
from bs4 import BeautifulSoup
import requests
import csv

# ----------------------------------------------------------------------------------------------------------------------
# Write data to csv file.
def write_to_csv(list_input):
    try:
        with open("product_info.csv", 'a') as fopen:
            w = csv.DictWriter(fopen, list_input.keys())

            if fopen.tell() == 0:
                w.writeheader()
            w.writerow(list_input)
    except:
        return False

# ----------------------------------------------------------------------------------------------------------------------
def get_table_data(table):
    t_headers = []
    i = 0
    for th in nutrient_table.find_all("th"):
        t_headers.append(th.text.replace('\n', ' ').strip())
    table_data = []
    for tr in nutrient_table.find_all("tr"):    # Find all tr's from table's tbody
        t_row = {}
        for td, th in zip(tr.find_all("td"), t_headers):
            table_data.append(td.text)
    return table_data

# ----------------------------------------------------------------------------------------------------------------------
url = "www.edeka24.de/3-Bears-Dreierlei-Beere-Porridge-400-g.html"
# "www.edeka24.de/Lebensmittel/Essig-Oel/Alnatura-Bio-Aceto-Balsamico-500ML.html"

with open(url, 'r') as f:
    contents = f.read()
    soup = BeautifulSoup(contents, 'lxml')
    listTitle = soup.find("div", class_="col-sm-6 detail-description").h1.text
    category = soup.find("div", class_="breadcrumb")
    list = category.find_all('li')
    categories = ''
    b = []
    for li in list:
        b.append(li.a.text)
    categories = '/'.join(map(str, b))
    image = soup.find("img", class_="img-responsive jq-img-zoom")["src"]
    price = soup.find("div", class_="price").text
    price_note = soup.find("p", class_="price-note").text
    product_note = soup.find("p", class_="product-note").text
    characteristics = soup.find("ul", class_="characteristics clearfix")
    feature = "NIL"
    if characteristics:
        feature = characteristics.text
    nutrient_table = soup.find("table", class_="table-striped")
    nutrient_info = ["NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL", "NIL"]
    if nutrient_table:
        nutrient_info = get_table_data(nutrient_table)
        
    final_data = {"Product Name": listTitle.replace('\n', ''), "Category": categories, "Image": image, "Price": price,
                  "Price Note": price_note, "Feature": feature, "Brennwert in kJ": nutrient_info[0],
                  "Brennwert in kcal": nutrient_info[1], "Fett in g": nutrient_info[2],
                  "davon gesättigte Fettsauren in g": nutrient_info[3], "Kohlenhydrate in g": nutrient_info[4],
                  "davon Zucker in g": nutrient_info[5], "Eiweiß in g": nutrient_info[6], "Salz in g": nutrient_info[7]}
    write_to_csv(final_data)
    print("Done")

# ----------------------------------------------------------------------------------------------------------------------
