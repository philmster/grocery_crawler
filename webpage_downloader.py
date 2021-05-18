# ----------------------------------------------------------------------------------------------------------------------
from pywebcopy import save_website
import os.path
from os import path
import webpage_navigator as wn
from datetime import datetime
import datetime
import shutil
from openpyxl import Workbook
import sys
import csv
# ----------------------------------------------------------------------------------------------------------------------
url = "https://www.edeka24.de/"
dirDownload = "/home/a.bhaita/food_data/"
beginTime = datetime.datetime.now()
kwargs = {"bypass_robots": True, "project_name": "recognisable-name", "load_css": False, "load_images": False,
          "load_javascript": False}
checkIfExist= str(path.exists(dirDownload+'recognisable-name/www.edeka24.de'))
#print('check if file exist '+ checkIfExist)
#if checkIfExist==True:
#    shutil.rmtree(dirDownload+'recognisable-name')
save_website(url=url, project_folder=dirDownload, **kwargs)
parseUrls = ["/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Backzutaten",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Fruehstueck",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Konserven",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Suess-und-Salzig",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Weitere-Artikel",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Beilagen",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Essig-Oel",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Gewuerze-Kraeuter-Bruehen",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Nudeln",
            "/home/a.bhaita/food_data/recognisable-name/www.edeka24.de/Lebensmittel/Sossen"
            ]
for urls in parseUrls:
    wn.navigateThroughSubDirs(urls)


data_initial = open('productInfo'+datetime.datetime.now(). strftime("%Y_%m_%d")+ '.csv', "r")
sys.getdefaultencoding()
workbook = Workbook()
worksheet = workbook.worksheets[0]
with data_initial as f:
    data = csv.reader((line.replace('\0','') for line in data_initial), delimiter=",")
    for r, row in enumerate(data):
        for c, col in enumerate(row):
            for idx, val in enumerate(col.split('/')):
                cell = worksheet.cell(row=r+1, column=c+1)
                cell.value = val
workbook.save('product'+datetime.datetime.now(). strftime("%Y_%m_%d")+'.xlsx')
endTime = datetime.datetime.now()
diff = endTime-beginTime

print('Total time taken by script is {}'.format(diff))
# ----------------------------------------------------------------------------------------------------------------------
