# ----------------------------------------------------------------------------------------------------------------------
from bs4 import BeautifulSoup
from datetime import datetime

import csv
import datetime
import os
import time
import utility as ut

# ----------------------------------------------------------------------------------------------------------------------
class Singleton:
    """
    The Singleton class is a non-thread-safe helper class which can be used to easily implement singletons.
    The decorated class can define an `__init__` method which takes only the `self` argument.
    To get the singleton instance, simply use the `instance` method.

    Important notes:
    - This class should only be used as a decorator - and not as a meta class - to the class that should be a singleton.
    - The decorated class cannot be inherited from. Other than that, there are no restrictions that apply to the
      decorated class.
    - Trying to use `__call__` will result in a `TypeError` being raised.
    """

    def __init__(self, decorated):
        self._decorated = decorated

    def instance(self):
        """
        Returns the singleton instance. Upon its first call, it creates a new instance of the decorated class and
        calls its `__init__` method. On all subsequent calls, the already created instance will be returned.
        """

        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated()
            return self._instance

    def __call__(self):
        raise TypeError("Singletons have to be accessed through `instance()`.")

    def __instanceheck__(self, inst):
        return isinstance(inst, self._decorated)

# ----------------------------------------------------------------------------------------------------------------------
@Singleton
class CsvHandler:
    def __init__(self):
        self._filePath = None

    def filePath(self):
        """
        Returns the CSV file path.
        """
        return self._filePath

    def setFilePath(self, filePath):
        """
        Sets the CSV file path.
            filePath: The specified CSV file path.
        """
        ut.createFileIfNotExist(filePath=filePath, removeIfExists=True)
        self._filePath = filePath

    def writeToCsv(self, dictData):
        """
        Write the date in the CSV file previously specified by the file path.
            dictData: Dictionary which contains all the data.
        """

        try:
            with open(self._filePath, 'a') as fileHandler:
                writer = csv.DictWriter(fileHandler, dictData.keys(), delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                if fileHandler.tell() == 0:
                    writer.writeheader()
                self.convertStringValuesToFloat(dictData)
                fileHandler.write('"{0}","{1}","{2}","{3}","{4}","{5}","{6}",{7},{8},{9},{10},{11},{12},{13},{14},"{15}"\n'
                                  .format(dictData["product_name"], dictData["category"], dictData["image"],
                                          dictData["price"], dictData["product_note"], dictData["price_note"],
                                          dictData["feature"], dictData["calorific_value_in_kJ"],
                                          dictData["calorific_value_in_kcal"], dictData["fat_in_g"],
                                          dictData["hereof_saturated_fatty_acids_in_g"],
                                          dictData["carbohydrates_in_g"], dictData["hereof_sugar_in_g"],
                                          dictData["protein_in_g"], dictData["salt_in_g"], dictData["timestamp"]))
                #writer.writerow(dictData)
                return True
        except:
            return False

    def convertStringValuesToFloat(self, dictData):
        for key, value in dictData.items():
            if isinstance(value, str):
                val = value.replace(',', '.').replace('g', '').replace('k', '').replace("'", "").replace('"', '')\
                    .replace('>', '').replace('<', '').replace('=', '').replace('_', '')
                val = "".join(val.split())
                if self.isFloat(val):
                    dictData[key] = float(val)
                else:
                    dictData[key] = value.replace("'", "").replace('"', '')

    def isFloat(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False

#--------------------------------------------------------------------------------------------------------------------
def getTimestamp():
    """
    Returns the current timestamp.
    """
    return datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d %H:%M:%S")

#--------------------------------------------------------------------------------------------------------------------
def getAbsolutePath(absoluteFilePath, relativeFilePath):
    """
    Returns the absolute path of the specified relative file path.
        absoluteFilePath: Absolute file path used for reference.
        relativeFilePath: Relative file path for which the absolute path should be returned.
    """
    try:
        currentPath = os.getcwd()
        os.chdir(os.path.dirname(absoluteFilePath))
        os.chdir(os.path.dirname(relativeFilePath))
        absolutePath = os.path.join(os.getcwd(), os.path.basename(relativeFilePath))
        os.chdir(currentPath)
        return absolutePath
    except:
        return relativeFilePath

# ----------------------------------------------------------------------------------------------------------------------
def getAllProductInfo(filePath):
    categories = ""
    priceNote = ""
    productNote = ""
    image = ""
    bList = []
    feature = ""

    with open(filePath, 'r') as fileHandler:
        contents = fileHandler.read()
        soup = BeautifulSoup(contents, "lxml")
        isProductPage = soup.find("div", class_="col-sm-6 detail-description")

        if isProductPage:
            listTitle = soup.find("div", class_="col-sm-6 detail-description").h1.text
            productName = listTitle.strip().replace("\n", '')
            category = soup.find("div", class_="breadcrumb")

            liList = category.find_all('li')
            for li in liList:
                bList.append(li.a.text)
            categories = '|'.join(map(str, bList))

            if soup.find("img", class_="img-responsive jq-img-zoom"):
                image = soup.find("img", class_="img-responsive jq-img-zoom")["src"]
                image = getAbsolutePath(filePath, image)

            price = soup.find("div", class_="price").text.strip().replace("\n", '')
            if soup.find("p", class_="price-note"):
                priceNote = soup.find("p", class_="price-note").text.strip().replace("\n", '')

            if soup.find("p", class_="product-note"):
                productNote = soup.find("p", class_="product-note").text
                productNote = productNote.strip().replace("\n", '')

            characteristics = soup.find("ul", class_="characteristics clearfix")
            if characteristics:
                feature = characteristics.text.strip().replace("\n", '')

            nutrientTable = soup.find("table", class_="table-striped")
            if nutrientTable:
                nutrientInfo = getTableData(nutrientTable)
            else:
                nutrientInfo = ["" for i in range(8)]

            if len(nutrientInfo) < 8:
                nutrientInfo = ["" for i in range(8)]
            timestamp = getTimestamp()

            dictData = {"product_name": productName, "category": categories, "image": image, "price": price,
                        "product_note": productNote, "price_note": priceNote, "feature": feature,
                        "calorific_value_in_kJ": nutrientInfo[0], "calorific_value_in_kcal": nutrientInfo[1],
                        "fat_in_g": nutrientInfo[2], "hereof_saturated_fatty_acids_in_g": nutrientInfo[3],
                        "carbohydrates_in_g": nutrientInfo[4], "hereof_sugar_in_g": nutrientInfo[5],
                        "protein_in_g": nutrientInfo[6], "salt_in_g": nutrientInfo[7], "timestamp": timestamp}

            print("Success" if CsvHandler.instance().writeToCsv(dictData) else "Failed")
            return
    print("Done")

# ----------------------------------------------------------------------------------------------------------------------
def getTableData(nutrientTable):
    """
    Currently not used.
    """

    tHeaders = []
    i = 0
    for th in nutrientTable.find_all("th"):
        tHeaders.append(th.text.replace('\n', ' ').strip())
    tableData = []

    # Find all tr's from table's tbody.
    for tr in nutrientTable.find_all("tr"):
        tRow = {}
        for td, th in zip(tr.find_all("td"), tHeaders):
            tableData.append(td.text)
    return tableData

# ----------------------------------------------------------------------------------------------------------------------
def checkIfProductAlreadyPresentInFile(file, searchString):
    """
    Currently not used.
    """

    with open(file, "rb") as fileHandler:
        reader = csv.DictReader(fileHandler)
        for row in reader:
            if row[0] == searchString:
                return False
            else:
                return True

# ----------------------------------------------------------------------------------------------------------------------
