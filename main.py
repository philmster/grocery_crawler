# ----------------------------------------------------------------------------------------------------------------------
from datetime import datetime

import crawler as crawler
import data_upload as du
import os
import pandas as pd
import utility as ut
import webpage_downloader as wd
import webpage_navigator as wn


# ----------------------------------------------------------------------------------------------------------------------
def main():
    """
    Web crawler main function.
    """

    url = "https://www.edeka24.de/"

    # Prepare download directory
    scriptPath = ut.identifyScriptPath()
    dirDownload = os.path.join(scriptPath, "data", "edeka24")
    #ut.createDirIfNotExist(dirDownload)

    # Set file path in the singleton class.
    outputDir = os.path.join(scriptPath, "data", "output")
    #ut.createDirIfNotExist(outputDir)
    filePath = os.path.join(outputDir, "product_info_{0}.csv".format(datetime.now().strftime("%Y_%m_%d")))
    #crawler.CsvHandler.instance().setFilePath(filePath)

    # Download entire webpage to local directory.
    #wd.downloadWebPage(url=url, dirDownload=dirDownload)

    # Run the crawler by traversing through the downloaded webpage.
    #wn.crawlThroughSubDirs(dirDownload)
    #exit()
    # Read in CSV as Pandas dataframe.
    ###dataFrame = pd.read_csv(filePath, sep=',', dtype={0: "string", 1: "string"})
    dataFrame = pd.read_csv(filePath, sep=',')
    #dataFrame = pd.read_csv(filePath, sep=',', dtype={0: "string", 1: "string", 2: "string", 3: "string", 4: "string",
    #                                                  5: "string", 6: "string", 7: "Int64", 8: "Int64", 9: "Int64",
    #                                                  10: "Int64", 11: "Int64", 12: "Int64", 13: "Int64", 14: "Int64",
    #                                                  15: "string"})
    print(dataFrame)

    # Upload data to MariaDB.
    dbName = "grocery"
    dataTableName = "edeka24"
    logDirPath = os.path.join(scriptPath, "data", "log", '')
    dictAdjustDataTypes = {"product_name": "TEXT",
                           "category": "TEXT",
                           "image": "TEXT",
                           "price": "DECIMAL(8,2)",
                           "product_note": "TEXT",
                           "price_note": "TEXT",
                           "feature": "TEXT",
                           "calorific_value_in_kJ": "DECIMAL(8,2)",
                           "calorific_value_in_kcal": "DECIMAL(8,2)",
                           "fat_in_g": "DECIMAL(8,2)",
                           "hereof_saturated_fatty_acids_in_g": "DECIMAL(8,2)",
                           "carbohydrates_in_g": "DECIMAL(8,2)",
                           "hereof_sugar_in_g": "DECIMAL(8,2)",
                           "protein_in_g": "DECIMAL(8,2)",
                           "salt_in_g": "DECIMAL(8,2)",
                           "timestamp": "TIMESTAMP",
                           "package_size":"TEXT",
                           "serving_size":"TEXT"}
    du.createTable(dataFrame, dbName, dataTableName, logDirPath, dropTable=False, dictAdjustDataTypes=dictAdjustDataTypes)

    # Upload data frame to data warehouse.
    du.uploadDataFrame(dataFrame, dbName, dataTableName, logDirPath=logDirPath)

    return 0

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    main()

# ----------------------------------------------------------------------------------------------------------------------
