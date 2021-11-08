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

    # Preparations
    scriptPath = ut.identifyScriptPath()

    # Download entire webpage to local directory.
    dirDownload = os.path.join(scriptPath, "data", "edeka24")
    ut.createDirIfNotExist(dirDownload)
    url = "https://www.edeka24.de/"
    wd.downloadWebPage(url=url, dirDownload=dirDownload)

    # Set file path in the singleton class.
    outputDir = os.path.join(scriptPath, "data", "output")
    ut.createDirIfNotExist(outputDir)
    fileName = "product_info_{0}.csv".format(datetime.now().strftime("%Y_%m_%d"))
    filePath = os.path.join(outputDir, fileName)
    ut.checkAndWipeMaxStoredCSVFilesIfNeeded(scriptPath, fileName, outputDir, iMaxCount=300)
    crawler.CsvHandler.instance().setFilePath(filePath)

    # Run the crawler by traversing through the downloaded webpage.
    wn.crawlThroughSubDirs(dirDownload)
    wn.printCrawlerStatistics()

    # Read in CSV as Pandas dataframe.
    dataFrame = pd.read_csv(filePath, sep=',')

    # Create the table in the database if not available yet
    dbName = "grocery"
    dataTableName = "edeka24"
    logDirPath = os.path.join(scriptPath, "data", "log", '')
    dictAdjustDataTypes = du.createDataTypeAdjustmentDict()
    du.createTable(dataFrame, dbName, dataTableName, logDirPath, dropTable=False, dictAdjustDataTypes=dictAdjustDataTypes)

    # Upload data frame to data warehouse.
    lstJsonArrayIndices = [dataFrame.columns.get_loc(key) for key, value in dictAdjustDataTypes.items() if value == "JSON"]
    du.uploadDataFrame(dataFrame, dbName, dataTableName, lstJsonArrayIndices, logDirPath=logDirPath)

    return 0

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    main()

# ----------------------------------------------------------------------------------------------------------------------
