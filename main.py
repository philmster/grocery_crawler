# ----------------------------------------------------------------------------------------------------------------------
from datetime import datetime

import crawler as crawler
import os
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
    ut.createDirIfNotExist(dirDownload)

    # Set file path in the singleton class.
    outputDir = os.path.join(scriptPath, "data", "output")
    ut.createDirIfNotExist(outputDir)
    filePath = os.path.join(outputDir, "product_info_{0}.csv".format(datetime.now().strftime("%Y_%m_%d")))
    crawler.CsvHandler.instance().setFilePath(filePath)

    # Download entire webpage to local directory.
    wd.downloadWebPage(url=url, dirDownload=dirDownload)

    # Run the crawler by traversing through the downloaded webpage.
    wn.crawlThroughSubDirs(dirDownload)

    # To do: Read in CSV as Pandas dataframe to upload it to MariaDB.

    return 0

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    main()

# ----------------------------------------------------------------------------------------------------------------------
