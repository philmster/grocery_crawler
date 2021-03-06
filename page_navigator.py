# ----------------------------------------------------------------------------------------------------------------------
from os import scandir
from os.path import isfile

# ----------------------------------------------------------------------------------------------------------------------
def navigateThroughSubDirs(dirPath, setMemo=set()):
    """
    Navigates through all sub directories recursively and calls the web crawler for each html file found.
        dirPath: Path to the current directory (string).
        setMemo: Lookup table containing all processed directory and file paths (set).
    """

    if dirPath in setMemo:
        return
    setMemo.add(dirPath)

    for element in scandir(dirPath):
        if element.is_dir():
            navigateThroughSubDirs(element.path, setMemo)
        if isfile(element.path) and element.path.endswith(".html"):
            callWebCrawler(element.path, setMemo)

# ----------------------------------------------------------------------------------------------------------------------
def callWebCrawler(filePath, setMemo):
    """
    Calls the web crawler for the specified file path.
        filePath: Path to the current file (string).
        setMemo: Lookup table containing all processed directory and file paths (set).
    """
    if filePath in setMemo:
        return

    # TO DO: Add web crawler in this function.
    return

# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':

    dirMain = "C:\\Users\\Beyond\\Downloads\\recognisable-name\\www.edeka24.de"
    navigateThroughSubDirs(dirPath=dirMain)

# ----------------------------------------------------------------------------------------------------------------------
