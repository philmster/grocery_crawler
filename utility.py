# ----------------------------------------------------------------------------------------------------------------------
from pathlib import Path

import os
import shutil
import sys

# ----------------------------------------------------------------------------------------------------------------------
def identifyScriptPath():
    """
    Identifies the path of the current Python script.
    """
    return os.path.dirname(os.path.realpath(sys.argv[0]))

# ----------------------------------------------------------------------------------------------------------------------
def createDirIfNotExist(dirPath, removeIfExists=False):
    """
    Creates the specified directory if it does not exist already.
        dirPath: Path to the directory.
        removeIfExists: Boolean if the directory should first be removed if does exist already.
    """

    if removeIfExists and os.path.exists(dirPath):
        shutil.rmtree(dirPath)
    if not os.path.exists(dirPath):
        os.makedirs(dirPath)
        return

# ----------------------------------------------------------------------------------------------------------------------
def createFileIfNotExist(filePath, removeIfExists=False):
    """
    Creates the specified file if it does not exist already.
        dirPath: Path to the file.
        removeIfExists: Boolean if the file should first be removed if does exist already.
    """

    if removeIfExists and os.path.exists(filePath):
        os.remove(filePath)
    if not os.path.exists(filePath):
        pathOfFile = Path(filePath)
        pathOfFile.touch(exist_ok=True)
        return

# ----------------------------------------------------------------------------------------------------------------------
