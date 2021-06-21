# -*- coding: utf-8 -*-

# ======================================================================================================================
# Import modules
# ======================================================================================================================
import datetime
import os
import pandas as pd
#from pykeepass import PyKeePass
#import pymysql
#import sys

# Custom modules
import pysql
# ======================================================================================================================
# Transform CSV files to pandas data frame
# ======================================================================================================================
def createTable(dataFrame, dbName, dataTableName, logDirPath=None, dropTable=False, dictAdjustDataTypes=None):
    # ------------------------------------------------------------------------------------------------------------------
    # Infer data types of data frame
    # ------------------------------------------------------------------------------------------------------------------
    if dictAdjustDataTypes is not None:
        dictColDataTypes = dictAdjustDataTypes
    else:
        dictColDataTypes = pysql.inferColumnDataTypes(dataFrame, useText=True,
                                                      useBigInt=False,
                                                      roundDecimal=3,
                                                      safetyFactorDecimalInt=3,
                                                      safetyFactorDecimalFloat=1)

    # ------------------------------------------------------------------------------------------------------------------
    # Establish server connection
    # ------------------------------------------------------------------------------------------------------------------

    hostIpAddress = ""	# insert ip address for mariadb here
    username = ""	# insert mariadb username here
    password = ""	# insert mariadb password here
    keepassDB = "",	# can be ignored
    keepassKey = "",	# can be ignored
    keepassTitle = ""	# can be ignored

    connection = pysql.establishServerConnection(hostIpAddress=hostIpAddress,
                                                 dbName=dbName,
                                                 username=username,
                                                 password=password,
                                                 keepassDB=keepassDB,
                                                 keepassKey=keepassKey,
                                                 keepassTitle=keepassTitle)

    if dropTable:
        sqlDrop = pysql.SQLDrop(connection, dbName, dataTableName, ifExists=True)
        sqlDrop.setCursor()
        statement = sqlDrop.buildStatement()
        sqlDrop.executeStatement(statement)

    lstColumnNames = dataFrame.columns.values.tolist()

    sqlCreate = pysql.SQLCreate(connection, dbName, dataTableName, lstColumnNames, dictColDataTypes, ifNotExists=True)
    sqlCreate.setCursor()
    statement = sqlCreate.buildStatement(addPrimaryId=False)
    sqlCreate.executeStatement(statement)

    # ------------------------------------------------------------------------------------------------------------------
    # Write failed statements to log
    # ------------------------------------------------------------------------------------------------------------------

    if logDirPath is not None:
        writeFailedStatementsToLog(logDirPath, sqlCreate, dataTableName, dbName)

    connection.close()
    print("Database connection successfully closed.\n")


# ======================================================================================================================
# Upload data frame
# ======================================================================================================================
def uploadDataFrame(dataFrame, dbName, dataTableName, logDirPath=None):
    """
    Uploads a data frame to a database in a data warehouse.
    """

    from dateutil import parser
    import csv

    # ------------------------------------------------------------------------------------------------------------------
    # Establish server connection
    # ------------------------------------------------------------------------------------------------------------------

    hostIpAddress = ""	# insert ip address for mariadb here
    username = ""	# insert mariadb username here
    password = ""	# insert mariadb password here
    keepassDB = "",	# can be ignored
    keepassKey = "",	# can be ignored
    keepassTitle = ""	# can be ignored

    connection = pysql.establishServerConnection(hostIpAddress=hostIpAddress,
                                                 dbName=dbName,
                                                 username=username,
                                                 password=password,
                                                 keepassDB=keepassDB,
                                                 keepassKey=keepassKey,
                                                 keepassTitle=keepassTitle)

    # ------------------------------------------------------------------------------------------------------------------
    # Execute SQL INSERT statement
    # ------------------------------------------------------------------------------------------------------------------

    lstColumnNames = dataFrame.columns.values.tolist()
    sqlInsert = pysql.SQLInsert(connection, dbName, dataTableName, lstColumnNames, values=None)

    # Define a cursor object which is necessary to execute SQL statements.
    sqlInsert.setCursor()

    print("Uploading data table " + dataTableName + " to database " + dbName + "...")

    #for dictRow in dataFrame.to_dict(orient="records"):
    for index, row in dataFrame.iterrows():
        dictRow = row.to_dict()
        values = list(dictRow.values())
        #values = [getattr(row, columnName) for columnName in lstColumnNames]
        sqlInsert.setValues(values)

        statement = sqlInsert.buildStatement()
        sqlInsert.executeStatement(statement, False)

    # ------------------------------------------------------------------------------------------------------------------
    # Write failed statements to log
    # ------------------------------------------------------------------------------------------------------------------

    if logDirPath is not None:
        writeFailedStatementsToLog(logDirPath, sqlInsert, dataTableName, dbName)

    connection.close()
    print("Database connection successfully closed.\n")

# ======================================================================================================================
# Write failed statements to log
# ======================================================================================================================
def writeFailedStatementsToLog(logDirPath, sqlInsert, dataTableName, dbName):

    failedStatements = sqlInsert.failedStatements()
    if len(failedStatements) == 0:
        print("No errors occurred.")
        return

    # Create name of log file.
    currentDate = formatDateToString()
    fileName = currentDate + "_" + "failed_statements.log"
    filePath = logDirPath + fileName

    # Open and save the file directly in the archive where its destination is anyhow.
    print("Writing failed statements into log file...")

    openingType = 'a'
    if not os.path.isfile(filePath):
        openingType = 'w'

    with open((logDirPath + fileName), openingType) as fileHandler:

        fileHandler.write("Number of failed statements: " + str(len(failedStatements)) + "\n")
        fileHandler.write("Data table:                  " + dataTableName + "\n")
        fileHandler.write("Database:                    " + dbName + "\n")

        for i in range(len(failedStatements)):
            fileHandler.write(failedStatements[i])
            fileHandler.write("\n\n")

# ======================================================================================================================
# Format date to string
# ======================================================================================================================
def formatDateToString():

    currentDate = datetime.datetime.today()

    # Example: Month = 7 + Day = 11 -> 2020-07-11
    if len(str(currentDate.month)) == 1 and len(str(currentDate.day)) == 2:
        return str(currentDate.year) + "-0" + str(currentDate.month) + "-" + str(currentDate.day)

    # Example: Month = 12 + Day = 5 -> 2020-12-05
    elif len(str(currentDate.month)) == 2 and len(str(currentDate.day)) == 1:
        return str(currentDate.year) + "-" + str(currentDate.month) + "-0" + str(currentDate.day)

    # Example: Month = 7 + Day = 5 -> 2020-07-05
    elif len(str(currentDate.month)) == 1 and len(str(currentDate.day)) == 1:
        return str(currentDate.year) + "-0" + str(currentDate.month) + "-0" + str(currentDate.day)

    # Example: Month = 12 + Day = 11 -> 2020-12-11
    else:
        return str(currentDate.year) + "-" + str(currentDate.month) + "-" + str(currentDate.day)
