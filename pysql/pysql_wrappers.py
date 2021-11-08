# -*- coding: utf-8 -*-

# ======================================================================================================================
# Script information
# ======================================================================================================================
# Class definitions for handling mySQL requests.
# (C) 2020 Philipp Meister
# Released under GNU Public License (GPL)
# email p.meister@dil-ev.de

# ======================================================================================================================
# Import modules
# ======================================================================================================================
import math
import decimal
import datetime as dt
from pykeepass import PyKeePass
import pymysql
import os
import pandas as pd
import logging
# import time

import pysql as sql

# ----------------------------------------------------------------------------------------------------------------------
logging.disable(logging.CRITICAL)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# ======================================================================================================================
# Establish a server connection
# ======================================================================================================================
def establishServerConnection(hostIpAddress="172.16.128.196",
                              dbName="corona",
                              username=None,
                              password=None,
                              keepassDB="Q:\\FoodData\\new\\01_technical\\Password Database\\DIL.kdbx",
                              keepassKey="Q:\\FoodData\\new\\01_technical\\Password Database\\2018-03-09_Schlüssel.key",
                              keepassTitle="MariaDB c.seebold"):

    if username == None and password == None:
        # Provide MariaDB account information by accessing the locally stored KeePass file.
        keePass = PyKeePass(keepassDB, keyfile=keepassKey)
        entry = keePass.find_entries(title=keepassTitle, first=True) # Find entry 'MariaDB c.seebold'.
        username = entry.username
        password = entry.password
                        
    # Try and except clause checks if connection was successful.
    try:
        connection = pymysql.connect(hostIpAddress, username, password, dbName, local_infile=True)  # Requires to add 'loose-local-infile = 1' in the '[client]' section of '/etc(/mysql)/my.cnf'.
        print("Connection to database {0} successfully established.".format(dbName))
        return connection
    except:
        print("ERROR: Connection to database {0} failed.".format(dbName))
        import sys
        sys.exit()    
    
# ======================================================================================================================
# Truncate tables
# ======================================================================================================================
def truncateTables(connection, dbName, dataTableNames_list):

    failed = 0

    for dataTableNames in dataTableNames_list:
        
        try:
            manualStatement = "TRUNCATE TABLE {0}.{1}".format(dbName, dataTableNames)

            sqlManual = sql.SQLManualStatement(connection, dbName, dataTableNames, manualStatement)
        
            # Define a cursor object which is necessary to execute SQL statements.
            sqlManual.setCursor()
            
            # Build the required SQL query which is exactly the manually typed one in variable 'manualStatement'.
            statement = sqlManual.buildStatement()
            
            sqlManual.executeStatement(statement, False)
            
        except:
            failed += 1
    
    if failed > 0:
        print("ERROR: TRUNCATION incomplete")

# ======================================================================================================================
# Load tables
# ======================================================================================================================
def loadTables(connection, dbName, lstDataTableNames, lstPathSQL):

    for i in range(len(lstDataTableNames)):
        
        dataTableNames = lstDataTableNames[i]
        pathSQL = lstPathSQL[i]

        loadSQL = sql.SQLLoad(connection, dbName, dataTableNames, pathSQL, fieldsTerminatedBy=",", enclosedBy="\"", linesTerminatedBy="\\n", ignore=1, replace=False)
        
        # Define a cursor object which is necessary to execute SQL statements.
        loadSQL.setCursor()
        
        # Build the required SQL query.
        statement = loadSQL.buildStatement()

        loadSQL.executeStatement(statement, True)

        for failedStatement in loadSQL.failedStatements():
            print(failedStatement)

# ======================================================================================================================
# Infer column data types
# ======================================================================================================================
def isValidDate(string, lstDateFormats=None):

    # Specify acceptable date formats to be identified in string.
    if lstDateFormats is None:
        lstDateFormats = ["%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%y-%m-%d", "%Y/%m/%d", "%y/%m/%d", "%d.%m.%Y", "%d.%m.%y"]
        # lstDateFormats = ["%Y", "%b %d, %Y", "%b %d, %Y", "%B %d, %Y", "%B %d %Y", "%m/%d/%Y", "%m/%d/%y", "%b %Y",
        #                   "%B%Y", "%b %d,%Y", "%Y-%m-%d", "%d.%m.%Y"]

    lstParsedDates = []
    bIsValid = False
    for frmt in lstDateFormats:
        try:
            date = dt.datetime.strptime(string, frmt)   # Creates a datetime object from a given string.
            bIsValid = True
            break
        except:
            pass

    return bIsValid

# ----------------------------------------------------------------------------------------------------------------------
def inferColumnDataTypes(dataFrame, dictColDataTypes=None, useText=False, useBigInt=False, roundDecimal=None, safetyFactorVarChar=0, safetyFactorDecimalInt=0, safetyFactorDecimalFloat=0, safetyFactorInt=0):
    """
    This function anticipates the SQL data types of the attributes of a passed pandas data frame.
    To be sure that data types are long enough, a safety factor 'sf' can be added, for example 'sfv' for VARCHAR types.
    Note: If a column of the CSV file has no entries, 'read_csv()' does not create an according column in the
    data frame and ignores that column. Needs to be handled.
    """

    # Dictionary with column names as keys and their corresponding data types as values.
    if dictColDataTypes is None:
        dictColDataTypes = {}

    # Add the column names as keys to the dictionary.
    for column in dataFrame:
        print(column)
        dictColDataTypes[column] = ""
        
    # Check which data type is appropriate for a given column.
    for column in dataFrame:
        
        # Auxiliary variables to determine the length of values within columns.
        maxLenString = 0        # Maximum length of a given string.
        maxLenDecimalInt = 0    # Maximum length of whole-numbered part of a decimal value.
        maxLenDecimalFloat = 0  # Maximum number of decimal places of a decimal value.
        #minInt = 0           # Maximum length of an integer.
        maxInt = 0           # Minimum length of an integer.

        j = 0
        for index, row in dataFrame.iterrows():

            # Check if data type is None; and if so just continue to next iteration.
            if row[column] is None:
                continue

            # Check if column includes strings; and if so define data type value as
            # "VARCHAR(<maximum string length + safety factor>)".
            if isinstance(row[column], str):
                if len(row[column]) > maxLenString:
                    maxLenString = len(row[column])
                if isValidDate(row[column]):
                    dictColDataTypes[column] = "DATE"
                elif useText:
                    dictColDataTypes[column] = "TEXT"
                else:
                    dictColDataTypes[column] = "VARCHAR(%s)" % (maxLenString + safetyFactorVarChar)
                continue

            # If a numeric field is empty, the term 'int(row[column])' raises a ValueError
            # stating "cannot convert float NaN to integer"; just continue.
            if math.isnan(row[column]):
                continue
            
            # Check if column includes decimal values.
            number = row[column]
            if roundDecimal is not None:
                number = round(number, roundDecimal)
            lenDecimalInt = len(str(int(number)))                                       # Length of whole-numbered part of value.
            lenDecimalFloat = decimal.Decimal(str(number)).as_tuple().exponent          # lenDecimalFloat amounts to the number of decimal places (for some reason you have to pass string values as arguments; that is why row[column] is parsed).
            lenDecimalFloat *= (-1)                                                     # lenDecimalFloat is negative, therefore multiply it with (-1).

            if lenDecimalFloat > 0:                                                     # If lenDecimalFloat is bigger than 0, value 'row[column]' has decimal places.
                if lenDecimalInt > maxLenDecimalInt:
                    maxLenDecimalInt = lenDecimalInt
                    #print(column + ": " + str(row[column]) + " = " + str(lenDecimalInt) + " -> " + str(maxLenDecimalInt))
                if lenDecimalFloat > maxLenDecimalFloat:
                    maxLenDecimalFloat = lenDecimalFloat
                    #print(column + ": " + str(row[column]) + " = " + str(lenDecimalFloat) + " -> " + str(maxLenDecimalFloat))
                # Assign the SQL data type.
                # Note: If 'DECIMAL(M,D)' were the given data type, 'M' is the maximum number of digits
                # and 'D' is the number of digits to the right of the decimal point.
                # Therefore, in our case 'M' is the sum of 'maxLenDecimalInt' and 'maxLenDecimalFloat'.
                dictColDataTypes[column] = "DECIMAL(%s,%s)" % ((maxLenDecimalInt + maxLenDecimalFloat + safetyFactorDecimalInt), (maxLenDecimalFloat + safetyFactorDecimalFloat))
                continue

            # Check if column includes integer values.
            if lenDecimalFloat == 0:                                        # If lenDecimalFloat is 0, value 'row[column]' has no decimal places.

                if useBigInt:
                    dictColDataTypes[column] = "BIGINT"
                    continue

                intRange = (abs(int(row[column])) + safetyFactorInt) * 2
                if intRange > maxInt:
                    maxInt = intRange

                if maxInt < 2**8:     # TINYINT = 1 Byte = 8 Bit
                    dictColDataTypes[column] = "TINYINT"
                    continue
                if maxInt < 2**16:    # SMALLINT = 2 Byte = 16 Bit
                    dictColDataTypes[column] = "SMALLINT"
                    continue
                if maxInt < 2**24:    # MEDIUMINT = 3 Byte = 24 Bit
                    dictColDataTypes[column] = "MEDIUMINT"
                    continue
                if maxInt < 2**32:    # INT = 4 Byte = 32 Bit
                    dictColDataTypes[column] = "INT"
                    continue
                if maxInt < 2**64:    # BIGINT = 5 Byte = 64 Bit
                    dictColDataTypes[column] = "BIGINT"
                    continue
                else:
                    dictColDataTypes[column] = "TEXT"
                    continue

    return dictColDataTypes

# ======================================================================================================================
# Infer column data types of BigFiles
# ======================================================================================================================
def inferColumnDataTypesOfBigFiles(path, filename, chunksize=1000, iterator=True, header=0, encoding='utf-8', sep=';', na_values=None, stop=0, sfv=0, sfw=0, sfd=0, sfi=0):
    """
    This function works analogous to 'anticipate_dataTypes()', but it is meant to be used for bigger files that cause a memory error when opened and loaded into a dataframe at once.
    Thus, the path and filename are passed as arguments instead of the entire data frame compared to 'anticipate_dataTypes()'.

    Note 1: If a column of the CSV file has no entries, 'read_csv()' does not create an according column in the
    data frame and ignores that column. Has to be handled.
    Note 2: This method would work even more memory usage friendly, if the data types of the columns were already
    defined during the function 'read_csv()'.
    """

    # Get string of current working directory.
    if na_values is None:
        na_values = [""]
    cwd = os.getcwd()

    # Change directory to  where the files are in.
    os.chdir(path)
    
    # Dictionary with the column names as keys and their corresponding data types as values.
    dictColDataTypes = {}
    
    # Auxiliary variables for the upcoming for loops.
    j = 0
    i = 0
    stopCounter = 0
    
    # Open the CSV file and cut it into chunks.
    for chunk in pd.read_csv(filename, chunksize=chunksize, iterator=iterator, header=header, encoding=encoding,  sep=sep, na_values=na_values):
        
        # If 'stopCounter' matches with passed value for argument 'stop', the for loop will break.
        if stop > 0:
            stopCounter = stopCounter + 1
        if stopCounter == stop and stop != 0:
            break
        
        # Check each column.
        for column in chunk:
            
            # Add the column names as keys to the dictionary (note that this is only necessary during the first loop).
            if j == 0:
                dictColDataTypes[column] = ["", []]      # First part is reserved for the SQL data type, second part is for the maximum value per column per each chunk.
                
            # Auxiliary variables to determine the length of values within columns.
            maxString = 0   # Maximum length of a given string.
            maxWhole = 0    # Maximum length of whole-numbered part of a decimal value.
            maxDec = 0      # Maximum number of decimal places of a decimal value.
            minInt = 0      # Maximum length of an integer
            maxInt = 0      # Minimum length of an integer
        
            # Go through each row of each chunk.
            for index, row in chunk.iterrows():
            
                # Skip the header (only needed during the first loop through the first chunk).
                if index == 0 and i == 0:
                    i = 1
                    continue
                
                # Check if data type is None; and if so just continue to next iteration.
                if row[column] is None:
                    continue
                
                # Check if column includes strings; and if so define data type value as
                # "VARCHAR(<maximum string length + safety factor>)".
                if isinstance(row[column], str):
                    if len(row[column]) > maxString:
                        maxString = len(row[column])
                        dictColDataTypes[column][0] = "VARCHAR"     # Add the SQL data type to the dictionary.
                    continue
    
                # If a numeric field is empty, the term 'int(row[column])' raises a ValueError stating "cannot convert float NaN to integer"; just continue.
                if math.isnan(row[column]):     # The term 'int(row[column])' raises a ValueError stating "cannot convert float NaN to integer".
                    continue
    
                # Check if column includes decimal values.
                wholePlaces = len(str(int(row[column])))                                # length of whole-numbered part of value.
                decimalPlaces = decimal.Decimal(str(row[column])).as_tuple().exponent   # decimalPlaces amounts to the number of decimal places (for some reason you have to pass string values as arguments; that is why row[column] is parsed).
                decimalPlaces = decimalPlaces * (-1)                                    # decimalPlaces is negative, therefore multiply it with '(-1)'.
                if decimalPlaces > 0:                                                   # Uf dd is bigger than 0, value 'row[column]' has decimal places.
                    if wholePlaces > maxWhole:
                        maxWhole = wholePlaces
                    if decimalPlaces > maxDec:
                        maxDec = decimalPlaces
                    # Assign the SQL data type.
                    dictColDataTypes[column][0] = "DECIMAL"
                    continue
    
                # Check if column includes integer values.
                if decimalPlaces == 0:                                                  # If dd is 0, value 'row[column]' has no decimal places.
                    if int(row[column]) < minInt:
                        minInt = int(row[column])
                    if int(row[column]) > maxInt:
                        maxInt = int(row[column])
                    dictColDataTypes[column][0] = "INTEGER"
                
            # After checking the column, add the maximum value to the respective list.
            if maxString > 0:
                dictColDataTypes[column][1].append(maxString)
            if maxWhole > 0 or maxDec > 0:
                dictColDataTypes[column][1].append((maxWhole, maxDec))                  # Add both values as tuple, since the belong together.
            if maxInt > 0 or minInt < 0:
                dictColDataTypes[column][1].append(maxInt - minInt)
            
        # Change auxiliary variable 'j'.
        j = 1

    # Use the dictionary 'dictColDataTypes' to build the final SQL data types.
    dictFinal = {}
    for key, value in dictColDataTypes.items():
        
        # First VARCHAR types.
        if dictColDataTypes[key][0] == "VARCHAR":
            dictFinal[key] = "VARCHAR(%s)" % (max(dictColDataTypes[key][1]) + sfv)
           
        # Secondly DECIMAL types.
        if dictColDataTypes[key][0] == "DECIMAL":
            maxW = 0
            maxD = 0
            # Go through the tuples and the find the maximum for both the whole-numbered and the decimal part.
            for i in range(len(dictColDataTypes[key][1])):
                if dictColDataTypes[key][1][i][0] > maxW:
                    maxW = dictColDataTypes[key][1][i][0]
                if dictColDataTypes[key][1][i][1] > maxD:
                    maxD = dictColDataTypes[key][1][i][1]
            dictFinal[key] = "DECIMAL(%s,%s)" % ((maxW + maxD + sfw),(maxD + sfd))
          
        # Thirdly INTEGER types.
        if dictColDataTypes[key][0] == "INTEGER":
            if (max(dictColDataTypes[key][1]) + sfi) < 255:
                dictFinal[key] = "TINYINT"
                continue
            if (max(dictColDataTypes[key][1]) + sfi) < 65535:
                dictFinal[key] = "SMALLINT"
                continue
            if (max(dictColDataTypes[key][1]) + sfi) < 16777215:
                dictFinal[key] = "MEDIUMINT"
                continue
            if (max(dictColDataTypes[key][1]) + sfi) < 4294967295:
                dictFinal[key] = "INT"
                continue
            if (max(dictColDataTypes[key][1]) + sfi) < (2**64-1):
                dictFinal[key] = "BIGINT"
                continue

    # Change back to original working directory.
    os.chdir(cwd)

    return dictFinal

# ======================================================================================================================
# Control structure for script as main program
# ======================================================================================================================
if __name__ == "__main__":

    # ------------------------------------------------------------------------------------------------------------------
    # Establish connection to MariaDB server
    # ------------------------------------------------------------------------------------------------------------------
    connection = establishServerConnection(hostIpAddress="172.16.129.30",
                                           dbName="Corona",
                                           keepassDB="Q:\\FoodData\\new\\01_technical\\Password Database\\DIL.kdbx",
                                           keepassKey="Q:\\FoodData\\new\\01_technical\\Password Database\\2018-03-09_Schlüssel.key",
                                           keepassTitle="MariaDB c.seebold")

    # ------------------------------------------------------------------------------------------------------------
    # TRUNCATE tables
    # ------------------------------------------------------------------------------------------------------------
    dbName = "Corona"
    lstDataTableNames = ["JohnsHopkinsDailyReports"]
    truncateTables(connection, dbName, lstDataTableNames)
        
    # ------------------------------------------------------------------------------------------------------------
    # Load CSV in SQL tables
    # ------------------------------------------------------------------------------------------------------------
    lstPathSQL = ["U:/Corona/JohnsHopkins/DailyReports/johnshopkins_dailyreports.csv"]
    loadTables(connection, dbName, lstDataTableNames, lstPathSQL)

    # waitingHours = 1
    # time.sleep(waitingHours * 3600) # 3600 seconds = 1 hour.
