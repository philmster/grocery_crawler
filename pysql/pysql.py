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
import datetime
import math
import logging
# ----------------------------------------------------------------------------------------------------------------------
logging.disable(logging.CRITICAL)
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
# ======================================================================================================================
# SQLBase
# ======================================================================================================================
class SQLBase:
    # ==================================================================================================================
    # Initializer
    # ==================================================================================================================
    def __init__(self, connection, dbName, dataTableNames):

        # SQL Database object.
        self._connection = connection

        # SQL Database name.
        self._dbName = dbName

        # Refers to (a) a single name of one data table or (b) a list of names of data tables
        # Note : One SQL statement can include more than one data table (which is why plural is used).
        self._dataTableNames = dataTableNames

        # List of statements whose execution succeeded.
        self._executedStatements = []

        # List of statements whose execution failed.
        self._failedStatements = []

        # Cursor object which has to be defined by using member method setCursor().
        # Note: The MySQLCursor class instantiates objects that can execute operations such as SQL statements.
        # Cursor objects interact with the MySQL server using a MySQLConnection object.
        self._cursor = None

    # ==================================================================================================================
    # Get methods
    # ==================================================================================================================
    def connection(self):
        return self._connection

    # ------------------------------------------------------------------------------------------------------------------
    def dbName(self):
        return self._dbName

    # ------------------------------------------------------------------------------------------------------------------
    def dataTableNames(self):
        return self._dataTableNames

    # ------------------------------------------------------------------------------------------------------------------
    def executedStatements(self):
        return self._executedStatements

    # ------------------------------------------------------------------------------------------------------------------
    def failedStatements(self):
        return self._failedStatements

    # ==================================================================================================================
    # Set methods
    # ==================================================================================================================
    def setConnection(self, connection):
        self._connection = connection

    # ------------------------------------------------------------------------------------------------------------------
    def setDbName(self, dbName):
        self._dbName = dbName

    # ------------------------------------------------------------------------------------------------------------------
    def setDataTableNames(self, dataTableNames):
        self._dataTableNames = dataTableNames

    # ------------------------------------------------------------------------------------------------------------------
    def setCursor(self):
        """Set a cursor object which is necessary to execute SQL statements."""
        self._cursor = self._connection.cursor()

    # ==================================================================================================================
    # Methods
    # ==================================================================================================================
    def executeStatement(self, statement, notifier=True):
        """
        Execute one single SQL statement.
        """
        try:
            self._cursor.execute(statement)     # Execute SQL command statement.
            self._connection.commit()           # Commit changes.
            if notifier:
                print("SQL statement executed.")
            self._executedStatements.append(statement)

        except Exception as exception:
            if notifier == True:
                print("SQL statement not executed.")
            statement = statement + "\nERROR: " + str(exception)
            self._failedStatements.append(statement)
            self._connection.rollback()         # Call ROLLBACK command (revert changes) in case there is any error.

    # ------------------------------------------------------------------------------------------------------------------
    def executeStatements(self, statementList, notifier=True):
        """
        Execute a list of several SQL statements.
        :param statementList List of SQL statements.
        :param notifier Boolean value; true if notification will be printed.
        """
        for statement in statementList:

            try:
                self._cursor.execute(statement)     # Execute SQL command statement.
                self._connection.commit()           # Commit changes.
                if notifier:
                    print("SQL statement executed.\n")
                self._executedStatements.append(statement)

            except Exception as e:
                if notifier:
                    print("SQL statement not executed.\n")
                statement = statement + "\nError: " + str(e)
                self._failedStatements.append(statement)
                self._connection.rollback()         # Call ROLLBACK command (revert changes) in case there is any error.

# ======================================================================================================================
# SQLCreate
# ======================================================================================================================
class SQLCreate(SQLBase):
    # ==================================================================================================================
    # Initializer
    # ==================================================================================================================
    def __init__(self, connection, dbName, dataTableNames, colNames, dictColDataTypes, characterSet="utf8", storageEngine="InnoDB", primaryKey=None, ifNotExists=True):

        # Inherit from base class SQLBase.
        super().__init__(connection, dbName, dataTableNames)

        # List of column names. Should match all column names in dictColDataTypes. Use inferColDataTypes() accordingly.
        self._colNames = colNames

        # Dictionary of column as keys and according data types as values; one data type value is a list of two elements: the actual data type and further information, e.g. on nullablity
        self._dictColDataTypes = dictColDataTypes

        # Name of character set.
        self._characterSet = characterSet

        # Name of storage engine.
        self._storageEngine = storageEngine

        # Name of primary key.
        self._primaryKey = primaryKey

        # Indicates that the SQL statement will only be executed if no corresponding data table exists.
        self._ifNotExists = ifNotExists

    # ==================================================================================================================
    # Get methods
    # ==================================================================================================================
    def colNames(self):
        return self._colNames

    # ------------------------------------------------------------------------------------------------------------------
    def dictDataTypes(self):
        return self._dictDataTypes

    # ------------------------------------------------------------------------------------------------------------------
    def characterSet(self):
        return self._characterSet

    # ------------------------------------------------------------------------------------------------------------------
    def storageEngine(self):
        return self._storageEngine

    # ------------------------------------------------------------------------------------------------------------------
    def primaryKey(self):
        return self._primaryKey

    # ==================================================================================================================
    # Set methods
    # ==================================================================================================================
    def set_colNames(self, colNames):
        self._colNames = colNames

    # ------------------------------------------------------------------------------------------------------------------
    def setDictDataTypes(self, dictDataTypes):
        self._dictDataTypes = dictDataTypes

    # ------------------------------------------------------------------------------------------------------------------
    def setCharacterSet(self, characterSet):
        self._characterSet = characterSet

    # ------------------------------------------------------------------------------------------------------------------
    def setStorageEngine(self, storageEngine):
        self._storageEngine = storageEngine

    # ------------------------------------------------------------------------------------------------------------------
    def setPrimaryKey(self, primaryKey):
        self._primaryKey = primaryKey

    # ==================================================================================================================
    # Methods
    # ==================================================================================================================
    def buildStatement(self, addPrimaryId=False):
        """
        Build a CREATE statement which can then be executed by SQLBase method 'executeStatements()'.
        Note: A cursor object which is defined for a PyMySQL database object can only execute one single instruction.
        For example, the combination of the SQL instructions 'USE' and 'CREATE' within one single statement is not
        possible and causes an error.
        """

        # Not more than one data table name should be provided since one CREATE statement only creates one specific table.
        if isinstance(self._dataTableNames, list):
            print("ERROR: More than one table has been queried for creation. Please provide only one table to 'dataTableNames'.")
            return ""

        sIfNotExists = ""
        if self._ifNotExists:
            sIfNotExists = "IF NOT EXISTS"

        # SQL instruction 'CREATE'.
        createTableStatement = "CREATE TABLE %s %s.%s (" % (sIfNotExists, self._dbName, self._dataTableNames) + "\n"

        # If addPrimaryId is set to true, add automatically incrementing id column as primary key.
        primaryIdStatement = ""
        if addPrimaryId:
            primaryIdStatement = "id BIGINT NOT NULL PRIMARY KEY AUTO_INCREMENT,\n"

        # Columns and their data types.
        colNamesStatement = ''
        for colName in self._colNames:
            if colName in self._dictColDataTypes:
                colNamesStatement = colNamesStatement + "     `%s`" % (colName) + ' ' + self._dictColDataTypes[colName] + ",\n"
            else:
                colNamesStatement = colNamesStatement + "     `%s`" % (colName) + ' ' + "TEXT" + ",\n"

        # Primary key.
        if self._primaryKey is not None:
            primaryKeyStatement = "     PRIMARY KEY (`%s`)" % (self._primaryKey) + "\n"
        else:
            colNamesStatement = colNamesStatement[:(len(colNamesStatement) - len(",\n"))]  # Remove term ",\n" from the end of part 2, otherwise the SQL statements will fail.
            primaryKeyStatement = ""

        # Storage engine and character set.
        engineCharSetStatement = ")\n ENGINE=%s DEFAULT CHARSET=%s;" % (self._storageEngine, self._characterSet)

        return createTableStatement + primaryIdStatement + colNamesStatement + primaryKeyStatement + engineCharSetStatement

# ======================================================================================================================
# SQLInsert
# ======================================================================================================================
class SQLInsert(SQLBase):
    # ==================================================================================================================
    # Initializer
    # ==================================================================================================================
    def __init__(self, connection, dbName, dataTableNames, colNames, values=None):

        # Inherit from base class SQLBase.
        super().__init__(connection, dbName, dataTableNames)

        # List of column names.
        self._colNames = colNames

        # Row of values to be added to the data table (technically the row is a list).
        # It makes sense to define 'values' as optional argument, since instantiated SQLInsert objects are often
        # used in for loops.
        self._values = values

        self._lstJsonArrayIndices = []

    # ==================================================================================================================
    # Get methods
    # ==================================================================================================================
    def colNames(self):
        return self._colNames

    # ------------------------------------------------------------------------------------------------------------------
    def values(self):
        return self._values

    # ------------------------------------------------------------------------------------------------------------------
    def lstJsonArrayIndices(self):
        return self._lstJsonArrayIndices

    # ==================================================================================================================
    # Set methods
    # ==================================================================================================================
    def setColNames(self, colNames):
        self._colNames = colNames

    # ------------------------------------------------------------------------------------------------------------------
    def setValues(self, values):
        self._values = values

    # ------------------------------------------------------------------------------------------------------------------
    def setLstJsonArrayIndices(self, lstJsonArrayIndices):
        self._lstJsonArrayIndices = lstJsonArrayIndices

    # ==================================================================================================================
    # Methods
    # ==================================================================================================================
    def buildColNamesStatement(self):
        """
        :return: String specifying insertion of column names.
        """
        dbTableStatement = "INSERT INTO {0}.{1} (".format(self._dbName, self._dataTableNames)  # same explanation regarding variable 'dataTableNames' like at method 'build_statement()' for subclass 'CREATE'

        colNamesListStatement = ''
        for i in range(len(self._colNames) - 1):
            colNamesListStatement = colNamesListStatement + "`" + self._colNames[i] + "`, "
        colNamesListStatement = colNamesListStatement + "`" + self._colNames[len(self._colNames) - 1] + "`)\n"

        return dbTableStatement + colNamesListStatement

    def raw(self, sText):
        """Returns a raw string representation of the text"""

        dictEscapeChars = {'\'': r'"'}
        sTextNew = ''
        for char in sText:
            try:
                sTextNew += dictEscapeChars[char]
            except KeyError:
                sTextNew += char
        return sTextNew

    # ------------------------------------------------------------------------------------------------------------------
    def buildValuesStatement(self):
        """

        :return: String specifying insertion of values.
        """
        valuesStatement = "VALUES ("

        # Since local variable 'statement' - see buildStatement() - is a string, if clauses are necessary to convert
        # all data types into strings (int, date, etc.).
        lengthValues = len(self._values) - 1
        for i in range(len(self._values)):

            terminator = ", "
            if i == lengthValues:
                terminator = ");"

            if self._values[i] is None:
                valuesStatement = valuesStatement + "NULL" + terminator

            else:

                # If the value shall be represented as a JSON array, add required wrapper
                if i in self._lstJsonArrayIndices:
                    valuesStatement = valuesStatement + self.raw("JSON_ARRAY({0})".format(self._values[i])) + terminator

                # Check if value is a 'datetime.date' object.
                elif type(self._values[i]) is datetime.date:
                    string = self._values[i].strftime('%Y-%m-%d')
                    valuesStatement = valuesStatement + "\"" + string + "\"" + terminator

                else:
                    # A try and except clause is necessary since a TypeError is raised if a string is checked on
                    # NaN and also a ValueError is raised if a string is tried to be converted into an integer
                    # (see 'type(int(values[i]))').
                    try:
                        # The order of if clauses is important. Check first if value is NaN.
                        if math.isnan(self._values[i]):
                            valuesStatement = valuesStatement + "NULL" + terminator
                            continue
                        #  Check secondly if data type of value is a number.
                        if type(int(self._values[i])) is int:
                            valuesStatement = valuesStatement + str(self._values[i]) + terminator
                            continue

                    except (TypeError, ValueError):
                        if type(self._values[i]) is str:
                            valuesStatement = valuesStatement + "\"" + self._values[i] + "\"" + terminator
                            continue

                    # Check if value is a 'datetime.datetime' object.
                    if type(self._values[i]) is datetime.datetime:
                        string = self._values[i].strftime('%Y-%m-%d %H:%M:%S')
                        valuesStatement = valuesStatement + "\"" + string + "\"" + terminator
                        continue

        return valuesStatement

    # ------------------------------------------------------------------------------------------------------------------
    def buildStatement(self):
        """
        Build an SQL statement which can then be executed by SQLBase method 'executeStatement()'.
        Note: A cursor object (which is defined for a PyMySQL database object) can only execute one single instruction.
        For example, the combination of the SQL instructions 'USE...;' and 'CREATE...;' within one single statement
        is not possible and causes an error.
        """

        # First statement part: Build 'INSERT INTO...' segment with column names.
        colNamesStatement = self.buildColNamesStatement()

        # Second statement part: Build Row of values to be added to the data table.
        valuesStatement = self.buildValuesStatement()

        # Return both statement parts combined as final SQL statement.
        return colNamesStatement + valuesStatement

# ======================================================================================================================
# SQLSelect
# ======================================================================================================================
class SQLSelect(SQLBase):
    # ==================================================================================================================
    # Initialization
    # ==================================================================================================================
    def __init__(self, connection, dbName, dataTableNames, colNames="*", where="True", join=""):

        # Inherit from base class SQLBase.
        super().__init__(connection, dbName, dataTableNames)

        # Optional list of column names.
        self._colNames = colNames

        # Optional 'WHERE' clause (if no parameter is passed, where tests the boolean value 'True' by default ).
        self._where = where

        # optional 'JOIN' condition (if no parameter is passed, join is just an empty string).
        self._join = join


    # ==================================================================================================================
    # Get methods
    # ==================================================================================================================
    def colNames(self):
        return self._colNames

    # ------------------------------------------------------------------------------------------------------------------
    def where(self):
        return self._where

    # ------------------------------------------------------------------------------------------------------------------
    def join(self):
        return self._join

    # ==================================================================================================================
    # Set methods
    # ==================================================================================================================
    def setColNames(self, colNames):
        self._colNames = colNames

    # ------------------------------------------------------------------------------------------------------------------
    def setWhere(self, where):
        self._where = where

    # ------------------------------------------------------------------------------------------------------------------
    def setJoin(self, join):
        self._join = join

    # ==================================================================================================================
    # Methods
    # ==================================================================================================================

    def buildStatement(self):
        """
        Build an SQL statement which can then be executed by SQLBase method 'executeStatement()'.
        """

        # If self._dataTableNames only includes the name of one data table (i.e. is not a list).
        if not isinstance(self._dataTableNames, list):

            # First statement part: 'SELECT'.
            if self._colNames == "*":
                selectStatement = "SELECT *\n"
            else:
                selectStatement = "SELECT "
                for i in range(len(self._colNames) - 1):
                    selectStatement = selectStatement + self._colNames[i] + ", "
                selectStatement = selectStatement + self._colNames[len(self._colNames) - 1] + "\n"

            # Second statement part: 'FROM'.
            fromStatement = "FROM %s.%s\n" % (self._dbName, self._dataTableNames)

            # Third statement part: 'WHERE'.
            whereStatement = "WHERE " + self._where

            # Return all statement parts as final SQL statement.
            return selectStatement + fromStatement + whereStatement

        # Otherwise, self._dataTableNames includes more than one data table name.
        else:
            # This part should handle join statements. Method 'build_statement()' can only handle one data table right
            # now. Insert the according code.
            return ""

# ======================================================================================================================
# SQLLoad
# ======================================================================================================================
class SQLLoad(SQLBase):
    # ==================================================================================================================
    # Initialization
    # ==================================================================================================================
    def __init__(self, connection, dbName, dataTableNames, pathSQL, fieldsTerminatedBy=",", enclosedBy="\"", linesTerminatedBy="\\n", ignore=1, replace=False):

        # Inherit from base class SQLBase.
        super().__init__(connection, dbName, dataTableNames)

        # Path where the file is stored.
        # Note: It is important to differentiate between how Python accepts path notation and how SQL accepts it.
        self._pathSQL = pathSQL

        # Optional argument that states how the fields are separated.
        self._fieldsTerminatedBy = fieldsTerminatedBy

        # Optional argument that states how expressions are enclosed.
        self._enclosedBy = enclosedBy

        # Optional argument that states how the lines are separated.
        self._linesTerminatedBy = linesTerminatedBy

        # Optional argument to ignore a number of lines at the beginning of the file.
        # Note: In order to avoid importing headers as a record ignore the first line.
        self._ignore = ignore

        # Optional argument determining whether data should be added (default) or replaced.
        self._replace = replace

    # ==================================================================================================================
    # Get methods
    # ==================================================================================================================
    def pathSQL(self):
        return self._pathSQL

    # ------------------------------------------------------------------------------------------------------------------
    def fieldsTerminatedBy(self):
        return self._fieldsTerminatedBy

    # ------------------------------------------------------------------------------------------------------------------
    def enclosedBy(self):
        return self._enclosedBy

    # ------------------------------------------------------------------------------------------------------------------
    def linesTerminatedBy(self):
        return self._linesTerminatedBy

    # ------------------------------------------------------------------------------------------------------------------
    def ignore(self):
        return self._ignore

    # ------------------------------------------------------------------------------------------------------------------
    def replace(self):
        return self._replace

    # ==================================================================================================================
    # Set methods
    # ==================================================================================================================
    def setPathSQL(self, pathSQL):
        self._pathSQL = pathSQL

    # ------------------------------------------------------------------------------------------------------------------
    def setFieldsTerminatedBy(self, fieldsTerminatedBy):
        self._fieldsTerminatedBy = fieldsTerminatedBy

    # ------------------------------------------------------------------------------------------------------------------
    def setEnclosedBy(self, enclosedBy):
        self._endlosedBy = enclosedBy

    # ------------------------------------------------------------------------------------------------------------------
    def setLinesTerminatedBy(self, linesTerminatedBy):
        self._linesTerminatedBy = linesTerminatedBy

    # ------------------------------------------------------------------------------------------------------------------
    def setIgnore(self, ignore):
        self._ignore = ignore

    # ------------------------------------------------------------------------------------------------------------------
    def setReplace(self, replace):
        self._replace = replace

    # ==================================================================================================================
    # Methods
    # ==================================================================================================================
    def buildStatement(self):
        """
        Build an SQL statement which can then be executed by SQLBase method 'executeStatement()'.
        """

        # If data type of a given column is INTEGER or DECIMAL, this method should add for each of these columns:
        # 'SET `<column>` = nullif(`<column>`,'')'

        # First statement part: 'LOAD'.
        loadStatement = "LOAD DATA LOCAL INFILE '{0}'\n".format(self._pathSQL)

        ## Second statement part: 'INTO'.
        if self._replace:
            intoStatement = "REPLACE INTO TABLE {0}.{1}\n".format(self._dbName, self._dataTableNames)
        else:
            intoStatement = "INTO TABLE {0}.{1}\n".format(self._dbName, self._dataTableNames)

        ## Third statement part: 'FIELDS'.
        fieldsStatement = "FIELDS TERMINATED BY '{0}'\n".format(self._fieldsTerminatedBy)

        ## Fourth statement part: 'ENCLOSED'.
        enclosedStatement = "ENCLOSED BY '{0}'\n".format(self._enclosedBy)

        ## Fifth statement part: 'LINES'.
        linesStatement = "LINES TERMINATED BY '{0}'\n".format(self._linesTerminatedBy)

        ## Sixth statement part: 'IGNORE'.
        ignoreStatement = "IGNORE {0} LINES;".format(self._ignore)

        # Return all statement parts as final SQL statement.
        return loadStatement + intoStatement + fieldsStatement + enclosedStatement + linesStatement + ignoreStatement

# ======================================================================================================================
# SQLDrop
# ======================================================================================================================
class SQLDrop(SQLBase):
    # ==================================================================================================================
    # Initialization
    # ==================================================================================================================
    def __init__(self, connection, dbName, dataTableNames, ifExists=True):

        # Inherit from base class SQLBase.
        super().__init__(connection, dbName, dataTableNames)

        # Optional variable indicating that the SQL statement will be executed, even if no corresponding data table
        # exists. If this variable is set to "False" and there is no corresponding data table, the SQL statement will
        # not be executed.
        self._ifExists = ifExists

    # ==================================================================================================================
    # Get methods
    # ==================================================================================================================
    def ifExists(self):
        return self._ifExists

    # ==================================================================================================================
    # Set methods
    # ==================================================================================================================
    def setIfExists(self, ifExists):
        self._ifExists = ifExists

    # ==================================================================================================================
    # Methods
    # ==================================================================================================================
    def buildStatement(self):
        """
        Build an SQL statement which can then be executed by SQLBase method 'executeStatement()'.
        """

        if self._ifExists:
            return "DROP TABLE IF EXISTS {0}.{1};".format(self._dbName, self._dataTableNames)
        else:
            return "DROP TABLE {0}.{1};".format(self._dbName, self._dataTableNames)

## =====================================================================================================================
# SQLManualStatement
# ======================================================================================================================
class SQLManualStatement(SQLBase):

    # ==================================================================================================================
    # Initialization
    # ==================================================================================================================
    def __init__(self, connection, dbName, dataTableNames, manualStatement):

        # Inherit from base class SQLBase.
        super().__init__(connection, dbName, dataTableNames)

        # The manually specified SQL statement.
        self._manualStatement = manualStatement

    # ==================================================================================================================
    # Get methods
    # ==================================================================================================================
    def manualStatement(self):
        return self._manualStatement

    # ==================================================================================================================
    # Set methods
    # ==================================================================================================================
    def setManualStatement(self, manualStatement):
        self._manualStatement = manualStatement

    # ==================================================================================================================
    # Methods
    # ==================================================================================================================
    def buildStatement(self):
        """
        Build an SQL statement which can then be executed by SQLBase method 'execute_statement()'.
        The statement is simply the manually passed one.
        """
        return self._manualStatement

# ======================================================================================================================
