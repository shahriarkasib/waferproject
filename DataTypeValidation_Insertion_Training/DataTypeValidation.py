import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
import json
import pandas as pd
from datetime import datetime as dt
from logs_insertion_to_db.training_log_insertion_to_db import training_log_insertion_to_db


class dBOperation:
    """
      This class shall be used for handling all the SQL operations.

      Written By: iNeuron Intelligence
      Version: 1.0
      Revisions: None

      """
    def __init__(self, client, resource):

        self.client = client
        self.resource = resource
        self.db_obj = training_log_insertion_to_db('DBOperationLog')


    # def dataBaseConnection(self):
    #
    #     """
    #             Method Name: dataBaseConnection
    #             Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
    #             Output: Connection to the DB
    #             On Failure: Raise ConnectionError
    #
    #              Written By: iNeuron Intelligence
    #             Version: 1.0
    #             Revisions: None
    #
    #             """
    #     try:
    #         client_mongo = pymongo.MongoClient(
    #             "mongodb://localhost:27017/")
    #
    #         db = client_mongo.waferProject
    #         file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
    #         self.logger.log(file, "database created  successfully" )
    #         file.close()
    #     except ConnectionError:
    #         file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
    #         self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
    #         file.close()
    #         raise ConnectionError
    #     except Exception as e:
    #         file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
    #         self.logger.log(file, "Error while connecting to database: %s" % e)
    #         file.close()
    #
    #     return client_mongo,db

    def createTableDb(self,tablename):
        """
                        Method Name: createTableDb
                        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                        Output: None
                        On Failure: Raise Exception

                         Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None

                        """
        try:
            message = 'DataBase Input Table Created Succesfully'

            data_db = {'objective': 'CreateInputTable', 'status': 'ok', 'error': '',
                    'message': "Training data table Creation Started", 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            db_obj_input = training_log_insertion_to_db(tablename)
            message = 'DataBase Input Table Created Succesfully'

            data_db = {'objective': 'CreateInputTable', 'status':'ok', 'error': '',
                    'message': "Training data table Created" ,'file':'','time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            # self.logger.log(file, "Closed %s database successfully" % DatabaseName)
            # file.close()



        except Exception as e:
            message = str(e)
            data_db = {'objective': 'CreateInputTable', 'status': 'error', 'error': 'ExceptionError',
                    'message': message, 'file': '', 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e
        db_obj_input.table.drop()
        return db_obj_input.table

    def insertIntoTableGoodData(self,table):

        """
                               Method Name: insertIntoTableGoodData
                               Description: This method inserts the Good data files from the Good_Raw folder into the
                                            above created table.
                               Output: None
                               On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                               Version: 1.0
                               Revisions: None

        """

        bucket = self.resource.Bucket('goodrawdata')
        files = [obj.key for obj in bucket.objects.filter()]
        for file in files:
            obj = self.client.get_object(
                Bucket='goodrawdata',
                Key=file)
            data= pd.read_csv(obj['Body'])
            data.drop('Unnamed: 0',axis = 1, inplace = True)
            data.drop('Unnamed: 0.1', axis=1, inplace=True)
            try:
                out = json.loads(data.to_json(orient='records'))
                table.insert_many(out)
                message = 'value inserted succesfully'
                data_db = {'objective': 'insertIntoTableGoodData', 'status': 'ok', 'error': '',
                        'message': message, 'file':file, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                self.db_obj.insert_data(data_db)

            except Exception as e:

                data_db = {'objective': 'insertIntoTableGoodData', 'status': 'error', 'error': 'ExceptionError',
                        'message': str(e), 'file':file, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                self.db_obj.insert_data(data_db)
                raise e


    # def selectingDatafromtableintocsv(self,Database):
    #
    #     """
    #                            Method Name: selectingDatafromtableintocsv
    #                            Description: This method exports the data in GoodData table as a CSV file. in a given location.
    #                                         above created .
    #                            Output: None
    #                            On Failure: Raise Exception
    #
    #                             Written By: iNeuron Intelligence
    #                            Version: 1.0
    #                            Revisions: None
    #
    #     """
    #
    #     self.fileFromDb = 'Training_FileFromDB/'
    #     self.fileName = 'InputFile.csv'
    #     log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
    #     try:
    #         conn = self.dataBaseConnection(Database)
    #         sqlSelect = "SELECT *  FROM Good_Raw_Data"
    #         cursor = conn.cursor()
    #
    #         cursor.execute(sqlSelect)
    #
    #         results = cursor.fetchall()
    #         # Get the headers of the csv file
    #         headers = [i[0] for i in cursor.description]
    #
    #         #Make the CSV ouput directory
    #         if not os.path.isdir(self.fileFromDb):
    #             os.makedirs(self.fileFromDb)
    #
    #         # Open CSV file for writing.
    #         csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
    #
    #         # Add the headers and data to the CSV file.
    #         csvFile.writerow(headers)
    #         csvFile.writerows(results)
    #
    #         self.logger.log(log_file, "File exported successfully!!!")
    #         log_file.close()
    #
    #     except Exception as e:
    #         self.logger.log(log_file, "File exporting failed. Error : %s" %e)
    #         log_file.close()





