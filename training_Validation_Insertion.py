
from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform
from logs_insertion_to_db.training_log_insertion_to_db import training_log_insertion_to_db
from datetime import datetime as dt

class train_validation:
    def __init__(self,client, resource):
        self.raw_data = Raw_Data_validation(client, resource)
        self.dataTransform = dataTransform(client,resource)
        self.dBOperation = dBOperation(client,resource)
        self.db_obj = training_log_insertion_to_db('TrainMainLog')

    def train_validation(self):
        try:

            # extracting values from prediction schema
            data_db = {'objective': 'rawdata', 'message': "Start of Validation on files", 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Start of Getting values From Schema",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            data_db = {'objective': 'rawdata', 'message': "Got values From Schema",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Start of definining regex to validate filename",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            regex = self.raw_data.manualRegexCreation()

            data_db = {'objective': 'rawdata', 'message': "Regex Defined to validate filename",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # validating filename of prediction files
            data_db = {'objective': 'rawdata', 'message': "Start of validating Raw Data",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Start of validating filename",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)

            data_db = {'objective': 'rawdata', 'message': "Filename Validated",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # validating column length in the file
            data_db = {'objective': 'rawdata', 'message': "Start of validating ColumnLength",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.raw_data.validateColumnLength(noofcolumns)

            data_db = {'objective': 'rawdata', 'message': "ColumnLength Validated",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # validating if any column has all values missing
            data_db = {'objective': 'rawdata', 'message': "Validating Missing Values In whole Column",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.raw_data.validateMissingValuesInWholeColumn()
            data_db = {'objective': 'rawdata', 'message': "Missing Values Validated",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Raw Data Validation Completed",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Starting Data Transforamtion",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # replacing blanks in the csv file with "Null" values to insert in table
            data_db = {'objective': 'rawdata', 'message': "Replacing Missing values With Null",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.dataTransform.replaceMissingWithNull()

            data_db = {'objective': 'rawdata', 'message': "Missing values Replaced",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "DataTransformation Completed",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # create database with given name, if present open the connection! Create table with columns given in schema
            data_db = {'objective': 'rawdata', 'message': "Creating Training database Table",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            table = self.dBOperation.createTableDb("InputData")
            data_db = {'objective': 'rawdata', 'message': "Training data table Created",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #self.log_writer.log(self.file_object, "Table creation Completed!!")
            #self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            data_db = {'objective': 'rawdata', 'message': "Insertion of Data into Table started",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.dBOperation.insertIntoTableGoodData(table)

            data_db = {'objective': 'rawdata', 'message': "Insertion of Training Data in Table completed",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            # self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # # Delete the good data folder after loading files in table
            # self.raw_data.deleteExistingGoodDataTrainingFolder()
            # self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            # self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # # Move the bad files to archive folder
            # self.raw_data.moveBadFilesToArchiveBad()
            # self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            # self.log_writer.log(self.file_object, "Validation Operation completed!!")
            # self.log_writer.log(self.file_object, "Extracting csv file from table")
            # # export data in table to csvfile
            # self.dBOperation.selectingDatafromtableintocsv('Training')
            # self.file_object.close()

        except Exception as e:
            raise e









