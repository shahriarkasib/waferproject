from datetime import datetime
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from logs_insertion_to_db.training_log_insertion_to_db import training_log_insertion_to_db
from datetime import datetime as dt
from application_logging import logger

class pred_validation:
    def __init__(self, client, resource):
        self.client = client
        self.resource = resource
        self.raw_data = Prediction_Data_validation(self.client, self.resource)
        self.dataTransform = dataTransformPredict(self.client,self.resource)
        self.dBOperation = dBOperation(self.client, self.resource)
        self.db_obj = training_log_insertion_to_db("PredictionMainLog")

    def prediction_validation(self):

        try:

            data_db = {'objective': 'rawdata', 'message': "Start of Validation on files for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,noofcolumns = self.raw_data.valuesFromSchema()

            data_db = {'objective': 'rawdata', 'message': "Got values From Schema for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #getting the regex defined to validate filename
            data_db = {'objective': 'rawdata', 'message': "Start of definining regex to validate filename for prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            regex = self.raw_data.manualRegexCreation()

            data_db = {'objective': 'rawdata', 'message': "Regex Defined to validate filename for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #validating filename of prediction files
            data_db = {'objective': 'rawdata', 'message': "Start of validating filename for prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)

            data_db = {'objective': 'rawdata', 'message': "Filename Validated",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #validating column length in the file
            data_db = {'objective': 'rawdata', 'message': "Start of validating Columnlength for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.raw_data.validateColumnLength(noofcolumns)

            data_db = {'objective': 'rawdata', 'message': "ColumnLength Validated for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #validating if any column has all values missing
            data_db = {'objective': 'rawdata', 'message': "Start of validating Missing Values In whole Column for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.raw_data.validateMissingValuesInWholeColumn()

            data_db = {'objective': 'rawdata', 'message': "Missing Values Validated for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Raw data Validation Complete for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Starting Data Transformation for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Start of Replacing Missing With Null for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #replacing blanks in the csv file with "Null" values to insert in table
            self.dataTransform.replaceMissingWithNull()

            data_db = {'objective': 'rawdata', 'message': "Missing Values Replaced with Null for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Data Transformation Completed for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #create database with given name, if present open the connection! Create table with columns given in schema
            data_db = {'objective': 'rawdata', 'message': "Creating Prediction database Table",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            table = self.dBOperation.createTableDb('PredictionData')

            data_db = {'objective': 'rawdata', 'message': "Table Creation Completed for prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Insertion of data into Table Started for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #insert csv files in the table
            self.dBOperation.insertIntoTableGoodData(table)
            data_db = {'objective': 'rawdata', 'message': "Insertion of data into Table Completed for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

        except Exception as e:
            raise e









