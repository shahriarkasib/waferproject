
import re
import pandas as pd
import json
from io import StringIO
from application_logging.logger import App_Logger
from logs_insertion_to_db.training_log_insertion_to_db import training_log_insertion_to_db
from datetime import datetime as dt





class Raw_Data_validation:

    """
             This class shall be used for handling all the validation done on the Raw Training Data!!.

             Written By: iNeuron Intelligence
             Version: 1.0
             Revisions: None

             """

    def __init__(self,client, resource):
        self.client = client
        self.resource = resource
        self.logger = App_Logger()
        self.db_obj = training_log_insertion_to_db('RawDataValidationLog')

    def valuesFromSchema(self):
        """
                        Method Name: valuesFromSchema
                        Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                        Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                        On Failure: Raise ValueError,KeyError,Exception

                         Written By: iNeuron Intelligence
                        Version: 1.0
                        Revisions: None

                                """
        try:
            bucket = self.resource.Bucket('wafertrainingschema')
            obj = self.client.get_object(
                Bucket='wafertrainingschema',
                Key='schema_training.json')
            dic = json.load(obj['Body'])
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']



            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            data_db = {'objective':'schema_validation', 'status': 'ok', 'error_type': '',
                    'file_name':'', 'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

        except ValueError:
            message = "Value not found inside schema_training.json"
            data_db = {'objective':'schema_validation','status': 'error', 'error_type':
                'ValueError', 'file_name': '', 'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise ValueError

        except KeyError:
            message = "incorrect key passed"
            data_db = {'objective': 'schema_validation', 'status': 'error', 'error_type': 'KeyError', 'file_name': '',
                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise KeyError

        except Exception as e:
            message = str(e)
            data_db = {'objective': 'schema_validation', 'status': 'error', 'error_type': 'ExceptionError', 'file_name': '',
                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """
                                Method Name: manualRegexCreation
                                Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                            This Regex is used to validate the filename of the training data.
                                Output: Regex pattern
                                On Failure: None

                                 Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                                        """
        regex = "['wafer']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                      Method Name: createDirectoryForGoodBadRawData
                                      Description: This method creates directories to store the Good Data and Bad Data
                                                    after validating the training data_db.

                                      Output: None
                                      On Failure: OSError

                                       Written By: iNeuron Intelligence
                                      Version: 1.0
                                      Revisions: None

                                              """

        try:
            if self.resource.Bucket('goodrawdata').creation_date is None :
                self.client.create_bucket(
                    Bucket='goodrawdata',
                    CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'}
                )

            else:
                pass
            if self.resource.Bucket('badrawdata').creation_date is None:
                self.client.create_bucket(
                    Bucket='badrawdata',
                    CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'}
                )

            else:
                pass

            message = "Directory for GoodBadrawData created Succesfully"
            data_db = {'objective': 'createDirectoryForGoodBadRawData', 'status': 'ok', 'error_type': '',
                    'file_name': '','message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
        except OSError as ex:
            message = str(ex)
            data_db = {'objective': 'createDirectoryForGoodBadRawData', 'status': 'error', 'error_type': 'OSError', 'file_name': '',
                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise OSError

    # def deleteExistingGoodDataTrainingFolder(self):
    #
    #     """
    #                                         Method Name: deleteExistingGoodDataTrainingFolder
    #                                         Description: This method deletes the directory made  to store the Good Data
    #                                                       after loading the data in the table. Once the good files are
    #                                                       loaded in the DB,deleting the directory ensures space optimization.
    #                                         Output: None
    #                                         On Failure: OSError
    #
    #                                          Written By: iNeuron Intelligence
    #                                         Version: 1.0
    #                                         Revisions: None
    #
    #                                                 """
    #
    #     try:
    #         path = 'Training_Raw_files_validated/'
    #         # if os.path.isdir("ids/" + userName):
    #         # if os.path.isdir(path + 'Bad_Raw/'):
    #         #     shutil.rmtree(path + 'Bad_Raw/')
    #         if os.path.isdir(path + 'Good_Raw/'):
    #             shutil.rmtree(path + 'Good_Raw/')
    #             file = open("Training_Logs/GeneralLog.txt", 'a+')
    #             self.logger.log(file,"GoodRaw directory deleted successfully!!!")
    #             file.close()
    #     except OSError as s:
    #         file = open("Training_Logs/GeneralLog.txt", 'a+')
    #         self.logger.log(file,"Error while Deleting Directory : %s" %s)
    #         file.close()
    #         raise OSError
    #
    # def deleteExistingBadDataTrainingFolder(self):
    #
    #     """
    #                                         Method Name: deleteExistingBadDataTrainingFolder
    #                                         Description: This method deletes the directory made to store the bad Data.
    #                                         Output: None
    #                                         On Failure: OSError
    #
    #                                          Written By: iNeuron Intelligence
    #                                         Version: 1.0
    #                                         Revisions: None
    #
    #                                                 """
    #
    #     try:
    #         path = 'Training_Raw_files_validated/'
    #         if os.path.isdir(path + 'Bad_Raw/'):
    #             shutil.rmtree(path + 'Bad_Raw/')
    #             file = open("Training_Logs/GeneralLog.txt", 'a+')
    #             self.logger.log(file,"BadRaw directory deleted before starting validation!!!")
    #             file.close()
    #     except OSError as s:
    #         file = open("Training_Logs/GeneralLog.txt", 'a+')
    #         self.logger.log(file,"Error while Deleting Directory : %s" %s)
    #         file.close()
    #         raise OSError
    #
    # def moveBadFilesToArchiveBad(self):
    #
    #     """
    #                                         Method Name: moveBadFilesToArchiveBad
    #                                         Description: This method deletes the directory made  to store the Bad Data
    #                                                       after moving the data in an archive folder. We archive the bad
    #                                                       files to send them back to the client for invalid data issue.
    #                                         Output: None
    #                                         On Failure: OSError
    #
    #                                          Written By: iNeuron Intelligence
    #                                         Version: 1.0
    #                                         Revisions: None
    #
    #                                                 """
    #     now = datetime.now()
    #     date = now.date()
    #     time = now.strftime("%H%M%S")
    #     try:
    #
    #         source = 'Training_Raw_files_validated/Bad_Raw/'
    #         if os.path.isdir(source):
    #             path = "TrainingArchiveBadData"
    #             if not os.path.isdir(path):
    #                 os.makedirs(path)
    #             dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
    #             if not os.path.isdir(dest):
    #                 os.makedirs(dest)
    #             files = os.listdir(source)
    #             for f in files:
    #                 if f not in os.listdir(dest):
    #                     shutil.move(source + f, dest)
    #             file = open("Training_Logs/GeneralLog.txt", 'a+')
    #             self.logger.log(file,"Bad files moved to archive")
    #             path = 'Training_Raw_files_validated/'
    #             if os.path.isdir(path + 'Bad_Raw/'):
    #                 shutil.rmtree(path + 'Bad_Raw/')
    #             self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
    #             file.close()
    #     except Exception as e:
    #         file = open("Training_Logs/GeneralLog.txt", 'a+')
    #         self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
    #         file.close()
    #         raise e
    #
    #
    #

    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None

                """
        #
        # #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        # self.deleteExistingBadDataTrainingFolder()
        # self.deleteExistingGoodDataTrainingFolder()
        # #create new directories
        self.createDirectoryForGoodBadRawData()
        bucket = self.resource.Bucket('waferinputdata')
        onlyfiles = [obj.key for obj in bucket.objects.filter()]
        try:
            for filename in onlyfiles:
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            copy_source = {
                                'Bucket': 'waferinputdata',
                                'Key': filename
                            }
                            self.resource.meta.client.copy(copy_source, 'goodrawdata', filename)

                            message = "Valid File name!! File moved to GoodRaw Folder "
                            data_db = {'objective': 'FileNameValidation', 'status': 'ok',
                                    'error_type': '', 'file_name': filename,
                                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                            self.db_obj.insert_data(data_db)

                        else:
                            copy_source = {
                                'Bucket': 'waferinputdata',
                                'Key': filename
                            }
                            self.resource.meta.client.copy(copy_source, 'badrawdata', filename)

                            message = "Invalid File name!! File moved to BadRaw Folder "
                            data_db = {'objective': 'FileNameValidation', 'status': 'error',
                                    'error_type': 'Lenght of Timestamp Mismatched', 'file_name': filename,
                                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                            self.db_obj.insert_data(data_db)
                    else:
                        copy_source = {
                            'Bucket': 'waferinputdata',
                            'Key': filename
                        }
                        self.resource.meta.client.copy(copy_source, 'badrawdata', filename)

                        message = "Invalid File name!! File moved to BadRaw Folder "
                        data_db = {'objective': 'FileNameValidation', 'status': 'error',
                                'error_type': 'Lenght of Date Sample Mismatched', 'file_name': filename,
                                'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                        self.db_obj.insert_data(data_db)
                else:
                    copy_source = {
                        'Bucket': 'waferinputdata',
                        'Key': filename
                    }
                    self.resource.meta.client.copy(copy_source, 'badrawdata', filename)

                    message = "Invalid File name!! File moved to BadRaw Folder "
                    data_db = {'objective': 'FileNameValidation', 'status': 'error',
                            'error_type': 'File Naming convention Mismatched', 'file_name': filename,
                            'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                    self.db_obj.insert_data(data_db)

        except Exception as e:
            message = str(e)
            data_db = {'objective': 'FileNameValidation', 'status': 'error',
                    'error_type': 'ExceptionError', 'file_name': '',
                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e




    def validateColumnLength(self,NumberofColumns):
        """
                          Method Name: validateColumnLength
                          Description: This function validates the number of columns in the csv files.
                                       It is should be same as given in the schema file.
                                       If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                       If the column number matches, file is kept in Good Raw Data for processing.
                                      The csv file is missing the first column name, this function changes the missing name to "Wafer".
                          Output: None
                          On Failure: Exception

                          Written By: iNeuron Intelligence
                          Modified By: Sourav
                          Modification Done For: AWS integration
                          Version: 1.0
                          Revisions: None

                      """
        try:
            bucket = self.resource.Bucket('goodrawdata')
            files = [obj.key for obj in bucket.objects.filter()]

            for file in files:
                obj = self.client.get_object(
                Bucket='goodrawdata',
                Key=file)
                csv = pd.read_csv(obj['Body'])
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    copy_source = {
                        'Bucket': 'goodrawdata',
                        'Key': file
                        }
                    self.resource.meta.client.copy(copy_source, 'badrawdata', file)
                    self.resource.Object('goodrawdata',  file).delete()
                    message = "Invalid Column Length for the file!! File moved to Bad Raw Folder"
                    data_db = {'objective': 'ColumnLengthValidation', 'status': 'error',
                            'error_type': 'Error occured while validating Column Length', 'file_name': file,
                            'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                    self.db_obj.insert_data(data_db)


        except OSError as ex:
            message = str(ex)
            data_db = {'objective': 'ColumnLengthValidation', 'status': 'error',
                    'error_type': 'OSError', 'file_name': "",
                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            raise OSError
        except Exception as e:

            message = str(e)
            data_db = {'objective': 'ColumnLengthValidation', 'status': 'error',
                    'error_type': 'ExceptionError', 'file_name': '',
                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data_db.
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            bucket = self.resource.Bucket('goodrawdata')
            files = [obj.key for obj in bucket.objects.filter()]

            for file in files:
                obj = self.client.get_object(
                        Bucket = 'goodrawdata',
                        Key = file)
                csv = pd.read_csv(obj['Body'])
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        copy_source = {
                        'Bucket': 'goodrawdata',
                        'Key': file
                        }
                        self.resource.meta.client.copy(copy_source, 'badrawdata', file)
                        self.resource.Object('goodrawdata', file).delete()
                        message = "Invalid Column Length for the file!! File moved to Bad Raw Folder"
                        data_db = {'objective': 'validateMissingValuesInWholeColumn', 'status': 'error',
                                'error_type': 'Whole Column has Missing Value', 'file_name': file,
                                'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                        self.db_obj.insert_data(data_db)
                        break
                if count==0:

                    csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv_buffer = StringIO()
                    csv.to_csv(csv_buffer)
                    self.resource.Object('goodrawdata', file).put(Body=csv_buffer.getvalue())
        except OSError as ex:
            message = str(ex)
            data_db = {'objective': 'validateMissingValuesInWholeColumn', 'status': 'error',
                    'error_type': 'OSError', 'file_name': "",
                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            raise OSError
        except Exception as e:
            message = str(e)
            data_db = {'objective': 'validateMissingValuesInWholeColumn', 'status': 'error',
                    'error_type': 'ExceptionError', 'file_name': '',
                    'message': message, 'time':dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e












