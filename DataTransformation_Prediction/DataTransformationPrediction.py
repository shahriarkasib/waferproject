from datetime import datetime
from os import listdir
import pandas as pd
from io import StringIO
from logs_insertion_to_db.training_log_insertion_to_db import training_log_insertion_to_db
from datetime import datetime as dt


class dataTransformPredict:

     """
                  This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

                  Written By: iNeuron Intelligence
                  Version: 1.0
                  Revisions: None

                  """

     def __init__(self,client,resource):
          self.client = client
          self.resource = resource
          self.db_obj = training_log_insertion_to_db('DataTransformLogPrediction')


     def replaceMissingWithNull(self):

          """
                                  Method Name: replaceMissingWithNull
                                  Description: This method replaces the missing values in columns with "NULL" to
                                               store in the table. We are using substring in the first column to
                                               keep only "Integer" data for ease up the loading.
                                               This column is anyways going to be removed during prediction.

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                                          """

          try:

               bucket = self.resource.Bucket('goodrawdataprediction')
               files = [obj.key for obj in bucket.objects.filter()]
               for file in files:
                    obj = self.client.get_object(
                         Bucket='goodrawdataprediction',
                         Key=file)
                    csv = pd.read_csv(obj['Body'])
                    csv.fillna('NULL', inplace=True)
                    # #csv.update("'"+ csv['Wafer'] +"'")
                    # csv.update(csv['Wafer'].astype(str))
                    csv['Wafer'] = csv['Wafer'].str[6:]
                    csv_buffer = StringIO()
                    csv.to_csv(csv_buffer)
                    self.resource.Object('goodrawdataprediction', file).put(Body=csv_buffer.getvalue())
                    message = "File Transformed successfully"
                    data_db = {'objective': 'replaceMissingWithNull', 'status': 'ok',
                               'error_type': '', 'file_name': file,
                               'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                    self.db_obj.insert_data(data_db)
               #log_file.write("Current Date :: %s" %date +"\t" + "Current time:: %s" % current_time + "\t \t" +  + "\n")

          except Exception as e:
               message = str(e)
               data_db = {'objective': 'replaceMissingWithNull', 'status': 'error',
                          'error_type': 'ExceptionError', 'file_name': '',
                          'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
               self.db_obj.insert_data(data_db)
               raise e
