import pandas as pd
from datetime import datetime as dt
from logs_insertion_to_db.training_log_insertion_to_db import training_log_insertion_to_db


class Data_Getter:
    """
    This class shall  be used for obtaining the data from the source for training.

    Written By: iNeuron Intelligence
    Version: 1.0
    Revisions: None

    """
    def __init__(self, db_obj):
        self.db_obj = db_obj
        self.db_obj_table = training_log_insertion_to_db('InputData')

    def get_data(self):
        """
        Method Name: get_data
        Description: This method reads the data from source.
        Output: A pandas DataFrame.
        On Failure: Raise Exception

         Written By: iNeuron Intelligence
        Version: 1.0
        Revisions: None

        """
        data_db = {'objective': 'training', 'status':'ok','error':'', 'message': "Entered the get_data method of the Data_Getter class",
                'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        try:

            data = pd.DataFrame(list(self.db_obj_table.table.find()))
            data.drop('_id', axis = 1, inplace = True)
            data_db = {'objective': 'training', 'status':'ok','error':'', 'message': "Data Load Successful.Exited the get_data method of the Data_Getter class",
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            return data
        except Exception as e:

            data_db = {'objective': 'training', 'status':'error','error':'ExceptionError',
                    'message': str(e),
                    'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            raise Exception()


