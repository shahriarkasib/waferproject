import pandas
import json
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from logs_insertion_to_db.training_log_insertion_to_db import training_log_insertion_to_db
from datetime import datetime as dt
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation


class prediction:

    def __init__(self,path,client, resource):
        self.client = client
        self.resource = resource
        self.db_obj = training_log_insertion_to_db('PredictionLog')
        self.db_obj_table = training_log_insertion_to_db('PredictionOutput')
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(self.client, self.resource)

    def predictionFromModel(self):

        try:


            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '', 'message': "Start of Training",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '',
                       'message': "Getting Prediction Data from DataBase",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}

            data_getter=data_loader_prediction.Data_Getter_Pred(self.client, self.resource)
            data=data_getter.get_data()
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '',
                       'message': "Got Prediction Data from DataBase",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            #code change
            # wafer_names=data['Wafer']
            # data=data.drop(labels=['Wafer'],axis=1)
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '',
                       'message': "Start Prepearing Data for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            preprocessor=preprocessing.Preprocessor(self.db_obj)
            is_null_present=preprocessor.is_null_present(data)
            if(is_null_present):
                data=preprocessor.impute_missing_values(data)
            cols_to_drop=preprocessor.get_columns_with_zero_std_deviation(data)
            data=preprocessor.remove_columns(data,cols_to_drop)
            #data=data.to_numpy()
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '', 'message': "Data Prepeared For Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            file_loader=file_methods.File_Operation(self.db_obj_table, self.client, self.resource)
            kmeans=file_loader.load_model('KMeans')
            ##Code changed
            #pred_data = data.drop(['Wafer'],axis=1)
            clusters=kmeans.predict(data.drop(['Wafer'],axis=1))#drops the first column for cluster prediction
            data['clusters']=clusters
            clusters=data['clusters'].unique()

            self.db_obj.table.drop()
            for i in clusters:
                cluster_data= data[data['clusters']==i]
                wafer_names = list(cluster_data['Wafer'])
                cluster_data=data.drop(labels=['Wafer'],axis=1)
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.find_correct_model_file(i,self.client, self.resource)
                model = file_loader.load_model(model_name)
                result=list(model.predict(cluster_data))
                result = pandas.DataFrame(list(zip(wafer_names,result)),columns=['Wafer','Prediction'])

                out = json.loads(result.to_json(orient='records'))
                self.db_obj_table.table.insert_many(out)

            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '', 'message': "End of Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
        except Exception as ex:
            data_db = {'objective': 'prediction', 'status': 'error', 'error': 'ExceptionError', 'message':str(ex),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise ex
        return  result.head().to_json(orient="records")




