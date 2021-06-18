from flask import Flask, request, render_template, Response
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
from predictFromModel import prediction
from Connect_with_awsS3.connect_aws import aws
from logs_insertion_to_db.training_log_insertion_to_db import training_log_insertion_to_db
import json
from datetime import datetime as dt
#
application = Flask(__name__)

@application.route("/", methods=['GET'])
def home():
    return render_template('index.html')
#
@application.route("/predict", methods=['POST'])
def predictRouteClient():
    aws_obj = aws()
    try:

        if request.json is not None:
            path = request.json['filepath']

            pred_val = pred_validation(aws_obj.client, aws_obj.resource)  # object initialization
            pred_val.prediction_validation()  # calling the prediction_validation function

            pred = prediction(path, aws_obj.client, aws_obj.resource)  # object initialization

            # predicting for dataset present in database

            json_predictions = pred.predictionFromModel()
            aws_obj.send_mail('shahriarsourav@iut-dhaka.edu', 'shahriar@moonfroglabs.com', 'Bad Data',
                              'Bad Data CSV files of prediction are added below', 'badrawdataprediction')
            return Response("Prediction File created at !!!"  +str(path) +'and few of the predictions are '+str(json.loads(json_predictions) ))
        elif request.form is not None:
            path = request.form['filepath']
            db_obj = training_log_insertion_to_db('PredictionGeneralLog')
            data_db = {'objective': 'PredictionSystem', 'message': "Prediction Started",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            db_obj.insert_data(data_db)

            pred_val = pred_validation(aws_obj.client, aws_obj.resource) #object initialization
            pred_val.prediction_validation() #calling the prediction_validation function

            pred = prediction(path, aws_obj.client, aws_obj.resource) #object initialization
            # predicting for dataset present in database
            json_predictions = pred.predictionFromModel()
            data_db = {'objective': 'PredictionSystem', 'message': "Prediction End",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            db_obj.insert_data(data_db)
            aws_obj.send_mail('shahriarsourav@iut-dhaka.edu', 'shahriar@moonfroglabs.com', 'Bad Data',
                      'Bad Data CSV files of prediction are added below', 'badrawdataprediction')
            return Response("Prediction File created at the PredictionOutput table and few of the predictions are" +str(json.loads(json_predictions) ))
        else:
            print('Nothing Matched')
    except ValueError as v:
        data_db = {'objective': 'PredictionSystem', 'message': str(v),
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)

        return Response("Error Occurred! %s" %ValueError)
    except KeyError as k:
        data_db = {'objective': 'PredictionSystem', 'message': str(k),
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        return Response("Error Occurred! %s" %KeyError)
    except Exception as e:
        data_db = {'objective': 'PredictionSystem', 'message': str(e),
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        return Response("Error Occurred! %s" %e)



@application.route("/train", methods=['GET'])
def trainRouteClient():
    aws_obj = aws()
    try:


        #if request.json['folderPath'] is not None:
            #path = request.json['folderPath']
            #path = 'Training_Batch_Files'
        db_obj = training_log_insertion_to_db('TrainingGeneralLog')
        print("obj created")
        data_db = {'objective': 'TrainSystem', 'message': "Training Started",
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        print("ok5")
        db_obj.insert_data(data_db)
        print("inserting data")
        aws_obj.delete_modelfiles('modelfilesh')
        #db_obj = training_log_insertion_to_db('TrainingGeneralLog')
        train_valObj = train_validation(aws_obj.client, aws_obj.resource)  # object initialization
        train_valObj.train_validation()  # calling the training_validation function
        trainModelObj = trainModel(aws_obj.client, aws_obj.resource)  # object initialization
        trainModelObj.trainingModel()  # training the model for the files in the table
        data_db = {'objective': 'TrainSystem', 'message': "Training Done",
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        aws_obj.send_mail('shahriarsourav@iut-dhaka.edu', 'shahriar@moonfroglabs.com', 'Bad Data',
                          'Bad Data CSV files of training are added below', 'badrawdata')
        print("mail sent")

    except ValueError:

        return Response("Error Occurred! %s" % ValueError)

    except KeyError:

        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:

        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")

#--------Main------------------
if __name__ == "__main__":
    #application.run()
   application.run(host='127.0.0.1', port=8001, debug=True)
#------------------------------

######