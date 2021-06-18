import pymongo
import ssl
class training_log_insertion_to_db:
    def __init__(self, tablename):
        self.tablename = tablename
        self.dbname = 'waferProject'
        # self.client_mongo = pymongo.MongoClient(
        #         "mongodb://localhost:27017/")
        self.client_mongo = pymongo.MongoClient\
            ("mongodb+srv://shahriarsourav:160021062Ss290557@waferproject.x3xdx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",ssl_cert_reqs=ssl.CERT_NONE)

        self.db = self.client_mongo[self.dbname]
        self.table = self.db[self.tablename]



    def insert_data(self,data):
        self.table.insert_one(data)
