import pymongo
import ssl
class training_log_insertion_to_db:
    def __init__(self, tablename):
        self.tablename = tablename
        print(self.tablename)
        self.dbname = 'waferProject'
        print(self.dbname)
        print("ok2")
        # self.client_mongo = pymongo.MongoClient(
        #         "mongodb://localhost:27017/")
        self.client_mongo = pymongo.MongoClient\
            ("mongodb+srv://shahriarsourav:160021062Ss290557@waferproject.x3xdx.mongodb.net/myFirstDatabase?retryWrites=true&w=majority",ssl_cert_reqs=ssl.CERT_NONE)

        print(self.client_mongo)
        print("ok3")
        self.db = self.client_mongo[self.dbname]
        print(self.db)
        print("ok4")
        self.table = self.db[self.tablename]
        print(self.table)



    def insert_data(self,data):
        self.table.insert_one(data)
