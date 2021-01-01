
import os
import sys
import json
import threading
import time

class createReadDelete:

    def __init__(self):

        path=os.getcwd()
        self.fl = threading.Lock()
        self.dl = threading.Lock()
        self.filepath = path + '/datastore.json'

        # checks for the datastore else will create new one
    
        try:
            
            file = open(self.filepath, 'r')
            filedata = json.load(file)
            self.data = filedata
            file.close()

            # checks for the filesize

            if not self.checkSize():
                raise Exception('The Size of the DataStore is more than 1 GB.')

        except:

            file = open(self.filepath, 'w')
            self.data = {}
            self.timetolivedict = {}
            file.close()


    def create(self, key='', value='', timetolive=None):

        self.checkKey(key)

        if key == '':
            raise Exception('Yep! The key is missing')

        if value == '':
            value = None

       
        if sys.getsizeof(value) > 16000:                                          
            raise Exception("Size of the value should not exceed 16KB.")

        if not self.checkSize():
            raise Exception('The Size of the DataStore is more than 1 GB.')

        self.dl.acquire()
        if key in self.data.keys():
            self.dl.release()
            raise Exception('Oh no! The Key is already existing the datastore.')

                                                                                    
        if timetolive is not None:                                                         
            timetolive = int(time.time()) + abs(int(timetolive))

        dict = {'value': value, 'timetolive': timetolive}
        self.data[key] = dict

        self.fl.acquire()
        json.dump(self.data, fp=open(self.filepath, 'w'), indent=2)

        self.fl.release()
        self.dl.release()

        print('Hurray! Value Stored in the datastore.')



    def read(self, key=''):                                                   

        self.checkKey(key)

        if key == '':
            raise Exception('Yep! The key is missing')

        self.dl.acquire()

        if key not in self.data.keys():
            self.dl.release()
            raise Exception('Key not found in datastore')
            
        timetolive = self.data[key]['timetolive']

        
        if not timetolive:
            timetolive = 0


        if (timetolive == 0) or (time.time() < timetolive):
            self.dl.release()
            dict = {}
            dict[key] = self.data[key]
            print(dict)
            return

        else:
            self.dl.release()
            raise Exception("Oh no! Key's Time-To-Live has been expired.")



    def delete(self, key=''):                                                          


        self.checkKey(key)

        if key == '':
            raise Exception('Yep! The key is missing')

        self.dl.acquire()

        if key in self.data.keys():
            pass
        else:
            self.dl.release()
            raise Exception('Key not found in datastore')

        timetolive = self.data[key]['timetolive']
        if not timetolive:                                                                     
            timetolive = 0

        if time.time() < timetolive or (timetolive == 0):

            self.data.pop(key)

            self.fl.acquire()
            file = open(self.filepath, 'w')
            json.dump(self.data, file)

            self.fl.release()
            self.dl.release()

            print("Hurray! Key-Value deleted in the datastore.")
            return
        else:
            self.dl.release()
            raise Exception("Oh no! Key's Time-To-Live has been expired.")
                                                            

    def checkSize(self):
                                                                                 
        self.fl.acquire()

        if os.path.getsize(self.filepath) <= 1e+9:
            self.fl.release()
            return True
        else:
            self.fl.release()
            return False

        
    def checkKey(self, key):                                                    
           
        if type(key) == type(""):                                                  
            if len(key) > 32:
                raise Exception('Key size should be capped at 32 not ' + str(len(key)))
            else:
                return True
        else:
            raise Exception('Key needs to be of type string not ' + str(type(key)))
            return False



DataStore =  createReadDelete()  



