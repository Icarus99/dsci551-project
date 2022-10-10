# from _typeshed import FileDescriptor
import requests
import csv
import json
import sys
import pandas as pd
import random
import string



class EDFS_SIMULATOR(object):

    def __init__(self):
        self.url_filesystem = 'https://dsci551-7b600-default-rtdb.firebaseio.com/edfs'
        self.url_data = 'https://dsci551-7b600-default-rtdb.firebaseio.com/data'
        self.current_dir = ''

        # if the root is not exists, create one
        if(requests.get(self.url_filesystem + '.json').json() == None):
            requests.put(self.url_filesystem + '.json', json.dumps('Empty'))

    def mkdir(self, dir):
        # check if valid input
        if(dir == ''):
            print('usage: .mkdir(directory_name)')
            return

        # check if absolute path
        if(dir[0] == '/'):
            url = self.url_filesystem + dir + '.json'

        else:
            url = self.url_filesystem + self.current_dir + '/' + dir +'.json'

        # check if parent dir exists ????
        dirs = dir.split('/')
        if(len(dirs) > 1 and (dirs[0] != '' or len(dirs) > 2)):
            preq = requests.get(self.url_filesystem +
                                '/'.join(dirs[:-1])+'.json').json()
            if(preq == None):
                print(
                    f'mkdir: {"/".join(dirs[:-1])}: No such file or directory')
                return

        # check if dir exists
        req = requests.get(url).json()
        if(req):
            print(f'mkdir: {dir}: File exists')
        else:
            # create dir
            requests.put(url, json.dumps('Empty'))

    def ls(self, dir=''):
        if dir == '':
            path = self.current_dir
        elif(dir[0] == '/'):
            path = dir
        else:
            path = self.current_dir+'/' + dir

        req = requests.get(self.url_filesystem + path + '.json').json()

        if(req == None):
            print(f'ls: {path}: No such file or directory')
        elif(req == 'Empty'):
            print('')
        ##???
        else:
            for i in req.keys():
                print(i)


    def put(self, fileName, dir, numPartitions, method=''):
        filePath = '/' + fileName
        jsonName = filePath.replace("csv", "json")

        if dir == '':
            path = self.current_dir
        elif(dir[0] == '/'):
            path = dir
        else:
            path = self.current_dir+ '/' + dir

        if method == '':
            df = pd.read_csv(fileName)
            dict = df.to_dict(orient = 'records')#Convert the DataFrame to a dictionary. ‘records’ : list like [{column -> value}, … , {column -> value}] https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html

            chunk_size = len(dict) / numPartitions
            list_chunked = [dict[i:i + int(chunk_size)] for i in range(0, len(dict), int(chunk_size))]
            print(dict)

            for i in range(0, len(list_chunked)):
                data = json.dumps(list_chunked[i])
                random_key = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(100)) #generate an unique key for different parts
                dataPath = self.url_data + dir + jsonName + '/' + random_key + '.json'
                filesystemPath = self.url_filesystem + dir + jsonName + '/' + f'{i}' + '.json'
                requests.put(dataPath, data)
                requests.put(filesystemPath, dataPath)
        return


    def getPartitionLocations(self, filePath):
        filesystemPath = self.url_filesystem + filePath + '.json'
        result = requests.get(filesystemPath)
        print(result)
        return
    
    def readPartition(self, filePath, partitionNum):
        filesystemPath = self.url_filesystem + filePath + '/' + f'{partitionNum}' + '.json'
        url_result = requests.get(filesystemPath)
        print(url_result)
        result = requests.get(url_result)
        print(result)
        return

            