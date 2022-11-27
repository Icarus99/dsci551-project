# from _typeshed import FileDescriptor
from fileinput import filename
from typing_extensions import runtime
import requests
import csv
import json
import sys
import pandas as pd
import random
import string
import math



class EDFS_SIMULATOR(object):

    def __init__(self):
        self.url_filesystem = 'https://dsci551-7b600-default-rtdb.firebaseio.com/edfs'
        self.url_data = 'https://dsci551-7b600-default-rtdb.firebaseio.com/data'
        self.current_dir = ''

        # if the root is not exists, create one
        if(requests.get(self.url_filesystem + '.json').json() == None):
            requests.put(self.url_filesystem + '.json', json.dumps(' '))

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
            requests.put(url, json.dumps(' '))

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
        elif(req == ' '):
            print('')
        ##???
        else:
            for i in req.keys():
                print(i)

    def rm(self, filePath):
        fileNameNoCSV = filePath.replace('.csv', '')
        filesystemPath = self.url_filesystem + fileNameNoCSV + '.json'
        datasystemPath = self.url_data + fileNameNoCSV + '.json'
        parentPath = fileNameNoCSV[:len(fileNameNoCSV) - len(fileNameNoCSV.split('/')[-1]) - 1]
        
        req = requests.get(filesystemPath).json()
        if(req == None):
            print(f'rm: {filePath}: No such file or directory')
        else:
            #delete data
            requests.delete(filesystemPath)
            #delete partitions
            requests.delete(datasystemPath)

            parentDir = self.url_filesystem + parentPath + '.json'
            if(requests.get(parentDir).json() == None):
                requests.put(parentDir, json.dumps(' '))

    def cat(self, filePath):
        fileNameNoCSV = filePath.replace('.csv', '')
        filesystemPath = self.url_filesystem + fileNameNoCSV + '.json'
        req = requests.get(filesystemPath).json()
        if(req == None):
            print(f'cat: {filesystemPath}: No such file or directory')
        else:
            csv = []
            for i in req:
                partition = requests.get(i).json()
                csv += partition
            csv = pd.DataFrame.from_dict(csv)
            print(csv)

# TO allow jupyter notebook to process large data input/output/transit, when open jupyter in terminal, 
# run: jupyter notebook --NotebookApp.iopub_data_rate_limit=1.0e10


# TEST put, getPartitionLocations, readPartition, run following command:
# edfs.put('Salary_Dataset.csv', '/user/jack', 3)
# edfs.getPartitionLocations('/user/jack/Salary_Dataset')
# edfs.readPartition('/user/jack/Salary_Dataset', 1)
    def put(self, fileName, dir, numPartitions, method=''):
        filePath = '/' + fileName
        fileNameNoCSV = filePath.replace('.csv', '')

        if dir == '':
            path = self.current_dir
        elif(dir[0] == '/'):
            path = dir
        else:
            path = self.current_dir+ '/' + dir

        if method == '':
            df = pd.read_csv(fileName, keep_default_na=False)
            dict = df.to_dict(orient = 'records')#Convert the DataFrame to a dictionary. ‘records’ : list like [{column -> value}, … , {column -> value}] https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_dict.html

            chunk_size = len(dict) / numPartitions
            list_chunked = [dict[i:min([i + int(chunk_size),len(dict)])] for i in range(0, len(dict), math.ceil(chunk_size))]
            
    
            for i in range(0, len(list_chunked)):
                data = json.dumps(list_chunked[i])
                random_key = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(5)) #generate an unique key for different parts
                # print(random_key)
                dataPath = self.url_data + dir + fileNameNoCSV + '/' + random_key + '.json'
                filesystemPath = self.url_filesystem + dir + fileNameNoCSV + '/' + f'{i}' + '.json'
                # print(dataPath)
                # print(filesystemPath)
                r = requests.put(dataPath, data)
                # print(r)
                requests.put(filesystemPath, json.dumps(dataPath))
                # print(i)
                # print("/n")
            print('finished')
        return


    def getPartitionLocations(self, filePath):
        filesystemPath = self.url_filesystem + filePath + '.json'
        # print(filesystemPath)
        result = requests.get(filesystemPath)
        # for ele in result.json():
        #     print(ele)
        # print(result)
        return result.json()
    
    def readPartition(self, filePath, partitionNum):
        #if user want the first partition, then the number 1 is fine, because the index 1 is the first partition
        filesystemPath = self.url_filesystem + filePath + '/' + f'{partitionNum}' + '.json'
        print(filesystemPath)
        url_result = requests.get(filesystemPath)
        print(url_result.json())
        # for ele in url_result.json():
        #     print(ele)
        result = requests.get(str(url_result.json()))
        print(result.json())
        # for item in result.json():
        #     print(item)
        # print(result)
        return

#------------------------   Task2   --------------------------
    def mapPartition(self, p, func, args=[]):
        partition = requests.get(p).json()
        if(len(args)>0):
            return func(partition, args)
        return func(partition)

    def reduce(self, partitions, func):
        return func(partitions)

    #database search functions
    def __get_job_with_salary(self, p, args):
        p = pd.DataFrame.from_dict(p)
        df = p[(p["Salary"] >= args[0]) & (p["Salary"] <= args[1])]
        return df[['Company Name','Job Title','Salary']]
 
    def __get_people_larger_than_50k(self, p, args):
        p = pd.DataFrame.from_dict(p)
        return p[p["income50K"] == 1][args]

    #database analytics functions
    def __get_avg_salary(self, p, args):
        p = pd.DataFrame.from_dict(p)
        title_df = p[p["Job Title"] == args[0]]
        return len(title_df.index),title_df["Salary"].sum()



    def __get_percent_50k(self, p, args):
        p = pd.DataFrame.from_dict(p)
        df = p[p["occupation"] == args[0]]
        return len(df), len(df[df["income50K"]==1])

    #reduce functions
    def __reduce_avg_salary(self, partitions):
        l = 0
        total = 0
        for i in partitions:
            l+=i[0]
            total+=i[1]
        return total/l

    def __reduce_job_with_salary(self, partitions):
        r = partitions[0]
        for i in range(1,len(partitions)):
            r = r.append(partitions[i], ignore_index = True)
        return r

    def __reduce_people_larger_than_50k(self, partitions):
        r = partitions[0]
        for i in range(1,len(partitions)):
            r = r.append(partitions[i], ignore_index = True)
        return r

    def __reduce_percent_50k(self, partitions):
        l = 0
        total = 0
        for i in partitions:
            l+=i[0]
            total+=i[1]
        if total > 0:
            return total/l * 100
        else:
            return 0

    #analytics functions
    def get_avg_salary(self, filePath, title):
        locations = self.getPartitionLocations(filePath)
        partitions = []
        for p in locations:
        #     print(p)
            t = self.mapPartition(p, self.__get_avg_salary, [title])
            if(t[0]>0):
                print(f'Partition: {p}\n   {title} num: {t[0]} sum: {t[1]} avg: {t[1]/t[0]}')
            else:
                print(f'Partition: {p}\n   {title} num: 0 sum: 0 avg: 0')
            partitions.append(t)
        result = self.reduce(partitions, self.__reduce_avg_salary)
        print(f'{title} avg salary: {result}')

    def get_percent_50k(self, filePath, occupation):
        locations = self.getPartitionLocations(filePath)
        partitions = []
        for p in locations:
            t = self.mapPartition(p, self.__get_percent_50k, [occupation])
            if(t[1]>0):
                print(f'Partition: {p}\n   {occupation} total num: {t[0]}   >50K num: {t[1]}   percentage: {t[1]/t[0] * 100}%')
            else:
                print(f'Partition: {p}\n   {occupation} total num: {t[0]}   >50K num: {t[1]}   percentage: 0%')
            partitions.append(t)
        result = self.reduce(partitions, self.__reduce_percent_50k)
        print(f'{occupation} >50K Percentage: {result}%')
    #search functions
    def get_job_with_salary(self, filePath, mi=0, ma=100000000):
        locations = self.getPartitionLocations(filePath)
        partitions = []
        for p in locations:
            t = self.mapPartition(p, self.__get_job_with_salary, [mi, ma])
            print(f'Partition: {p}\n   {t}')
            partitions.append(t)
        result = self.reduce(partitions, self.__reduce_job_with_salary)
        print(result)

    def get_people_larger_than_50k(self, filePath, args):
        locations = self.getPartitionLocations(filePath)
        partitions = []
        for p in locations:
            t = self.mapPartition(p, self.__get_people_larger_than_50k, args)
            print(f'Partition: {p}\n   {t}')
            partitions.append(t)
        result = self.reduce(partitions, self.__reduce_people_larger_than_50k)
        print(result)
#------------------------   Task3   --------------------------
    def cd(self, dir):
        if(dir=='../'):
            if(self.current_dir==''):
                print(f'cd: {dir}: No such directory')
            else:
                self.current_dir = self.current_dir[:len(self.current_dir)-len(self.current_dir.split('/')[-1])-1]
        elif('/' in dir):
            print('usage: do not contain '/'')
        else:
            if(requests.get(self.url_filesystem + '/' + self.current_dir + '/' + dir  + '.json').json() == None):
                print(f'cd: {dir}: No such directory')
            else:
                self.current_dir = self.current_dir + '/' + dir




# edfs = EDFS_SIMULATOR()
# edfs.readPartition('/user/jack/Salary_Dataset', 1)

