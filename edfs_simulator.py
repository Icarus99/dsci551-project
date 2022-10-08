import requests
import csv
import json
import sys


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
            url = self.url_filesystem+dir+'.json'

        else:
            url = self.url_filesystem + self.current_dir+'/'+dir+'.json'

        # check if parent dir exists
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
        else:
            for i in req.keys():
                print(i)
