import csv
import os

class StorageGoogleDrive():

    def __init__(self, path):
        self.files = {}
        self.path = path

    def get_file_handle(self, name):
        if not name in self.files:
            handle = open(self.path + name + '.csv', 'w')
            self.files.update({name: handle})
            just_created = True
        else:
            just_created = False
        return self.files[name], just_created

    def get_accounts(self, name):
        with open(self.path + name + '.csv') as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            return list(row for row in csv_reader)
        
    def dump_metrics(self, name, data=[], metadata={}):
        pass

    def flush_metrics(self, location):
        pass
