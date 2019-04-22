import csv
import os
import time

class StorageLocal():

    get_output_folder_name = lambda self,t: time.strftime('MATURITY_RUN-%Y-%m-%d %H:%M', t)

    def __init__(self, folder, timestamp=None):
        self.__cache = {}

        if timestamp:
            output_folder_name = self.get_output_folder_name(timestamp)
        else:
            output_folder_name = self.get_output_folder_name(time.localtime())

        self.output_folder = os.path.join(folder, output_folder_name)
        if not os.path.exists(self.output_folder):
            os.mkdir(self.output_folder, mode=0o755)

    def get_handle(self, name):
        if not name in self.__cache:
            handle = open(os.path.join(self.output_folder, name + '.csv'), 'w')
            self.__cache.update({name: handle})
            just_created = True
        else:
            just_created = False
        return self.__cache[name], just_created

    def get_accounts(self, filename):
        with open(os.path.join(filename)) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            return list(row for row in csv_reader) 
        
    def dump_metrics(self, name, data=[]):
        if type(data) == list and len(data) > 0:
            handle, just_created = self.get_handle(name)
            csv_writer = csv.DictWriter(handle, fieldnames=data[0].keys())
            if just_created:
                csv_writer.writeheader()
            for row in data:
                csv_writer.writerow(row)
            handle.flush()