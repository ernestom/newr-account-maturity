import csv
import os
import time

class StorageLocal():

    get_output_folder_name = lambda self: time.strftime('MATURITY_RUN-%Y-%m-%d %H:%M', time.localtime())

    def __init__(self, folder):
        self.__cache = {}
        self.output_folder = os.path.join(folder, self.get_output_folder_name())

        if not os.path.exists(self.output_folder):
            os.mkdir(self.output_folder)

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
        
    def dump_metrics(self, name, data=[], metadata={}):
        if type(data) == list and len(data) > 0:
            if len(metadata) > 0:
                for row in data:
                    row.update(metadata)
            handle, just_created = self.get_handle(name)

            csv_writer = csv.DictWriter(handle, fieldnames=data[0].keys())
            if just_created:
                csv_writer.writeheader()
            for row in data:
                csv_writer.writerow(row)
            handle.flush()