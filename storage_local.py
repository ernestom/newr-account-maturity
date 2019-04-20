import csv
import os
import time

class StorageLocal():

    def __init__(self, folder):
        self.files = {}
        self.folder = folder
        output_folder = time.strftime('MATURITY_RUN-%Y-%m-%d %H:%M', time.localtime())
        self.output_folder = os.path.join(self.folder, output_folder)
        if not os.path.exists(self.output_folder):
            os.mkdir(self.output_folder)

    def get_file_handle(self, name):
        if not name in self.files:
            handle = open(os.path.join(self.output_folder, name + '.csv'), 'w')
            self.files.update({name: handle})
            just_created = True
        else:
            just_created = False
        return self.files[name], just_created

    def get_accounts(self, name):
        with open(os.path.join(self.folder, name + '.csv')) as csv_file:
            csv_reader = csv.DictReader(csv_file, delimiter=',')
            return list(row for row in csv_reader) 
        
    def dump_metrics(self, name, data=[], metadata={}):
        if type(data) == list:
            handle, just_created = self.get_file_handle(name)
            if len(data) > 0:
                if len(metadata) > 0:
                    for row in data:
                        row.update(metadata)
                fieldnames = data[0].keys()
                csv_writer = csv.DictWriter(handle, fieldnames=fieldnames)
                if just_created:
                    csv_writer.writeheader()
                for row in data:
                    csv_writer.writerow(row)
                handle.flush()