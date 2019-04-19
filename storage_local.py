import csv
import os

class StorageLocal():

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
        if type(data) == list:
            handle, write_header = self.get_file_handle(name)
            if len(data) > 0:
                if len(metadata) > 0:
                    for row in data:
                        row.update(metadata)
                fieldnames = data[0].keys()
                csv_writer = csv.DictWriter(handle, fieldnames=fieldnames)
                if write_header:
                    csv_writer.writeheader()
                for row in data:
                    csv_writer.writerow(row)
                handle.flush()

    def flush_metrics(self, location):
        if os.path.exists(location):
            os.remove(location)
