import csv
import os
import time

class StorageLocal():

    def __init__(self, account_file, output_folder, subfolder_prefix='RUN', timestamp=None):
        """ init """
        
        self.__cache = {}
        self.__account_file = account_file
        self.__output_folder = \
            os.path.join(
                output_folder,
                time.strftime(
                    f'{subfolder_prefix}_%Y-%m-%d_%H-%M', 
                    time.localtime() if not timestamp else timestamp
                )
            )

    def __get_handle(self, name):
        """ returns a file handle from the cache or creates a new one """

        if not name in self.__cache:
            handle = open(os.path.join(self.__output_folder, name + '.csv'), 'w')
            self.__cache.update({name: handle})
            just_created = True
        else:
            just_created = False

        return self.__cache[name], just_created

    def get_accounts(self):
        """ returns a list of accounts dictionaries """

        with open(os.path.join(self.__account_file)) as f:
            csv_reader = csv.DictReader(f, delimiter=',')
            return list(dict(row) for row in csv_reader) 
        
    def dump_data(self, output_file, data=[]):
        """ appends the data to the output file """

        # creates the output folder on the first dump
        if not len(self.__cache) and not os.path.exists(self.__output_folder):
            os.mkdir(self.__output_folder, mode=0o755)

        if type(data) == list and len(data):
            handle, just_created = self.__get_handle(output_file)
            csv_writer = csv.DictWriter(handle, fieldnames=data[0].keys())
            if just_created:
                csv_writer.writeheader()
            for row in data:
                csv_writer.writerow(row)
            handle.flush()