import traceback
from json import dump, loads, load
from pprint import pprint, pformat
from glob import glob
from os.path import basename, exists, isfile, isdir, sep, dirname, join
from os import makedirs
from datetime import datetime
from argparse import ArgumentParser
from multiprocessing import Pool
from math import ceil
import utils
import fasttext
from re import search
from time import sleep
import requests
from requests.exceptions import Timeout

class CorpusBuilderJSON:
    """
    API to read JSON Lines files of entries extracted from CORE.
    
    Attributes:
        input_filepaths: The absolute paths to the input file(s).
        output_directory: The path to the directory where CSVs, JSONSs, logs and metadata will be saved in a timestamped subdirectory below output_directory.
        pool_size: Size of process pool for multiprocessing.

    """
    def __init__(self, input_filepath, pool_size = 10):

        if isfile(input_filepath):
            self.input_filepaths = [input_filepath]
        if isdir(input_filepath):
            self.input_filepaths = sorted(glob(input_filepath + sep + "*"))
        
        self.pool_size = pool_size
        self.input_filepath = input_filepath
        self.output_directory = None


    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

    def setup(self, output_directory, conditions, message):
        """
        Set up run.
        """
        print(message)
        self.timestamp = str(datetime.now())[:-7].replace(":","_").replace("-","_").replace(" ","_")
        self.output_directory = output_directory + sep + self.timestamp
        if not exists(self.output_directory): makedirs(self.output_directory)
        with open(self.output_directory + sep + self.timestamp + ".metadata", "w") as metadata_file:
            if len(self.input_filepaths) > 1:
                metadata_file.write("Entries extracted from: " + self.input_filepaths[0][:self.input_filepaths[0].rfind('/')] + "\n")
            else:
                metatdata_file.write("Entries extracted from: " + self.input_filepaths[0] + "\n")
            metadata_file.write("conditions: " + str(conditions) + "\n")

    def load_entry(self, line):
        """
        Load a JSON line in CORE to JSON and report broken entries.

        Args:
            line: A JSON line.
        Returns:
            A JSON entry, None if broken.
        """
        try:
            l = loads(line)
            if not 'coreId' in l:
                traceback.print_exc()
                return None
            else:
                return l
        except:
            traceback.print_exc()
            return None
            
            
    def dict_authors_in_batches(self, output_filepath, conditions = None):
        """
            Creates a dictionary listing all authors and how often they appear in the data.
        """
        with open(output_filepath, "w") as output_file:
            with Pool(self.pool_size) as pool:
                results = pool.map(self.dict_authors_in_batch, [(input_filepath, conditions) for input_filepath in self.input_filepaths])
            authors = {}
            for entry in results:
                for person in entry[1]:
                    if person in authors:
                        authors[person] = authors[person] + entry[1][person]
                    else:
                        authors[person] = entry[1][person]
            dump(authors, output_file)
            print(f"Found {len(authors)} different authors.")

    def dict_authors_in_batch(self, input_filepath_and_conditions):
        """Helper function for parallel processing."""
        input_filepath = input_filepath_and_conditions[0]
        conditions = input_filepath_and_conditions[1]
        print(basename(input_filepath))
        authors = {}
        with open(input_filepath) as input_file:
            for line in input_file:
                entry = self.load_entry(line)
                if conditions:
                    for condition in conditions:
                        if not eval(condition):
                            break
                    else:
                        omit = []
                        for author in entry['authors']:
                            if author in authors and not author in omit:
                                authors[author] = authors[author] + 1
                                if entry['authors'].count(author) > 1:
                                    omit.append(author)
                            else:
                                authors[author] = 1
                else:
                    omit = []
                    for author in entry['authors']:
                        if author in authors and not author in omit:
                            authors[author] = authors[author] + 1
                            if entry['authors'].count(author) > 1:
                                omit.append(author)
                        else:
                            authors[author] = 1
        return (basename(input_filepath), authors)                       
                                
    def extract_features_from_batches(self, output_filepath, conditions = None, features = ["entry['coreId']"]):
        """
            Extracts the features specified in the "features" argument. Multiple features can be extracted at once.
            
            Args:
                output_filepath: File the resulting dict should be stored in
                conditions: conditions an entry should match to be included in the result set
                features
                            
            Creates a dictionary listing all selected features from all core entries matching the conditions.
        """
        with open(output_filepath, "w") as output_file:
            with Pool(self.pool_size) as pool:
                results = pool.map(self.extract_features_from_batch, [(input_filepath, conditions, features) for input_filepath in self.input_filepaths])
            feature_list = {}
            for result in results:
                for e in result[1]:
                    if e in feature_list:
                        print("Error: ID found multiple times")
                    else:
                        feature_list[e] = result[1][e]
            dump(feature_list, output_file)
            print(f"Length of dict generated: {len(feature_list)}")


    def extract_features_from_batch(self, input_filepath_and_conditions):
        """Helper function for parallel processing."""
        input_filepath = input_filepath_and_conditions[0]
        conditions = input_filepath_and_conditions[1]
        features = input_filepath_and_conditions[2]
        print(basename(input_filepath))
        result = {}
        with open(input_filepath) as input_file:
            for line in input_file:
                entry = self.load_entry(line)
                if entry is None:
                    print('empty line found')
                    continue
                if conditions:
                    for condition in conditions:
                        if not eval(condition):
                            break
                    else: 
                        r = {}
                        for feature in features:
                            r[feature] = eval(feature)
                        if entry['coreId'] in result:
                            print("Error: ID found multiple times")
                        else:
                            result[entry['coreId']] = r
                else:  
                    r = {}
                    for feature in features:
                        r[feature] = eval(feature)
                    if entry['coreId'] in result:
                        print("Error: ID found multiple times")
                    else:
                        result[entry['coreId']] = r
        return (basename(input_filepath), result)
    
    def timestamp(self, output_filepath, conditions = None, features = None):
        """
        Count entries in JSON Lines file(s).

        Args:
            output_filepath: Path to the file where the overview will be saved.
            conditions: Conditions according to which entries will be counted.
                        "entry['publisher'] == 'Hindawi Publishing Corporation'" will count any entry
                        where the value to the key 'publisher' matches 'Hindawi Publishing Corporation'.
        """
        with open(output_filepath, "w") as output_file:
            output_file.write("Started counting at: " + str(datetime.now()) + "\n")
            with Pool(self.pool_size) as pool:
                results = pool.map(self.extract_features_from_batch, [(input_filepath, conditions, features) for input_filepath in self.input_filepaths])
            feature_list = {}
            for result in results:
                for e in result[1]:
                    if e in feature_list:
                        print("Error: ID found multiple times")
                    else:
                        feature_list[e] = result[1][e]
            print(f"Length of dict generated: {len(feature_list)}")
            output_file.write("Finished counting at: " + str(datetime.now()))

    
    def create_content_table(self, conditions = None):
        """
            Creates a dictionary listing all authors and how often they appear in the data.
        """
        with open(f"{dirname(self.input_filepath)}/content_table.json", "w") as output_file:
            with Pool(self.pool_size) as pool:
                results = pool.map(self.create_content_table_batch, [(input_filepath, conditions) for input_filepath in self.input_filepaths])
            adresses = {}
            for entry in results:
                for cid in entry[1]:
                    if cid in adresses:
                        print(f"Error: id {cid} appeared multiple times")
                    else:
                        adresses[cid] = entry[1][cid]
            dump(adresses, output_file)
            print(f"Mapped {len(adresses)} different entries.")

    def create_content_table_batch(self, input_filepath_and_conditions):
        """Helper function for parallel processing."""
        input_filepath = input_filepath_and_conditions[0]
        conditions = input_filepath_and_conditions[1]
        print(basename(input_filepath))
        adresses = {}
        linec = 0
        with open(input_filepath) as input_file:
            for line in input_file:
                entry = self.load_entry(line)
                if conditions:
                    for condition in conditions:
                        if not eval(condition):
                            break
                    else:
                        adresses[entry['coreId']] = (basename(input_filepath),linec)
                else:
                    adresses[entry['coreId']] = (basename(input_filepath),linec)
                linec += 1
        return (basename(input_filepath), adresses)
    
    def open_specific(self, coreid):
        with open(f"{dirname(self.input_filepath)}/content_table.json") as contents:
            con = load(contents)
            adress = con[coreid]
            with open(f"{self.input_filepath}/{adress[0]}") as batch:
                for i, line in enumerate(batch):
                    if i == adress[1]:
                        entry = self.load_entry(line)
                        print("#" * 100)
                        for tag in ['title','authors','year','doi','fullText']:
                            if tag == 'fullText':
                                if len(entry['fullText']) > 4000:
                                    print(f'{tag}: {entry[tag][:4000]},\n')
                                else:
                                   print(f'{tag}: {entry[tag]},\n') 
                            else:
                                print(f'{tag}: {entry[tag]},\n')
                        break
                        

if __name__ == "__main__":

    argument_parser = ArgumentParser()

    argument_parser.add_argument("-i", "--input")
    argument_parser.add_argument("-o", "--output")
    argument_parser.add_argument("--cond", nargs='+', default=[])
    argument_parser.add_argument("--key", nargs='+', default=["publisher"])
    argument_parser.add_argument("--mode", default='explore')
    argument_parser.add_argument("--list", default=None)
    argument_parser.add_argument("--features", nargs='+', default=[])
    argument_parser.add_argument("--size", default=10, type=int)
    argument_parser.add_argument("--id")

    args = vars(argument_parser.parse_args())

    INPUT_PATH = args["input"]
    OUTPUT_PATH = args["output"]
    COND = args["cond"]
    KEYS = args["key"]
    MODE = args["mode"]
    SIZE = args["size"]
    LIST = args["list"]
    FEATURES = args["features"]
    ENTRYID = args["id"]
    
    with CorpusBuilderJSON(input_filepath=INPUT_PATH, pool_size=SIZE) as cb:
        if LIST:
            input_json = load(open(LIST))
            
        if MODE == "feature_extract":
            cb.extract_features_from_batches(OUTPUT_PATH, COND, FEATURES)
        elif MODE == "authors_json":
            cb.dict_authors_in_batches(OUTPUT_PATH, COND)
        elif MODE == "timestamp":
            cb.timestamp(OUTPUT_PATH, COND, FEATURES)
        elif MODE == "content_table":
            cb.create_content_table(COND)
        elif MODE == "open_specific":
            cb.open_specific(ENTRYID)
        else:
            print("INVALID MODE.")