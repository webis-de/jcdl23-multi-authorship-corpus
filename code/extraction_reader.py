import traceback
from json import dump, loads, load
from pprint import pprint, pformat
from glob import glob
from os.path import basename, exists, isfile, isdir, sep
from os import makedirs
from datetime import datetime
from argparse import ArgumentParser
from multiprocessing import Pool
from math import ceil
import fasttext
import utils

class ExtractionReaderJSON:
    """
    API to read JSON Lines files of entries extracted from CORE.
    
    Attributes:
        input_filepaths: The absolute paths to the input file(s).
        output_directory: The path to the directory where CSVs, JSONSs, logs and metadata will be saved in a timestamped subdirectory below output_directory.
        pool_size: Size of process pool for multiprocessing.

    """
    def __init__(self, input_filepath, pool_size = 10, load_model = False):

        if isfile(input_filepath):
            self.input_filepaths = [input_filepath]
        if isdir(input_filepath):
            self.input_filepaths = sorted(glob(input_filepath + sep + "*"))

        self.pool_size = pool_size
        self.output_directory = None
        self.load_model = load_model

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
            return loads(line)
        except:
            traceback.print_exc()
            return 

    def explore(self, conditions = None, prune_text = False, process_text = True): 
        """
        Read extracted JSON Lines file and pretty print entries.
        """
        options = {0:"e",1:"f",2:"s",3:"q"}
        for filepath in self.input_filepaths:
            if input(basename(filepath) + " Skip batch? ('s') ") == "s":
                continue
            with open(filepath) as input_file:
                for line in input_file:
                    entry = self.load_entry(line)
                    if process_text:
                        entry['full_text'] = utils.preprocessor(entry['full_text'])
                    if conditions:
                        for condition in conditions:
                            if not eval(condition):
                                break
                        else:
                            if prune_text:
                                if len(entry["full_text"]) > 100:
                                    entry["full_text"] = entry["full_text"][:100]+'[...]'
                                if entry["abstract"] and len(entry["abstract"]) > 100:
                                    entry["abstract"] = entry["abstract"][:100]+'[...]'
                            print("#" * 100)
                            check = input(entry["title"] + "\n" + "Print entry ({}), print fullText ({}), skip batch ({}), quit({}) or skip entry (ENTER)? ".format(options[0],options[1],options[2],options[3]))
                            if check == options[0]:
                                print(pprint(entry, sort_dicts=False))
                            elif check == options[1]:
                                print(entry["full_text"].replace("\n",""))
                            elif check == options[2]:
                                break
                            elif check == options[3]:
                                exit()
                            else:
                                continue
                    else:
                        if prune_text:
                            if len(entry["full_text"]) > 100:
                                entry["full_text"] = entry["full_text"][:100]+'[...]'
                            if entry["abstract"] and len(entry["abstract"]) > 100:
                                entry["abstract"] = entry["abstract"][:100]+'[...]'
                        print("#" * 100)
                        check = input(entry["title"] + "\n" + "Print entry ({}), print fullText ({}), skip batch ({}), quit({}) or skip entry (ENTER)? ".format(options[0],options[1],options[2],options[3]))
                        if check == options[0]:
                            print(pformat(entry,sort_dicts=False))
                        elif check == options[1]:
                            print(entry["full_text"].replace("\n",""))
                        elif check == options[2]:
                            break
                        elif check == options[3]:
                            exit()
                        else:
                            continue

    def fulltextlength_of_entries_in_batches(self, output_filepath):
        """
        Write length of fullText of each entry in JSON Lines file(s) to file.

        Args:
            output_filepath: Path to which fullText lengths will be written.
        """
        with open(output_filepath, "w") as output_file:
            with Pool(self.pool_size) as pool:
                results = pool.map(self.fulltextlength_of_entries_in_batch, self.input_filepaths)
            for result in results:
                for text_length in result:
                    output_file.write(str(text_length) + "\n")

    def fulltextlength_of_entries_in_batch(self, input_filepath):
        """Helper function for parallel processing."""
        print(basename(input_filepath))
        text_lengths = []
        with open(input_filepath) as input_file:
            for line in input_file:
                entry = loads(line)
                if entry["fullText"]:
                    text_lengths.append(len(entry["fullText"]))
                else:
                    text_lengths.append(0)
        return text_lengths

    def reduce_batches(self, output_directory, conditions):
        """
        Reduce extracted JSON Lines file(s) to entries matching conditions.

        Args:
            conditions: Conditions according to which entries will be extracted.
                        "entry['publisher'] == 'Hindawi Publishing Corporation'" will extract any entry
                        where the value to the key 'publisher' matches 'Hindawi Publishing Corporation'.
            output_directory: Directory to which the JSON data will be extracted.
        """
        self.setup(output_directory, conditions, "Extracting entries matching conditions: " + str(conditions))
        batch_directory = self.output_directory + sep + "batches"
        if not exists(batch_directory): makedirs(batch_directory)
        with Pool(self.pool_size) as pool:
            pool.map(self.reduce_batch, [(input_filepath, batch_directory + sep + basename(input_filepath), conditions) for input_filepath in self.input_filepaths])

    def reduce_batch(self, input_filepath_and_output_filepath_and_conditions):
        """Helper function for parallel processing."""
        input_filepath = input_filepath_and_output_filepath_and_conditions[0]
        output_filepath = input_filepath_and_output_filepath_and_conditions[1]
        conditions = input_filepath_and_output_filepath_and_conditions[2]
        print(basename(input_filepath))
        with open(output_filepath, "w") as output_file:
            with open(input_filepath) as input_file:
                if self.load_model:
                    path_to_pretrained_model = 'models/lid.176.ftz'
                    fmodel = fasttext.load_model(path_to_pretrained_model)
                for line in input_file:
                    entry = self.load_entry(line)
                    for condition in conditions:
                        if not eval(condition):
                            break
                    else:
                        dump(entry, output_file)
                        output_file.write("\n")

    def count_entries_in_batches(self, output_filepath, conditions = None):
        """
        Count entries in JSON Lines file(s).

        Args:
            output_filepath: Path to the file where the overview will be saved.
            conditions: Conditions according to which entries will be counted.
                        "entry['publisher'] == 'Hindawi Publishing Corporation'" will count any entry
                        where the value to the key 'publisher' matches 'Hindawi Publishing Corporation'.
        """
        with open(output_filepath, "w") as output_file:
            print(self.pool_size)
            with Pool(self.pool_size) as pool:
                results = pool.map(self.count_entries_in_batch, [(input_filepath, conditions) for input_filepath in self.input_filepaths])
            if len(self.input_filepaths) > 1:
                output_file.write("Entries counted from: " + self.input_filepaths[0][:self.input_filepaths[0].rfind('/')] + "\n")
            else:
                output_file.write("Entries counted from: " + self.input_filepaths[0] + "\n") 
            if conditions:
                output_file.write("Entries matching conditions: " + str(sum([result[1] for result in results])) + "\n")
                output_file.write("Conditions: " + str(conditions) + "\n")
            else:
                output_file.write("Total entry count: " + str(sum([result[1] for result in results])) + "\n")
            for entry in results:
                output_file.write(basename(entry[0]) + ": " + str(entry[1]) + "\n")

    def count_entries_in_batch(self, input_filepath_and_conditions):
        """Helper function for parallel processing."""
        input_filepath = input_filepath_and_conditions[0]
        conditions = input_filepath_and_conditions[1]
        print(basename(input_filepath))
        count = 0
        with open(input_filepath) as input_file:
            for line in input_file:
                if conditions:
                    entry = self.load_entry(line)
                    if not entry:
                        continue
                    for condition in conditions:
                        if not eval(condition):
                            break
                    else:
                        count += 1
                else:
                    count += 1
                    
        return (basename(input_filepath), count)
    
    

    def map_key_of_entries_in_batches(self, output_filepath, key):
        """
        Map entries in JSON Lines file(s) to specific key.

        Args:
            output_filepath: Path to the file where the map will be saved.
            key: Key according to which entries will be mapped.
                 'publisher' will create of map of all publishers in the file entries and their frequency.
        """
        with open(output_filepath, "w") as output_file:
            with Pool(self.pool_size) as pool:
                mappings = pool.map(self.map_key_of_entries_in_batch, [(input_filepath, key) for input_filepath in self.input_filepaths])
            unified_mapping = {}
            for mapping in mappings:
                for key in mapping:
                    if key not in unified_mapping:
                        unified_mapping[key] = 0
                    unified_mapping[key] += mapping[key]
            unified_mapping = {key:unified_mapping[key] for key in sorted(list(unified_mapping.keys()), key = lambda x: unified_mapping[x], reverse=True)}
            for key in unified_mapping:
                output_file.write(str(key) + ": " + str(unified_mapping[key]) + "\n")

    def map_key_of_entries_in_batch(self, input_filepath_and_key):
        """Helper function for parallel processing."""
        input_filepath = input_filepath_and_key[0]
        key = input_filepath_and_key[1]
        print(basename(input_filepath))
        mapping = {}
        with open(input_filepath) as input_file:
            for line in input_file:
                entry = loads(line)
                str_key = str(entry[key])
                if str_key not in mapping:
                    mapping[str_key] = 0
                mapping[str_key] += 1
        return mapping
    
                 

if __name__ == "__main__":

    argument_parser = ArgumentParser()

    argument_parser.add_argument("-i", "--input")
    argument_parser.add_argument("-o", "--output")
    argument_parser.add_argument("--cond", nargs='+', default=[])
    argument_parser.add_argument("--key", nargs='+', default=["publisher"])
    argument_parser.add_argument("--mode", default='explore')
    argument_parser.add_argument("--size", default=10, type=int)
    argument_parser.add_argument("--list", default=None)
    argument_parser.add_argument("--load_model", default=False)



    args = vars(argument_parser.parse_args())

    INPUT_PATH = args["input"]
    OUTPUT_PATH = args["output"]
    COND = args["cond"]
    KEYS = args["key"]
    MODE = args["mode"]
    SIZE = args["size"]
    LIST = args["list"]
    MODEL = args["load_model"]


    with ExtractionReaderJSON(input_filepath=INPUT_PATH, pool_size=SIZE, load_model=MODEL) as er:
        if LIST:
            input_json = load(open(LIST))
            print("Input set successfully loaded.")
        if MODEL:
            path_to_pretrained_model = 'models/lid.176.ftz'
            fmodel = fasttext.load_model(path_to_pretrained_model)

        
        if MODE == "explore":
            er.explore(COND)
        elif MODE == "textlength":
            er.fulltextlength_of_entries_in_batches(OUTPUT_PATH)
        elif MODE == "reduce":
            er.reduce_batches(OUTPUT_PATH, COND)
        elif MODE == "entrycount":
            er.count_entries_in_batches(OUTPUT_PATH, COND)
        elif MODE == "map":
            er.map_key_of_entries_in_batches(OUTPUT_PATH, KEY)
        else:
            print("INVALID MODE.")
