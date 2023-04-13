from glob import glob
from json import loads, dump, load
from os.path import basename, exists, isfile, isdir, sep
from os import makedirs, listdir, getcwd
from datetime import datetime
import pandas as pd
import pyarrow.parquet as pq

print("Merging.")

"""Input paths of the data sources are specified. Data from the Microsoft Academic Graph, CORE, grobid-extracted fulltexts and fields of study have to be merged together. For all four sources, the origin directories are specified."""

mode = 'normal'

if mode == 'test':
    mag_path = "process_2/mag_sets/mag_in_core"
    core_path = "process_2/method_test/mergetest"
    grobid_path = "/home/jovyan/mnt/ceph/storage/data-in-progress/data-research/text-reuse/phoenix/spark/grobid-sample.parquet"
    fos_path = "process_2/results/merge/fields_df.json"
    output_path = "process_2/method_test/test_corpus_merged"
else:
    mag_path = "process_2/mag_sets/mag_in_core"
    core_path = "process_2/core_sets/mag_matched/2021_04_09_12_08_34/batches"
    grobid_path = "/home/jovyan/mnt/ceph/storage/data-in-progress/data-research/text-reuse/phoenix/spark/grobid-sample.parquet"
    fos_path = "process_2/results/merge/fields_df.json"
    output_path = "corpus_merged"
timestamp = str(datetime.now())[:-7].replace(":","_").replace("-","_").replace(" ","_")
output_directory = output_path + sep + timestamp
if not exists(output_directory):
    makedirs(f"{output_directory}/batches")
    
    
"""A list of all matches between MAG and CORE generated in a seperate botebook (MAG-merging.ipynb) is loaded, containing a list of id pairs."""
    
    
mlist = load(open("process_2/results/mag_match/matches.json","r"))['matches']
matches = {}
for pair in mlist:
    if not pair[1] in matches:
        matches[pair[1]] = [pair[0]]
    else:
        matches[pair[1]] = matches[pair[1]] + [pair[0]]
        
"""All the dois of the grobid extracted fulltexts are loaded to identify the texts."""
        
grobid_dois = set(pq.read_table(grobid_path, columns=['doi']).to_pandas()['doi'])        

core_filepaths = sorted(glob(core_path + sep + "*"))
grobid_filepaths = sorted(glob(grobid_path + sep + "*"))[1:]


"""All data from the MAG entries needed is loaded and stored in a dict to quickly access when called via their magId"""

mag_dict = {}
with open(mag_path) as magfile:
    for line in magfile:
        entry = loads(line)
        mag_dict[entry['id']] = entry
print("Mag_dict created successfully")

"""Field of study data is loaded and stored in a dataframe, with the doi of the document as index."""

fos_df = pd.read_json('process_2/results/merge/fields_df.json').set_index('doi')
fos_dois = set(fos_df.index)
print("Fos_df loaded successfully")


"""A list of all dois in the corpus, sorted by the order of their appearance when going through the corpus line by line, is generated. This list will be used to load batches of grobid-fulltexts in the correct order, without running out of memory and avoiding to look for entries individually. This way, we know which fulltexts will be needed next."""

if exists('process_2/results/merge/ordered_dois.json'):
    doilist = load(open('process_2/results/merge/ordered_dois.json','r'))['dois']
    print("Existing DOI-List loaded.")
else:
    doilist = []
    print("Creating ordered DOI-List")
    for path in core_filepaths:
        with open(path) as f:
            print(basename(path))
            for line in f:
                entry = loads(line)
                if 'doi' in entry and entry['doi'] in grobid_dois:
                    doilist.append(entry['doi'])
                for m in matches[entry['coreId']]:
                    md = mag_dict[m]
                    if 'doi' in md and md['doi'] and md['doi'] in grobid_dois:
                        doilist.append(md['doi'])
    dump({"dois":doilist},open('process_2/results/merge/ordered_dois.json','w+'))
    print("DOI-List created.")

print("\n")


"""Specifies a method to load a specific chunk of fulltexts, the order given via a list of dois, from the grobid-directory.
Attributes:
    chunknumber: the i-part of the list which should be loaded. The list is split into 40 chunks by default.
    doilist: the list specifying the order of the dois in which texts should be loaded."""

def load_fullTexts(chunknumber,doilist):
    print(f"loading new FullTexts, chunk {chunknumber}")
    l = len(doilist)
    slices = 40
    start = chunknumber*int(l/slices)
    end = (chunknumber+1)*int(l/slices)
    if start > l:
        print("Startindex out of range")
        return None
    if end >= l:
        end = l
    print(f"Chunk: from {start} - {end}")
    doichunk = set(doilist[start:end])
    doi_to_text = pd.DataFrame()
    for i,p in enumerate(grobid_filepaths):
        if i%25 == 0: print(i)
        table = pq.read_table(p).to_pandas().set_index('doi')
        table = table[table.index.isin(doichunk)]
        doi_to_text = doi_to_text.append(table)
    print("New FullTexts loaded")
    return doi_to_text

"""Define method that goes through authorlist and finds Authors whose ids are listed more than once. Returns an authorlist where each author is included just once.
Attributes:
    authors: a list of authors, each author of the form {'id':String,'name':String}"""

def undupe_authors(authors):
    new_auth = []
    ids = []
    for a in authors:
        if not a['id'] in ids:
            ids.append(a['id'])
            new_auth.append(a)
        else:
            continue
    return new_auth

"""We open a new file. After writing 100.000 lines to that file, the file will be closed and a new file opened. Counters for potential conflicts while merging are initialized."""

filecount = 0
linecount = 0
n = "{0:0=2d}".format(filecount)
outfile = open(f"{output_directory}/batches/{n}","w+")
chunkcount = 0
doi_to_text = load_fullTexts(chunkcount,doilist)

differing_dois = 0
doi_conflict = []
differing_authors = 0
differing_publisher = 0
differing_citCount = 0

"""Script goes through all files of the CORE-set sequentially"""


print("Beginning merging.")
for path in core_filepaths:
    with open(path) as f:
        print(basename(path))
        for line in f:
            linecount += 1
            if linecount % 100000 == 0:
                outfile.close()
                filecount += 1
                n = "{0:0=2d}".format(filecount)
                outfile = open(f"{output_directory}/batches/{n}","w+")
            entry = loads(line)
                
            new_entry = {}
            new_entry['mag_ids'] = []
            
            
            """Script goes through all matches found for the current CORE entry. All Idsof the matches arr added to the new entry. If a match contains a doi, this is taken as the doi of the new entry."""
            
            for m in matches[entry['coreId']]:
                md = mag_dict[m]
                
                new_entry['mag_ids'] = new_entry['mag_ids'] + [md['id']]

                
                if not 'doi' in new_entry or not new_entry['doi']:
                    if 'doi' in md and md['doi']:
                        new_entry['doi'] = md['doi']
                        new_entry['doi_source'] = 'MOAG'
                        
                """The longest authorlist of the potential matches is chosen."""
                        
                if 'authors' in md:
                    a = undupe_authors(md['authors'])
                    if not 'authors' in new_entry or not new_entry['authors']:
                        new_entry['authors'] = a
                    elif len(a) > len(new_entry['authors']):
                        new_entry['authors'] = a
                        
                if 'title' in md and (not 'title' in new_entry or not new_entry['title']):
                    new_entry['title'] = md['title']
                    
                """For all other keys, data from all matches is combined to fill potential gaps in the first match"""
                        
                for e in ['venue','year','n_citation','page_start','page_end','doc_type','publisher','volume','issue']:
                    if e in md and md[e] and (not e in new_entry or not new_entry[e]):
                        new_entry[e] = md[e]
                        
            """If a key was not included in any of the matches, it is filled with a None value"""
                        
            for e in ['doi','doi_source','authors','title','venue','year','n_citation','page_start','page_end','doc_type','publisher','volume','issue']:
                if not e in new_entry:
                    new_entry[e] = None
                    
            new_entry['core_id'] = entry['coreId']
            
            
            """Conflicts between the CORE entry and the information from the MAG-matches is checked. If another DOI or a differing number of authors is found, the conflict is noted and written to the output file for later checkup whether the match was actually correct."""
                
                
            if 'doi' in entry and entry['doi'] and new_entry['doi'] and new_entry['doi'] != entry['doi']:
                differing_dois += 1
                doi_conflict.append((new_entry['doi'],entry['doi'],new_entry['title'],entry['title']))
            elif 'doi' in entry and entry['doi'] and not new_entry['doi']:
                new_entry['doi'] = entry['doi']
                new_entry['doi_source'] = 'CORE'

            if len(new_entry['authors']) != len(entry['authors']):
                differing_authors += 1
            
            """If the doi of the entry is among the dois for the grobid extracted texts, those are searched for the DOI. If the DOI is not found, the next chunk of full texts is loaded and the CORE fulltext is replaced by the grobid fulltext."""
            
            if new_entry['doi'] in grobid_dois:
                while(True):
                    try:
                        loc = doi_to_text.loc[new_entry['doi']]
                        break
                    except KeyError:
                        print(f"Not found: {new_entry['doi']}")
                        i = 0
                        for x in doilist:
                            if x == new_entry['doi']:
                                print(f"Element located at position {i}")
                            i+=1
                        print("Finished looking for  elements")
                        chunkcount += 1
                        del doi_to_text
                        doi_to_text = load_fullTexts(chunkcount,doilist)
                if len(loc) > 1:
                    new_entry['full_text'] = entry['fullText']
                    new_entry['full_text_source'] = ['CORE']
                else:
                    new_entry['full_text'] = loc['content']
                    new_entry['full_text_source'] = 'grobid'
            else:
                new_entry['full_text'] = entry['fullText']
                new_entry['full_text_source'] = 'CORE'
                
                
            """If the doi is present among the field of study dois, the respective fields are added to the new entry."""
                
            if new_entry['doi'] in fos_dois:
                fields = []
                fs = fos_df.loc[new_entry['doi']]['board']
                if isinstance(fs,list):
                    new_entry['fields_of_study'] = fs
                else:
                    for line in fs:
                        for f in line:
                            if not f in fields:
                                fields.append(f)
                    new_entry['fields_of_study'] = fields 
            else:
                new_entry['fields_of_study'] = []
                
                
            """Other datapoints from CORE are added. If potential conflicts with the MAG data might exist, those are checked and added to the output data for later."""

            
            for e in entry:
                if e in ["abstract","oai"]:
                    new_entry[e] = entry[e]
                elif e == 'enrichments':
                    if not new_entry['doc_type'] and 'documentType' in entry['enrichments'] and 'type' in entry['enrichments']['documentType']:
                        new_entry['doc_type'] = entry['enrichments']['documentType']['type']
                    if not new_entry['n_citation'] and 'citationCount' in entry['enrichments']:
                        new_entry['n_citation'] = entry['enrichments']['citationCount']
                    elif 'citationCount' in entry['enrichments'] and entry['enrichments']['citationCount']:
                        print('here')
                        if new_entry['n_citation'] != entry['enrichments']['citationCount']:
                            differing_citCount += 1
                        new_entry['n_citation'] = max(new_entry['n_citation'],entry['enrichments']['citationCount'])    
                elif e == 'publisher':
                    if not new_entry['publisher']:
                        new_entry['publisher'] = entry['publisher']
                    elif new_entry['publisher'] != entry['publisher']:
                        differing_publisher += 1
                elif e == 'downloadUrl':
                    new_entry['download_url'] = entry['downloadUrl']
                        
            """If any key is still missing from the entry, it is included with a None value"""
                        
            for e in ["abstract","oai","identifiers","download_url"]:
                if not e in new_entry:
                    new_entry[e] = None
                    
            """The new entry is written to the output file with a following newline"""
            
            
            dump(new_entry, outfile)
            outfile.write("\n")
            
            
"""After finishing, the last file is closed. A report with the number of occurring conflicts is written to the output directory."""            
outfile.close()
with open(f"{output_directory}/merge-output.txt","w+") as f:
    f.write("Merging successful.\n")
    f.write(f"Number of DOI conflicts: {differing_dois}\n")
    f.write(f"Number of clear author conflicts: {differing_authors}\n")
    f.write(f"Number of publisher conflicts: {differing_publisher}\n")
    f.write(f"Number of citation count conflicts: {differing_citCount}\n")
    
conflicts = pd.DataFrame(doi_conflict, columns=['mag_doi','core_doi','mag_title','core_title'])
conflicts.to_json(r'process_2/results/merge/doi_checkup.json')