# jcdl23-multi-authorship-corpus

## Steps for corpus recreation

This repo contains code used for the creation on the SmauC corpus.
To fully recreate the dataset, you will need a version of the [!CORE-dataset](https://core.ac.uk/), the [!Microsoft Open Academic Graph](https://www.microsoft.com/en-us/research/project/open-academic-graph/), and [!FastText language recognition model](https://fasttext.cc/docs/en/language-identification.html).

### Selection of english fulltexts

The CORE dataset has to be extracted from the compressed file and transformed into json-line files. The extraction_reader.py script then can be used to extract json-lines according to criteria specified in the `-cond`-argument of the script.
A path to a pretrained fasttext model has to be specified in the extraction_reader.py file. The conditions described in the paper were applied as:

`python3 extraction_reader.py -i <input_directory> -o <output_directory> --mode reduce --cond "(entry['language'] and entry['language']['code'] == 'en') or helpers.fast_text_lang(entry['fullText'],'en',5,1,fmodel)"`

### MAG matching

After filtering the CORE-set, the output of the extraction_reader and the Opaen Academic Graph File were used as inputs in the the notebook *mag_matching.ipynb*, that generates a matchtable of corresponding entries in both datasets.

Closer insights into the results of the mapping can be provided in *core_mag_mapping.ipynb*.

### Merge datasets

For merging the final dataset, we used the *mergescript.py*. This needs a path to a CORE and OAG-dataset and a matchtable. Our script additionally introduced some improved fullTexts extracted from PDFs at this point. Those cannot be included in this repository.
The script also uses a mapping of fields_of_study.

Afterwards, we filtered the merged entries with some additonal quality assurance for the fullTexts. For this, we used the extraction reader again with the command
`python3 extraction_reader.py -i <input_directory> -o <output_directory> --mode reduce --cond "len(utils.preprocessor(entry['full_text'])) > 2000 and utils.text_readable(entry['full_text'],'en',3,2,1,fmodel)"`

## Checkup scripts

The repository also included two notebooks which were used to check on results of the merging process and develop the merging criteria based on rare mismatches.
*author_merging.ipynb* was used to check matches and adapt the criteria used in *mag_matching.ipynb*.

## Creating figures and stats

Finally, to create figures and gain insights into the structure of the corpus, the merged json-line files were used as inputs for *corpus_description.ipynb* and *final_numbers.ibynb*. These notbeooks can be used to go through some additional corpus stats or can be adapted to conduct some basic analysis on your own.
