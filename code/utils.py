from math import ceil
import fasttext
import requests
import re
from Levenshtein import distance as levenshtein_distance
from time import sleep

def doi2title(doi):
    """
    Return a title extracted from metadata for a given DOI.
    """
    while not (doi[-1].isdigit() or doi[-1].isalpha()):
        doi = doi[:-1]
    url = f"https://api.crossref.org/works/{doi}"

    headers = {"User-Agent": "python-requests/2.25.1 (mailto:philipp.sauer@uni-leipzig.de) student_research_project"}
    try:
        r = requests.get(url, headers = headers)
    except ConnectionError as e:
        print()
        print(e)
        sleep(2)
        try: 
            r = requests.get(url, headers = headers)
        except:
            print("retry Failed")
            sleep(2)
            return None
    except:
        print("some other error occurred")
        try: 
            r = requests.get(url, headers = headers)
        except:
            print("retry Failed")
            sleep(10)
            return None
    if(r.status_code == 200):
        try:
            j = r.json()
            if 'message' in j and 'title' in j['message'] and len(j['message']['title']) > 0:
                return j['message']['title'][0]
            else:
                return None
        except:
            return None
    else:
        return str(r.status_code)
        
def levenshtein_compare(t1, t2):
    if not t1 or not t2:
        return False
    return (levenshtein_distance(t1.lower(), t2.lower()) < abs(len(t1)-len(t2)) + 0.1*min(len(t1),len(t2)) and not ((len(t1) < 6 or len(t2) < 6) and abs(len(t1)-len(t2)) > 6))

fasttext.FastText.eprint = lambda x: None

def fast_text_lang(text, tar_lang, slices, conf, fmodel=None):
    """
    Splits a given text in a number of parts given in 'slices' parameter. 
    Then performs a language detection using fasttext package, either by a given model fmodel or a default model.
    Checks whether each part is the language described via language code in 'tar_lang' parameter ('en' for english e.g.).
    Return True if at least one slice was recognized as the tar_lang, and if a number of slices smaller or equal to 'conf'
    was recognized as any other language.
    """
    if not fmodel:
        path_to_pretrained_model = 'models/lid.176.ftz'
        fmodel = fasttext.load_model(path_to_pretrained_model)
        #print("No model passed")
    text = text.replace('\n',' ').replace('\uffff','')
    if(len(text) < slices):
        return False
    n = ceil(len(text)/slices)
    parts = [text[i:i+n] for i in range(0, len(text), n)]
    test = []
    result = []
    for i in range(slices):
        try:
            r = fmodel.predict(parts[i])
            test.append(r[0][0] == f"__label__{tar_lang}")
            result.append(r)
        except:
            test.append(None)
            result.append(None)
            if len(parts) <= i:
                break
    if test.count(False) > conf or test.count(True) == 0:
        print(test)
        print(result)
        return False
    else:
        print(test)
        print(result)
        return True
    
def fast_text_lang_words(text, tar_lang, slices, conf, fmodel=None):
    """
    Splits a given text in a number of parts given in 'slices' parameter. 
    Then performs a language detection using fasttext package, either by a given model fmodel or a default model.
    Checks whether each part is the language described via language code in 'tar_lang' parameter ('en' for english e.g.).
    Return True if at least one slice was recognized as the tar_lang, and if a number of slices smaller or equal to 'conf'
    was recognized as any other language.
    """
    if not fmodel:
        path_to_pretrained_model = 'models/lid.176.ftz'
        fmodel = fasttext.load_model(path_to_pretrained_model)
        #print("No model passed")
    text = text.replace('\n',' ').replace('\uffff','').split()
    if(len(text) < slices):
        return False
    n = ceil(len(text)/slices)
    parts = [text[i:i+n] for i in range(0, len(text), n)]
    test = []
    for i in range(slices):
        try:
            test.append(fmodel.predict(' '.join(parts[i]))[0][0] == f"__label__{tar_lang}")
        except:
            test.append(None)
            if len(parts) <= i:
                break
    if test.count(False) > conf or test.count(True) == 0:
        #print(test)
        return False
    else:
        #print(test)
        return True
    
def textparts(text):
    alpha = 0
    digit = 0
    space = 0
    other = 0
    for c in text:
        if c.isalpha():
            alpha += 1
        elif c.isdigit():
            digit += 1
        elif c.isspace():
            space += 1
        else:
            other += 1
    return (alpha,digit,space,other)

def ft_lang_detect(text, tar_lang, slices, max_false=1, min_true=1, fmodel=None):
    """
    Splits a given text in a number of parts given in 'slices' parameter. 
    Then performs a language detection using fasttext package, either by a given model fmodel or a default model.
    Checks whether each part is the language described via language code in 'tar_lang' parameter ('en' for english e.g.).
    Return True if at least one slice was recognized as the tar_lang, and if a number of slices smaller or equal to 'conf'
    was recognized as any other language.
    """
    if not fmodel:
        path_to_pretrained_model = 'models/lid.176.ftz'
        fmodel = fasttext.load_model(path_to_pretrained_model)
    text = text.replace('\n',' ').replace('\uffff','')
    if(len(text) < slices):
        return False
    n = ceil(len(text)/slices)
    parts = [text[i:i+n] for i in range(0, len(text), n)]
    test = []
    result = []
    for i in range(slices):
        try:
            r = fmodel.predict(parts[i])
            if(r[1][0] > 0.5):
                test.append(r[0][0] == f"__label__{tar_lang}")
            else:
                test.append(None)
            result.append(r)
        except:
            test.append(None)
            result.append(None)
            if len(parts) <= i:
                break
    if test.count(False) > max_false or test.count(True) < min_true:
        return False
    else:
        return True



def preprocessor(text):
    if isinstance((text), (str)):
        text = re.sub('<[^>]*>', '', text)
        text = re.sub('[^a-zA-Z_ !.,;:?]', '', text.lower())
        text = re.sub('\s+', ' ', text)
        return text
    else:
        pass

def text_readable(text, tar_lang, slices=1, min_true=1, max_false=0, fmodel=None, print_command=False):
    """
    Splits a given text in a number of parts given in 'slices' parameter. 
    Then performs a language detection using fasttext package, either by a given model fmodel or a default model.
    Checks whether each part is the language described via language code in 'tar_lang' parameter ('en' for english e.g.).
    Return True if at least one slice was recognized as the tar_lang, and if a number of slices smaller or equal to 'conf'
    was recognized as any other language.
    """
    if not fmodel:
        path_to_pretrained_model = 'models/lid.176.ftz'
        fmodel = fasttext.load_model(path_to_pretrained_model)
        #print("No model passed")
    text = preprocessor(text)
    if(len(text) < slices):
        return False
    n = ceil(len(text)/slices)
    parts = [text[i:i+n] for i in range(0, len(text), n)]
    predictions = []
    check = []
    for i in range(slices):
        try:
            pred = fmodel.predict(parts[i])
            predictions.append(pred)
            if pred[0][0] == f"__label__{tar_lang}" and pred[1][0] > 0.6:
                check.append(True)
            else:
                check.append(False)
        except:
            check.append(None)
            if len(parts) <= i:
                break
    if print_command:
        print(check)
        print(predictions)
    if check.count(False) > max_false or check.count(True) < min_true:
        return False
    else:
        return True
    
def author_dupes(authors):
    found = []
    for i in authors:
        if i['id'] in found:
            return True
        else:
            found.append(i['id'])
    return False