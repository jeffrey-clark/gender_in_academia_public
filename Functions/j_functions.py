import json, re, io, os, sys, time, math
import numpy as np

## These are the function that I intend to re-use in future projects ##

# encode so that numpy doesnt kill JSON
class Npencoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, np.bool_):
            return bool(obj)
        else:
            return super(Npencoder, self).default(obj)



class Counter:
    def __init__(self):
        self.dic = {}
        self.dic_percent = {}
        self.total = 0
        self.ref = {}


    def add(self, value, reference=None):
        try:
            self.dic[value] += 1
        except:
            self.dic[value] = 1

        if reference != None:
            try:
                self.ref[value].append(reference)
            except:
                self.ref[value] = [reference]

        self.total += 1

    def compute(self):
        # store the count of number of keys
        self.q = len(self.dic)
        # sort the dictionary by values (this is a list of tuples)
        self.ordered = sorted(self.dic.items(), key=lambda x: x[1], reverse=True)
        # convert the list of tuples back to dictionary
        self.dic = dict(self.ordered)

        # update the percentage dictionary
        for key in list(self.dic.keys()):
            val = self.dic[key]
            percentage = round((100 * val / self.total), 2)
            self.dic_percent[key] = percentage

    def distribution(self):
        output = {}
        for key in list(self.dic.keys()):
            output[key] = str(self.dic[key]) + " (" + str(self.dic_percent[key]) + "%)"
        return output


# progress bar
def progress_bar(i, N):
    if isinstance(N, list):
        index = N.index(i) + 1
        total = len(N)
    elif isinstance(N, int):
        index = i
        total = N

    progress = round(100*index/total)
    no_of_dashes = math.floor(progress/5)
    no_of_blanks = 20 - no_of_dashes

    beg = "|" + "-"*no_of_dashes
    end = " " *no_of_blanks + "| " + str(progress) + "%"

    sys.stdout.write('\r'+beg+end)
    sys.stdout.flush()

    if total == index:
        print("\n")





# this function checks if a string is empty, 0, Nan, None, etc.
def check_none(string):
    #values = ["0", "Nan", "None", "", " "]
    values = ["0", "None", "", " "]
    if str(string) in values:
        return True
    else:
        return False

# this function returns the delimiter of a string
def find_delim(string, extensive=False):
    possible_delimiters = [",", ";"]
    #ranking_dic = {}
    ranking = Counter()
    for x in possible_delimiters:
        for char in str(string):
            if char == x:
                ranking.add(x)

    ranking.compute()
    # the number of delimters found
    found_delimiters = ranking.q

    if found_delimiters == 0:
        if extensive:
            return [None, found_delimiters, ranking.dic]
        else:
            return None
    else:
        max = list(ranking.dic.keys())[0]
        if extensive:
            return [max, found_delimiters, ranking.dic]
        else:
            return max

    # Extensive form returns [Identified delimiter, number of found delimiters, sorted dictionary]
    #                   e.g. [";", 2, [(';', 4), (',', 1)]]
    # Simple form returns [Identified delimiter]
    #                   e.g. [";"]


def clean_str_list(list):
    output = []
    for x in list:
        x = str(x).strip()
        if x != "":
            output.append(x)
    output = sorted(output)
    return output

# reorders a dictionary putting the key_list keys first in
# the key_list order, followed by rest in inherited order.
def part_reorder_dic(dic, key_list):
    all_keys = dic.keys()
    new_dic = {}
    for key in key_list:
        try:
            new_dic[key] = dic[key]
        except:
            pass
    for key in all_keys:
        if key not in key_list:
            new_dic[key] = dic[key]
    return new_dic


def part_reorder_list(list, preferred_list):
    output = []
    for p in preferred_list:
        if p in list:
            output.append(p)
    for i in list:
        if i in output:
            pass
        else:
            output.append(i)
    return output


def object_atts_to_dic(obj):
    atts = []
    for a in vars(obj):
        if a[0:2] != "__":
            atts.append(a)
    output = {}
    for a in atts:
        output[a] = getattr(obj, a)

    return output


def dic_to_tuples(dic):
    output = []
    for key in dic.keys():
        output.append((key, dic[key]))
    return output

def object_atts_to_tuples(obj, preferred_order):
    dic = object_atts_to_dic(obj)
    try:
        dic = part_reorder_dic(dic,preferred_order)
    except:
        pass
    return dic_to_tuples(dic)

def unique(input_list):
    output = []
    reduced = list(set(input_list))
    for li in input_list:
        if li in reduced:
            output.append(li)
            reduced.remove(li)
    return output



def json_obj(obj, preferred_order=None):
    # convert object to dictionary of attributes
    dic = object_atts_to_dic(obj)
    # partial reordering of the dictionary
    if preferred_order != None:
        dic = part_reorder_dic(dic, preferred_order)
    # convert dictionary to JSON
    string = json.dumps(dic, ensure_ascii=False, cls=Npencoder)
    return string + "\n"

def expand_str_list(list):
    output = []
    for i in list:
        output = output + i.split(" ")
    return output


def only_letters(text):
    #remove dashes, spaces and slashes, then lowercase
    string = re.sub('[\-\s\\\/]', '', text).lower()
    # remove special characters
    special_chars = {'a': ['á', 'à', 'ä', 'å'],
                     'c': ['č'],
                     'e': ['é'],
                     'i': ['í', 'ı', 'í', 'ì,' 'ĭ', 'î', 'ǐ', 'ï', 'ḯ', 'ĩ', 'į', 'ī', 'ỉ', 'ȉ', 'ȋ', 'ị'],
                     'o': ['ó', 'ø', 'ö'],
                     's': ['ś', 'š', 'ş', 'ș', 'š'],
                     'u': ['ú', 'ü'],
                     'n': ['ñ']}
    for x in special_chars:
        for c in special_chars[x]:
            while True:
                if c in string:
                    string = re.sub(c, x, string)
                else:
                    break

    return string