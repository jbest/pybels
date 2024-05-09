"""
Paring down the processing by assuming that the input is DwC compatible
Consolidating functions into one file

"""


import csv
import os

#BELS
from id_utils import dwc_location_hash, location_match_str, super_simplify
from dwca_terms import locationmatchsanscoordstermlist
from bels_query import get_location_by_hashid, row_as_dict
from dwca_vocab_utils import darwinize_dict
from chardet import UniversalDetector
from dwca_utils import safe_read_csv_row, lower_dict_keys

def bels_simplify(occurrence=None):

    location = dict(
        ID=occurrence.get('id'),
        continent=occurrence.get('continent'),
        country=occurrence.get('country'),
        countrycode=occurrence.get('countrycode'),
        stateprovince=occurrence.get('stateprovince'),
        county=occurrence.get('county'),
        locality=occurrence.get('locality')
    )

    dirname = os.path.dirname(__file__)
    vocabpath = os.path.join(dirname, './vocabularies/')
    darwincloudfile = os.path.join(vocabpath, 'darwin_cloud.txt')
    row_dict = {}
    for item in list(occurrence.items()):
        row_dict[item[0]]=item[1]

    """
    #loc = darwinize_dict(row_as_dict(occurrence), darwincloudfile)
    """
    #lowerloc = lower_dict_keys(loc)
    #just assume no changes are needed to keys
    lowerloc = lower_dict_keys(row_dict)
    sanscoordslocmatchstr = location_match_str(locationmatchsanscoordstermlist, lowerloc)
    matchstr = super_simplify(sanscoordslocmatchstr)
    return matchstr

with open('bels_sample_input.csv', newline='', encoding="utf-8-sig") as inputfile:
    reader = csv.DictReader(inputfile)
    for row in reader:
        print('Searching for dups for id:', row['id'])
        response = bels_simplify(occurrence=row)
        print(response)



