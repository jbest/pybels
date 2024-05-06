import requests
import csv
import os

#BELS
from id_utils import dwc_location_hash, location_match_str, super_simplify
from dwca_terms import locationmatchsanscoordstermlist
from bels_query import get_location_by_hashid, row_as_dict
from dwca_vocab_utils import darwinize_dict
from chardet import UniversalDetector
from dwca_utils import safe_read_csv_row, lower_dict_keys


def bels_occur(occurrence=None):
    #print(kwargs)

    url = 'https://localityservice.uc.r.appspot.com/api/bestgeoref'

    location = dict(
        ID=occurrence.get('id'),
        continent=occurrence.get('continent'),
        country=occurrence.get('country'),
        countrycode=occurrence.get('countrycode'),
        stateprovince=occurrence.get('stateprovince'),
        county=occurrence.get('county'),
        locality=occurrence.get('locality')
    )

    # generate matchstring locally
    dirname = os.path.dirname(__file__)
    vocabpath = os.path.join(dirname, './vocabularies/')
    darwincloudfile = os.path.join(vocabpath, 'darwin_cloud.txt')
    loc = darwinize_dict(row_as_dict(occurrence), darwincloudfile)
    lowerloc = lower_dict_keys(loc)
    sanscoordslocmatchstr = location_match_str(locationmatchsanscoordstermlist, lowerloc)
    matchstr = super_simplify(sanscoordslocmatchstr)
    print('Local matchstr', matchstr)

    # create BELS API request    
    bels_request = dict(give_me='BEST_GEOREF', for_location=location)

    r = requests.post(url=url, json=bels_request)
    data = r.json()
    #print(data)
    data['local_matchstr'] = matchstr
    return data, matchstr

def flatten_bels(response=None):
    # alternative might be to use pandas json_normalize
    if response:
        response_flat = {}
        response_flat['local_matchstr'] = response.get('local_matchstr', None)
        message = response.get('Message', None)
        if message:
            response_flat['status'] = message.get('status', None)
            response_flat['elapsed_time'] = message.get('elapsed_time', None)
            result = message.get('Result', None)
            if result:

                response_flat['ID'] = result.get('ID', None)
                response_flat['continent'] = result.get('continent', None)
                response_flat['country'] = result.get('country', None)
                response_flat['countrycode'] = result.get('countrycode', None)
                response_flat['stateprovince'] = result.get('stateprovince', None)
                response_flat['county'] = result.get('county', None)
                response_flat['locality'] = result.get('locality', None)
                response_flat['bels_countrycode'] = result.get('bels_countrycode', None)
                response_flat['bels_match_string'] = result.get('bels_match_string', None)
                response_flat['bels_decimallatitude'] = result.get('bels_decimallatitude', None)
                response_flat['bels_decimallongitude'] = result.get('bels_decimallongitude', None)
                response_flat['bels_geodeticdatum'] = result.get('bels_geodeticdatum', None)
                response_flat['bels_coordinateuncertaintyinmeters'] = result.get('bels_coordinateuncertaintyinmeters', None)
                response_flat['bels_georeferencedby'] = result.get('bels_georeferencedby', None)
                response_flat['bels_georeferenceddate'] = result.get('bels_georeferenceddate', None)
                response_flat['bels_georeferenceprotocol'] = result.get('bels_georeferenceprotocol', None)
                response_flat['bels_georeferencesources'] = result.get('bels_georeferencesources', None)
                response_flat['bels_georeferenceremarks'] = result.get('bels_georeferenceremarks', None)
                response_flat['bels_georeference_score'] = result.get('bels_georeference_score', None)
                response_flat['bels_georeference_source'] = result.get('bels_georeference_source', None)
                response_flat['bels_best_of_n_georeferences'] = result.get('bels_best_of_n_georeferences', None)
                response_flat['bels_match_type'] = result.get('bels_match_type', None)
            return response_flat
        else:
            return None
    else:
        return None

def main():
    field_names = [
        'local_matchstr',
        'status',
        'elapsed_time',
        'ID',
        'continent',
        'country',
        'countrycode',
        'stateprovince',
        'county',
        'locality',
        'bels_countrycode',
        'bels_match_string',
        'bels_decimallatitude',
        'bels_decimallongitude',
        'bels_geodeticdatum',
        'bels_coordinateuncertaintyinmeters',
        'bels_georeferencedby',
        'bels_georeferenceddate',
        'bels_georeferenceprotocol',
        'bels_georeferencesources',
        'bels_georeferenceremarks',
        'bels_georeference_score',
        'bels_georeference_source',
        'bels_best_of_n_georeferences',
        'bels_match_type'
        ]

    with open('bels_result.csv', 'w') as outputfile:
        writer = csv.DictWriter(outputfile, fieldnames=field_names)
        writer.writeheader()

        #bels_sample_input.csv
        #occurrences_02
        print('Reading occurrences')
        with open('occurrences.csv', newline='', encoding="utf-8-sig") as inputfile:
            reader = csv.DictReader(inputfile)
            for row in reader:
                print('Searching for dups for id:', row['id'])
                response, local_matchstr = bels_occur(occurrence=row)
                response_flat = flatten_bels(response=response)
                #print(response_flat)
                writer.writerow(response_flat)

if __name__ == '__main__':
    main()
