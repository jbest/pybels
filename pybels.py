import requests
import csv

def bels(ID = None, continent = None, country = None, countrycode = None, stateprovince = None, county = None, locality = None, **kwargs):
    #print(kwargs)

    url = 'https://localityservice.uc.r.appspot.com/api/bestgeoref'

    location = dict(
        ID=ID,
        continent=continent,
        country=country,
        countrycode=countrycode,
        stateprovince=stateprovince,
        county=county,
        locality=locality
    )

    bels_request = dict(give_me='BEST_GEOREF', for_location=location)

    r = requests.post(url=url, json=bels_request)
    data = r.json()
    #print(data)
    return data

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

    bels_request = dict(give_me='BEST_GEOREF', for_location=location)

    r = requests.post(url=url, json=bels_request)
    data = r.json()
    #print(data)
    return data


"""
# Test data
ID = "1"
continent = "North America"
country = "United States"
countrycode = "US"
stateprovince = "Oklahoma"
county = "Cherokee"
locality = "5 mi NE of Tahlequah."

data = bels(
    ID=ID,
    continent=continent,
    country=county,
    countrycode=countrycode,
    stateprovince=stateprovince,
    county=county,
    locality=locality,
    other='Test')
print(data)
"""

with open('bels_sample_input.csv', newline='', encoding="utf-8-sig") as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        continent = row.get('continent')
        country = row.get('country')
        countrycode = row.get('countrycode')
        stateprovince = row.get('stateprovince')
        county = row.get('county')
        locality = row.get('locality')
        #print(continent, country, countrycode, stateprovince, county, locality)
        #print(row)
        #data = bels(row)
        data = bels_occur(occurrence=row)
        #data = bels(continent=continent, country=country, countrycode=countrycode, stateprovince=stateprovince, county=county, locality=locality)
        print(data)
        field_names = [
            'status',
            'elapsed_time',
            'id',
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

