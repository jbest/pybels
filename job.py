#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

__author__ = "Marie-Elise Lecoq"
__contributors__ = "John Wieczorek"
__copyright__ = "Copyright 2022 Rauthiflor LLC"
__filename__ = 'job.py'
__version__ = __filename__ + ' ' + "2022-06-21T21:54-03:00"

import base64
import json
import csv
import io
import os
import tempfile
import logging
import re
import sys
import time
import sendgrid

from sendgrid.helpers.mail import Email, To, Bcc, Content, Mail
from contextlib import contextmanager
from google.cloud import bigquery
from google.cloud import storage

lib_path = os.path.abspath('./')
sys.path.append(lib_path)
lib_path = os.path.abspath('./bels')
sys.path.append(lib_path)

from id_utils import dwc_location_hash, location_match_str, super_simplify
from dwca_utils import safe_read_csv_row, lower_dict_keys
from bels_query import get_location_by_hashid, row_as_dict
from bels_query import get_best_sans_coords_georef_reduced
from bels_query import import_table
from bels_query import export_table
from bels_query import delete_table
from bels_query import process_import_table
from bels_query import bigquerify_header
from bels_query import country_fields
from dwca_vocab_utils import darwinize_dict
from dwca_vocab_utils import darwinize_list
from dwca_terms import locationmatchsanscoordstermlist

PROJECT_ID = 'localityservice'
OUTPUT_LOCATION = 'bels_output'

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
sg_api = sendgrid.SendGridAPIClient(SENDGRID_API_KEY)

def process_csv_in_bulk(event, context, bq_client, storage_client):
    """ Background Cloud Function to be triggered by Pub/Sub to georeference all the rows
        in a CSV file by uploading the file to Cloud Storage and then loading that into
        a BigQuery table for processing.
    Args:
        event (dict):  The dictionary with data specific to this type of
        event. The `data` field contains the PubsubMessage message. The
        `attributes` field will contain custom attributes if there are any.
        context (google.cloud.functions.Context): The Cloud Functions event
        metadata. The `event_id` field contains the Pub/Sub message ID. The
        `timestamp` field contains the publish time.
    """
    logging.basicConfig(level=logging.INFO)
    if bq_client is None:
        bq_client = bigquery.Client()
        
    if storage_client is None:
        storage_client = storage.Client()
        
    starttime = time.perf_counter()
    
    if 'data' not in event:
        raise ValueError('no data provided')

    config = base64.b64decode(event['data']).decode('utf-8')
    json_config_in = json.loads(config)
    logging.info(f'New job: {json_config_in}')
    json_config = json_config_in['data']

    # Google Cloud Storage location of input file
    upload_file_url = json_config['upload_file_url'] 

    # User-provided Output file name
    output_filename = json_config['output_filename'] 
    email = json_config['email']
    header = json_config['header']

    # Alter output file name to [filename]-[UUID of input file location].csv
    upload_file_parts = upload_file_url.split('/')
    file_suffix = upload_file_parts[len(upload_file_parts)-1].split('.')[0]

    # Don't allow any of the following characters in output file names, substitute '_'
    output_filename = re.sub(r'[ ~`!@#$%^&*()_+={\[}\]|\\:;"<,>?\'/]', '_', output_filename)
    output_filename = '%s-%s.csv.gz' % (output_filename, file_suffix)

    # Darwinize the header
    vocabpath = './bels/vocabularies/'
    dwccloudfile = vocabpath + 'darwin_cloud.txt'

    # Translate the fields in the header to lowercase Darwin Core standard field name.
    darwinized_header = darwinize_list(header, dwccloudfile, case='l')
    
    # Make sure the field names satisfy BigQuery field name requirements
    bigqueryized_header = bigquerify_header(darwinized_header)

    # Get the list of field names that can be used for countrycode interpretation
    countryfieldlist = country_fields(bigqueryized_header)
    if countryfieldlist is None:
        s = f'The file {upload_file_url} has no interpretable country field '
        s += f'(interpreted_countrycode, countrycode, v_countrycode, country) '
        s += f'in the header after processing:\n{bigqueryized_header}.'
        logging.error(s)
        send_failure_email(email, s, sg_api)
        return
    
    input_table_id = import_table(bq_client, upload_file_url, bigqueryized_header)

    # Do georeferencing on the imported table with SQL script
    # Pass the countryfieldlist so the bels_match_field can be set with distinct
    # SQL queries based on priority.
    output_table_id = process_import_table(bq_client, input_table_id, countryfieldlist)

    # Export results to Google Cloud Storage
    # Make this work for big files that get split
    destination_uri = f'gs://{PROJECT_ID}/{OUTPUT_LOCATION}/{output_filename}'
    outputfilelist = export_table(bq_client, output_table_id, destination_uri)

    bucket = storage_client.get_bucket(PROJECT_ID)

    output_url_list = []
    for file in outputfilelist:
        blob = bucket.blob(f'{file}')
        output_url_list.append(blob.public_url)
    output_url_list.sort()

    # Remove the input and output tables from BigQuery
    try:
        bq_client.get_table(input_table_id)
        delete_table(bq_client, input_table_id)
        try:
            bq_client.get_table(input_table_id)
        except Exception as e:
            logging.debug(f'Table {input_table_id} was deleted.')
    except Exception as e:
        logging.error(f'Table {input_table_id} does not exist.')

    try:
        bq_client.get_table(output_table_id)
        delete_table(bq_client, output_table_id)
        try:
            bq_client.get_table(output_table_id)
        except Exception as e:
            logging.debug(f'Table {output_table_id} was deleted.')
    except Exception as e:
        logging.error(f'Table {output_table_id} does not exist.')

    # Notify the receiving party by email given.
    try:
        send_email(email, output_url_list, sg_api)
    except Exception as e:
        logging.error(f'Error sending email: {e}. Files stored at {output_url_list}')

    elapsedtime = time.perf_counter()-starttime
    s = f'Success! Total elapsed time = {elapsedtime:1.3f}s\n'
    s += f'Output to {output_url_list} for job:\n'
    s += f'{json_config_in}'
    logging.info(s)

def find_best_georef(client, filename):
    # vocabpath is the location of vocabulary files to test with
    dirname = os.path.dirname(__file__)
    vocabpath = os.path.join(dirname, './vocabularies/')

    darwincloudfile = os.path.join(vocabpath, 'darwin_cloud.txt')
    
    listToCsv = []
    # logging.info('find_best_georef() filename: %s' % filename)
    for row in safe_read_csv_row(filename):
        # logging.info(f'row: {row}')
        rowdict = row_as_dict(row)

        loc = darwinize_dict(row_as_dict(row), darwincloudfile)
        lowerloc = lower_dict_keys(loc)

        sanscoordslocmatchstr = location_match_str(locationmatchsanscoordstermlist, \
            lowerloc)
        matchstr = super_simplify(sanscoordslocmatchstr)
        result = get_best_sans_coords_georef_reduced(client, matchstr)
        if result:
            for field in ['dwc_location_hash', 'locationid']:
                if field in result:
                    result[field] = base64.b64encode(result[field]).decode('utf-8')
            row.update(result)
        else:
            # Create a dict of empty results for all results fields anyway to make sure
            # the header is correct even if the first result is empty.
#             s = []
#             s.append(__version__)
#             s.append(f' no georef found for: {matchstr}'
#             print(''.join(s))
            pass
        listToCsv.append(row)
    return listToCsv

def create_output(occurrences):
    fieldnames = occurrences[0].keys()
    output_file = io.StringIO()
    dict_writer = csv.DictWriter(output_file, fieldnames)
    dict_writer.writeheader()
    dict_writer.writerows(occurrences)
    return output_file.getvalue()

def send_email(target, output_url_list, sg_api):
    if sg_api is None:
        logging.error('Error! No SendGrid API Client.')
        return
    from_email = Email("vertnetinfo@vertnet.org")
    to_email = To(target)
    subject = "File ready for download from Biodiversity Enhanced Location Services (BELS)"
    s = []
    s.append('A request to process a file uploaded to BELS to find the best existing ')
    s.append('georeferences and send the results to this email address has been ')
    s.append('completed. Following is a list of URLs where the results can be ')
    s.append('downloaded. If there is more than one file in the list, it is because ')
    s.append('the size of the result exceeded the size for a single file export. In ')
    s.append('this case only the first file on the list contains the column header. ')
    s.append('Files will remain accessible at this location for a period of at least ')
    s.append('30 days.\n')
    for url in output_url_list:
        s.append(f'{url}\n')
    content = Content("text/plain", ''.join(s))
    message = Mail(from_email, to_email, subject, content)

    sg_api.client.mail.send.post(request_body=message.get())

def send_failure_email(target, msg, sg_api):
    if sg_api is None:
        logging.error('Error! No SendGrid API Client.')
        return
    message = Mail()
    message.from_email = From('vertnetinfo@vertnet.org', 'BELS Georeference Matcher')
    message.to = To(target)
    message.bcc = Bcc('gtuco.btuco@gmail.com')
    message.subject = Subject("BELS - failure to process file")
    s = []
    s.append('A request to process a file with the BELS Georeference Matcher failed ')
    s.append('for the following reason:\n')
    s.append(msg)
    message.content = Content("text/plain", ''.join(s))
    sg_api.client.mail.send.post(request_body=message.get())

@contextmanager
def temp_file():
    _, name = tempfile.mkstemp()
    try:
        yield name
    finally:
        os.remove(name)
