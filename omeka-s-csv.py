#!/usr/bin/env python3

"""omeka-s-csv.py

Dump metadata from the Omeka S API into CSV files.

Based on omekacsv.py for Omeka Classic, itself based on Caleb McDaniel's
original Python CSV file generator: https://github.com/wcaleb/omekadd
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import csv
import json
import math
import time

try:
    from urllib.parse import urlencode
    from urllib.request import urlopen
    from urllib.error import URLError, HTTPError
except ImportError:
    from urllib import urlencode
    from urllib2 import urlopen, URLError, HTTPError

try:
    input = raw_input
    str = unicode
    py2 = True
except NameError:
    py2 = False

def request(endpoint, resource, query={}):
    url = endpoint + "/" + resource
    if key_identity is not None and key_credential is not None:
        query["key_identity"] = key_identity
        query["key_credential"] = key_credential
    url += "?" + urlencode(query)

    response = urlopen(url)
    return response.info(), response.read()

def get_all_pages(endpoint, resource):
    data = []
    page = 1
    # default pages to 1 before we see the omeka-s-total-results header
    pages = 1
    while page <= pages:
        response, content = request(endpoint, resource, {'page': str(page)})
        content_list = json.loads(content)
        data.extend(content_list)

        # use first page to determine site's per_page setting
        if (page == 1):
            total = int(response['omeka-s-total-results'])
            page_length = len(content_list)
            if (page_length < total):
                pages = int(math.ceil(total/page_length))
            total_text = '\tTotal results: ' + str(total)
            if (pages > 1):
                total_text += ' (across ' + str(pages) + ' pages)'
            print(total_text)

        if (pages > 1):
            print('\tGot results page ' + str(page))

        page += 1
        time.sleep(1)
    return data

def is_internal_link(val):
    return type(val) is dict and len(val) == 2 and '@id' in val and 'o:id' in val
def is_metadata_property(val):
    return type(val) is list and type(val[0]) is dict and 'property_id' in val[0]
def is_date(val):
    return type(val) is dict and len(val) == 2 and '@value' in val and '@type' in val and val['@type'] == 'http://www.w3.org/2001/XMLSchema#dateTime'

endpoint = ''
while not endpoint:
    endpoint = input('Enter your Omeka API endpoint\n')
endpoint = endpoint.strip().rstrip('/')

key_identity = input('\nIf you are using an API key, please enter the key_identity now. Otherwise press Enter.\n')
if key_identity:
    key_credential = input('\nPlease input the key_credential\n')
    if not key_credential:
        key_identity = key_credential = None
else:
    key_identity = key_credential = None

multivalue_separator = input('\nEnter a character to separate mutiple values within a single cell.\nThis character must not be used anywhere in your actual data.\nLeave blank to use the default separator: |\n')
if not multivalue_separator:
    multivalue_separator = '|'

# get list of supported resources by this site
response, content = request(endpoint, 'api_resources')
available_resources = [resource['o:id'] for resource in json.loads(content)]

resources = ['items', 'item_sets', 'media']
for resource in resources:
    if (resource not in available_resources):
        continue

    print('\nExporting ' + resource)

    # get all pages
    data = get_all_pages(endpoint, resource)

    fields = []
    csv_rows = []

    for D in data:
        csv_row = {}

        for k, v in D.items():
            if k == '@context' or k == '@id' or k == 'o:resource_class':
                # skip json-ld context, id, resource class id
                continue
            if v is None:
                # skip nulls
                continue
            if type(v) is list and len(v) == 0:
                # skip empty arrays
                continue

            if k == '@type':
                resource_type = None
                if type(v) is list:
                    resource_type = v[1]
                csv_row['o:resource_class'] = resource_type
            elif k == 'thumbnail_display_urls':
                for thumbnail_type, url in v.items():
                    if url is not None:
                        csv_row['thumbnail_' + thumbnail_type] = url
            elif is_internal_link(v):
                # single internal links
                csv_row[k] = str(v['o:id'])
            elif type(v) is list and is_internal_link(v[0]):
                # multiple internal links
                csv_row[k] = multivalue_separator.join([str(res['o:id']) for res in v])
            elif is_date(v):
                csv_row[k] = v['@value']
            elif is_metadata_property(v):
                literals = []
                resources = []
                uris = []
                for value in v:
                    if '@value' in value:
                        literals.append(str(value['@value']))
                    elif 'value_resource_id' in value:
                        resources.append(str(value['value_resource_id']))
                    elif '@id' in value:
                        uris.append(value['@id'])
                if literals:
                    csv_row[k] = multivalue_separator.join(literals)
                if resources:
                    csv_row[k + '_resources'] = multivalue_separator.join(resources)
                if uris:
                    csv_row[k + '_uris'] = multivalue_separator.join(uris)
            elif type(v) is int or type(v) is float or type(v) is bool:
                csv_row[k] = str(v)
            elif isinstance(v, str):
                csv_row[k] = v

        for k in csv_row.keys():
            if k not in fields: fields.append(k)
        csv_rows.append(csv_row)

    fields = sorted(fields, key=lambda field: (field != 'id', field))
    filename = resource + '.csv'
    if (py2):
        o = open(filename, 'wb')
        c = csv.DictWriter(o, [f.encode('utf-8', 'replace') for f in fields], extrasaction='ignore')
        c.writeheader()
        for row in csv_rows:
            c.writerow({k:v.encode('utf-8', 'replace') for k,v in row.items() if isinstance(v, str)})
    else:
        o = open(filename, 'w', encoding='utf-8', newline='')
        c = csv.DictWriter(o, fields, extrasaction='ignore')
        c.writeheader()
        for row in csv_rows:
            c.writerow(row)
    o.close()
    print('File created: ' + filename)
