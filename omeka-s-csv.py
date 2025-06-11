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
from collections import defaultdict
import argparse
import csv
import json
import math
import time
import sys

try:
    from urllib.parse import urlencode
    from urllib.parse import parse_qsl
    from urllib.request import urlopen
    from urllib.error import URLError, HTTPError
except ImportError:
    from urllib import urlencode
    from urllib2 import urlopen, URLError, HTTPError
    from urlparse import parse_qsl

try:
    input = raw_input
    str = unicode
    py2 = True
except NameError:
    py2 = False

def request(endpoint, resource, query):
    url = endpoint + "/" + resource
    url += "?" + urlencode(query)

    response = urlopen(url)
    return response.info(), response.read()

def get_all_pages(endpoint, resource, query):
    data = []
    page = 1
    # default pages to 1 before we see the omeka-s-total-results header
    pages = 1
    while page <= pages:
        query['page'] = str(page)
        response, content = request(endpoint, resource, query)
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

def get_value_data(value):
    if '@value' in value:
        return 'literal', str(value['@value'])
    if 'value_resource_id' in value:
        return 'resource', str(value['value_resource_id'])
    if '@id' in value:
        return 'uri', value['@id']
    return None, None

def is_internal_link(val):
    return type(val) is dict and len(val) == 2 and '@id' in val and 'o:id' in val
def is_metadata_property(val):
    return type(val) is list and type(val[0]) is dict and 'property_id' in val[0]
def is_date(val):
    return type(val) is dict and len(val) == 2 and '@value' in val and '@type' in val and val['@type'] == 'http://www.w3.org/2001/XMLSchema#dateTime'

def export(endpoint, resource, query, multivalue_separator, filename=None):
    print('\nExporting ' + resource)

    # get all pages
    data = get_all_pages(endpoint, resource, query)

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
                values_by_type = {
                    'literal': [],
                    'resource': [],
                    'uri': [],
                }
                value_type_suffixes = {
                    'literal': '',
                    'resource': '_resources',
                    'uri': '_uris',
                }
                # infinite defaultdict
                def dd():
                    return defaultdict(dd)
                annotation_texts = dd()
                for value in v:
                    value_type, value_val = get_value_data(value)

                    if not value_type:
                        continue;

                    values_by_type[value_type].append(value_val)
                    value_index = len(values_by_type[value_type]) - 1

                    if '@annotation' in value:
                        for annotation_property, annotations in value['@annotation'].items():
                            for annotation_index, annotation in enumerate(annotations):
                                annotation_type, annotation_val = get_value_data(annotation)
                                if not annotation_type:
                                    continue;

                                annotation_texts[value_type][annotation_property][annotation_type][annotation_index][value_index] = annotation_val

                for value_type, values in values_by_type.items():
                    csv_row[k + value_type_suffixes[value_type]] = multivalue_separator.join(values)

                for anno_value_type, anno_props in annotation_texts.items():
                    for anno_prop, anno_types in anno_props.items():
                        for anno_type, anno_indexes in anno_types.items():
                            for anno_index, anno_value_indexes in anno_indexes.items():
                                anno_values = [''] * len(values_by_type[anno_value_type])
                                for anno_value_index, anno_val in anno_value_indexes.items():
                                    anno_values[anno_value_index] = anno_val
                                anno_csv_key = k + value_type_suffixes[anno_value_type] + '_annotation_' + anno_prop + value_type_suffixes[anno_type]
                                if (anno_index > 0):
                                    anno_csv_key += '_' + str(anno_index + 1)
                                csv_row[anno_csv_key] = multivalue_separator.join(anno_values)
            elif type(v) is int or type(v) is float or type(v) is bool:
                csv_row[k] = str(v)
            elif isinstance(v, str):
                csv_row[k] = v

        for k in csv_row.keys():
            if k not in fields: fields.append(k)
        csv_rows.append(csv_row)

    fields = sorted(fields, key=lambda field: (field != 'id', field))

    if filename is None:
        filename = resource + '.csv'

    if py2:
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

def main():
    resources = ['items', 'item_sets', 'media', 'collecting_items']
    resource_error = False
    query = {}

    if (len(sys.argv) > 1):
        parser = argparse.ArgumentParser(
            prog='omeka-s-csv.py',
            description='Export to CSV from an Omeka S site using the REST API.\n\nRunning with no arguments will prompt the user for the necessary settings.')
        parser.add_argument('endpoint', help='API URL for the Omeka S site to export from')
        parser.add_argument('--resource', help='resource to export (default is to export items, item_sets, media, and collecting_items)')
        parser.add_argument('--query', help='query string to pass to the API (requires --resource)')
        parser.add_argument('--multivalue-separator', default='|', help='character to separate multiple values in a cell (default: %(default)s)')
        parser.add_argument('--filename', help='output path for CSV file (requires --resource, default is <resource>.csv in current directory)')
        parser.add_argument('--key-identity', help='key identity for authenticated API access (requires --key-credential)')
        parser.add_argument('--key-credential', help='key credential for authenticated API access (requires --key-identity)')
        args = parser.parse_args()

        if args.resource is None:
            if args.query is not None:
                print('Error: --resource is required when using --query')
                sys.exit(1)
            if args.filename is not None:
                print('Error: --resource is required when using --filename')
                sys.exit(1)

        if bool(args.key_identity) is not bool(args.key_credential):
            print('Error: --key-identity and --key-credential must be used together')
            sys.exit(1)

        endpoint = args.endpoint
        multivalue_separator = args.multivalue_separator
        filename = args.filename
        key_identity = args.key_identity
        key_credential = args.key_credential

        if args.resource:
            resources = [args.resource]
            resource_error = True

        if args.query:
            query = dict(parse_qsl(args.query))
            query.pop('page', None)
            query.pop('limit', None)
            query.pop('offset', None)
            query.pop('format', None)
    else:
        filename = None

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
    response, content = request(endpoint, 'api_resources', {})
    available_resources = [resource['o:id'] for resource in json.loads(content)]

    if key_identity is not None and key_credential is not None:
        query["key_identity"] = key_identity
        query["key_credential"] = key_credential

    for resource in resources:
        if resource not in available_resources:
            if resource_error:
                print('Error: the site does not support the requested resource')
                sys.exit(1)
            else:
                continue
        export(endpoint, resource, query, multivalue_separator, filename)

if __name__ == '__main__':
    main()
