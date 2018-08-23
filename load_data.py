#!/usr/bin/env python3
import requests
from urllib.error import HTTPError
import database_connector
import json


class APIConnector(object):

    def __init__(self):
        self.resources_url = 'http://api.vipunen.fi/api/resources'
        self.headers = {'Accept': 'application/json'}

    def get_metadata(self, content):
        metadata = requests.get(self.resources_url + '/' + content, headers=self.headers)
        return metadata.json()

    def get_data(self, content, offset, limit):
        URI = self.resources_url + '/' + content + '/data?' + 'offset=' + str(offset) + '&limit=' + str(limit)
        data = requests.get(URI, headers=self.headers)
        return data.json()

    def get_resources(self):
        resources = requests.get(self.resources_url, headers=self.headers)
        return resources.json()

    def start_parse(self):
        for dataset in self.get_resources():
            try:
                meta = self.get_metadata(dataset)
                connector = database_connector.DatabaseConnector(dataset, meta)
                connector.create_schema()
                offset, limit = 0, 10000
                data = self.get_data(dataset, offset, limit)
                while data:
                    connector.insert_data(data, offset)
                    offset += limit
                    data = self.get_data(dataset, offset, limit)
                print("inserted all data to table " + dataset)

            except json.JSONDecodeError:
                print("corrupt json: " + x + " \nmoving on to next file")
                pass

            except HTTPError as e:
                print("got error response" + e.msg)
                pass


if __name__ == '__main__':
    AC = APIConnector()
    AC.start_parse()
