#!/usr/bin/env python3
import requests
from time import sleep
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
        r = requests.get(URI, headers=self.headers)
        retries = 0
        while r.status_code != 200:
            if r.status_code != 502:
                # ignores inserting data entirely and skips to next dataset
                return None
            sleep(10)
            r = requests.get(URI, headers=self.headers)
            retries += 1
            if retries > 15:
                print("failed request after " + str(retries + 1) + " retries")
                break
        return r.json()

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
                print("inserted all available data to table " + dataset)

            except json.JSONDecodeError:
                print("corrupt json: " + dataset + " \nmoving on to next file")
                pass

            except requests.ConnectionError as e:
                print("Could not connect to the host. Dataset: " + dataset)
                print(e)
                print("ignoring dataset")
                pass

            except requests.HTTPError as e:
                print("HTTP error. Dataset " + dataset)
                print(e)
                print("ignoring dataset")
                pass


if __name__ == '__main__':
    AC = APIConnector()
    AC.start_parse()
