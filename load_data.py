import requests
from urllib.request import urlopen
import database_connector
import json

class APIConnector(object):

    def __init__(self):
        self.resources_url = 'http://api.vipunen.fi/api/resources'

    def get_metadata(self, content):
        metadata = requests.get(self.resources_url + '/' + content)
        return metadata.json()

    def get_data(self, content):
        URI = self.resources_url + '/' + content + '/data'
        return urlopen(URI)


    def get_resources(self):
        resources = requests.get(self.resources_url)
        return resources.json()

    def start_parse(self):
        for x in self.get_resources():
            try:
                meta = self.get_metadata(x)
                data = self.get_data(x)
                connector = database_connector.DatabaseConnector(x, data, meta)
                connector.create_schema()
                connector.insert_data()

            except json.JSONDecodeError:
                print("corrupt json: " + x + " \nmoving on to next file")
                pass


if __name__ == '__main__':
    AC = APIConnector()
    AC.start_parse()


