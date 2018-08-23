#!/usr/bin/env python3
import sqlite3
from decimal import Decimal


class DatabaseConnector(object):

    def __init__(self, name, meta):
        self.name = name
        self.meta = meta
        self.variables = []
        self.database = 'vipunen.db'
        self.connection = sqlite3.connect(self.database)
        self.cursor = self.connection.cursor()

    def create_schema(self):
        # apparently parameter substitution is not available for creating tables
        # so this part might and probably is slightly unsafe. It's all created in a static database though.
        sql_cmd = ("""CREATE TABLE IF NOT EXISTS {} (uid INTEGER PRIMARY KEY, """.format(self.name))
        self.variables.append('?')
        attributes = []
        replace_list = [(':', '_'), ('-', '_'), (',','_'), ('(', '_'), (')', '_'), ('.', '_')]
        for column in self.meta:
            name = column['name']
            for r in replace_list:
                name = name.replace(*r)
            type_ = column['type']
            # there are some really interesting number formats
            # in some of the files. Feel free to decipher those formats
            # and deal with them accordingly. Vipunen doesn't give any more
            # metadata other than "number"
            if type_ == 'number':
                type_ = 'NUMERIC'
            else:
                type_ = 'TEXT'
            attributes.append(name + ' ' + type_)
            self.variables.append('?')

        attributes_ = ', '.join(attributes)
        sql_cmd += """{});""".format(attributes_)
        self.cursor.execute(sql_cmd)
        self.connection.commit()
        print("created table " + self.name)

    def insert_data(self, data, offset):
        variables = ','.join(self.variables)
        # result could look like e.g.
        # 'INSERT INTO avoin_yliopisto VALUES (?, ?, ?, ?, ?, ?)'
        insert_cmd = 'INSERT OR IGNORE INTO {} VALUES ({});'.format(self.name, variables)
        values = []
        for row in data:
            values.append(offset)
            offset += 1
            for key in row:
                value = row[key]
                if type(value) == Decimal:
                    value = float(value)
                values.append(value)
            self.cursor.execute(insert_cmd, values)
            values = []
        self.connection.commit()
