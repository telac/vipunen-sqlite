import sqlite3

class DatabaseConnector(object):

    def __init__(self, name, data, meta):
        self.name = name
        self.data = data
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
        # well this is pretty dumb.
        replace_list = [(':', '_'), ('-', '_'), (',','_'), ('(', '_'), (')', '_')]
        for column in self.meta:
            name = column['name']
            for r in replace_list:
                name = name.replace(*r)
            type_ = column['type']
            if type_ == 'number':
                type_ = 'INTEGER'
            else:
                type_ = 'TEXT'
            attributes.append(name + ' ' + type_)
            self.variables.append('?')

        attributes_ = ', '.join(attributes)
        sql_cmd += """{});""".format(attributes_)
        print(sql_cmd)
        self.cursor.execute(sql_cmd)
        self.connection.commit()

    def insert_data(self):
        variables = ','.join(self.variables)
        # result could look like e.g.
        # 'INSERT INTO avoin_yliopisto VALUES (?, ?, ?, ?, ?, ?)'
        insert_cmd = 'INSERT INTO {} VALUES ({});'.format(self.name, variables)
        print(insert_cmd)
        values = []
        for index, row in enumerate(self.data):
            values.append(index)
            for key in row:
                values.append(row[key])
            self.cursor.execute(insert_cmd, values)
            values = []
        self.connection.commit()

