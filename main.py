import json
import pandas as pd
import pymysql

class DB_Inserter:
    def __init__(self, path, db:pymysql.Connection) -> None:
        self.db = db
        with open(path) as f:
            self.conf = json.load(f)

    def do(self):
        for self.name, self.file_config in self.conf.items():
            self.generate_template()
            self.get_data_and_placeholders()
            self.insert()

    def generate_template(self):
        table_name = self.file_config['table']
        self.query = f'INSERT INTO {table_name} ('
        self.data_template = {}
        for column_n, column_v in self.file_config['columns'].items():
            self.query += f'{column_v["db_name"]},'

            if column_v.get('fk'):
                def get_fk(column):
                    cursor = db.cursor()
                    self.query = f'SELECT id FROM {column_v["fk"]["table"]} where {column_v["fk"]["column"]}=%s'
                    value = (column,)
                    cursor.execute(self.query, value)
                    id = cursor.fetchone()
                    cursor.close()
                    return id[0] if id else id
                self.data_template[column_n] = get_fk

            else:
                self.data_template[column_n] = lambda x:x

        defaults = self.file_config.get('defaults')
        if defaults:
            for i, (db_name, value) in enumerate(defaults.items()):
                self.data_template[-i] = value
                self.query += f'{db_name},'
        self.query = f'{self.query[:-1]})'

    def get_data_and_placeholders(self):
        df = pd.read_csv(self.name, delimiter=';')
        self.data = []
        self.place_holder = 'VALUES '

        for row in df.itertuples(name=None):
            self.place_holder += '('
            for column, func in self.data_template.items():
                column = int(column)
                self.place_holder += '%s,'

                if column <= 0:
                    self.data.append(func)
                    continue

                self.data.append(func(row[column]))

            self.place_holder = f'{self.place_holder[:-1]}),'

        self.place_holder = f'{self.place_holder[:-1]}'

    def insert(self):
        cursor = self.db.cursor()
        print(cursor.execute(f'{self.query} {self.place_holder}', self.data))
        cursor.close()
        db.commit()

if __name__ == "__main__":
    db = pymysql.connect(host='localhost', user='dolibarr', database='dolibarr', passwd="senha")
    dbi = DB_Inserter('test.json', db)
    dbi.do()
    db.close()
