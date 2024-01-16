#!/bin/python3
import sys
import json
import csv

from abc import ABC, abstractmethod

import pymysql

class Func(ABC):
    @abstractmethod
    def run(self, column):
        pass

class FK_getter(Func):
    def __init__(self, db:pymysql.Connection, table:str, column:str) -> None:
        self.db = db
        self.table = table
        self.column = column
        self.query = f'SELECT rowid FROM {table} WHERE {column} =%s'

    def run(self, column):
        print('executing query searching', column)
        cursor = self.db.cursor()
        value = (column,)
        cursor.execute(self.query, value)
        id = cursor.fetchone()
        cursor.close()
        print(id)
        return id[0] if id else id

class Basic_Field(Func):
    def run(self, column):
        return column

class DB_Inserter:
    def __init__(self, path, db:pymysql.Connection) -> None:
        self.db = db
        with open(path) as f:
            self.conf = json.load(f)

    def do(self):
        self.generate_template()
        self.get_data_and_placeholders()
        self.insert()

    def generate_template(self):
        table_name = self.conf['table']
        self.query = f'INSERT INTO {table_name} ('
        self.data_template = {}
        for column_n, column_v in self.conf['columns'].items():
            self.query += f'{column_v["db_name"]},'

            if column_v.get('fk'):
                get_fk = FK_getter(self.db, column_v["fk"]["table"], column_v["fk"]["column"])
                self.data_template[column_n] = (column_n, get_fk, column_v.get('null'))

            else:
                self.data_template[column_n] = (column_n, Basic_Field(), column_v.get('null'))

        defaults = self.conf.get('defaults')
        if defaults:
            for i, (db_name, value) in enumerate(defaults.items(),1):
                self.data_template[-i] = (-i, value, None)
                self.query += f'{db_name},'
        self.query = f'{self.query[:-1]})'

    def get_data_and_placeholders(self):
        df = csv.reader(open(0), delimiter=self.conf['delimiter'])
        
        # skip vertical offset
        if not (voffset:=self.conf.get('voffset')): voffset = 1
        for _ in range(voffset):
            next(df)

        self.data = []
        self.place_holder = 'VALUES '
        for row in df:
            row_place_holder = '('
            tmp_data=[]
            for column, func, null in self.data_template.values():
                column = int(column)
                row_place_holder += '%s,'
                # for default values
                if column < 0:
                    tmp_data.append(func)
                    continue

                if not row[column]:
                    if not null: break
                    tmp_data.append(None)
                    continue

                tmp_data.append(func.run(row[column]))
            # only add if there is no null
            else:
                row_place_holder = f'{row_place_holder[:-1]}),'
                self.place_holder += row_place_holder
                self.data += tmp_data

        self.place_holder = f'{self.place_holder[:-1]}'

    def insert(self):
        print(self.query)
        print(self.place_holder.count('('))
        cursor = self.db.cursor()
        print(cursor.execute(f'{self.query} {self.place_holder}', self.data))
        cursor.close()
        db.commit()

if __name__ == "__main__":
    if len(sys.argv) != 7: exit()
    _, config_file, user, passwd, host, port, database = sys.argv
    db = pymysql.connect(host=host, user=user, database=database, passwd=passwd, port=int(port))
    dbi = DB_Inserter(config_file, db)
    dbi.do()
    db.close()
