# csv-to-db
a script to easily import various csv files into a database

# USAGE:
1. replace the values in example.json with the wanted configuration

2. pipe the csv to the script with these arguments
```sh 
cat somecsv.csv | . /main.py example.json db_user db_passwd db_address db_port db_database
```

