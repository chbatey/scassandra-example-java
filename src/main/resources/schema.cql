CREATE KEYSPACE IF NOT EXISTS scassandra WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE if NOT EXISTS person(first_name text, last_name text, age int, interesting_dates set<timestamp>, primary KEY ((first_name, last_name ))) ;