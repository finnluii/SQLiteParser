##commands.py

This script creates 4 databases (q3a\_db.db, q3b\_db.db, q3c\_db.db, q3d\_db.db).
The file with the data being loaded into the databases is called '500000 Records.csv' (currently holding around 100 records or else it was too big to be added to the project).
It can be simply executed in the format:
```
python commands.py
```

The difference between the 4 databases being created is

####q3a_db.db
No index, and a page size of 4KB

####q3b_db.db
No index, and a page size of 16KB

####q3c_db.db
Unclustered Index on the 'Emp ID' column, and a page size of 4KB 

####q3d_db.db
Primary Index on the 'Emp ID' column, and a page size of 4KB

##queries_3a.py, queries_3b.py, queries_3c.py, queries_3d.py
These python scripts performs the search queries in the 4 databases created, traversing through the file depending on the different file formats created by the commands.py script.
They perform 3 queries: 
1. Scan
2. Equality Search
3. Range Search
