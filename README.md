## commands.py

This script creates 4 SQLite databases (q3a\_db.db, q3b\_db.db, q3c\_db.db, q3d\_db.db).
The file with the data being loaded into the databases is called '500000 Records.csv' (currently holding around 100 records or else it was too big to be added to the project).
It can be simply executed in the format:
```
python commands.py
```

The difference between the 4 databases being created is

#### q3a\_db.db
No index, and a page size of 4KB

#### q3b\_db.db
No index, and a page size of 16KB

#### q3c\_db.db
Unclustered Index on the 'Emp ID' column, and a page size of 4KB 

#### q3d\_db.db
Primary Index on the 'Emp ID' column, and a page size of 4KB

## queries\_3a.py, queries\_3b.py, queries\_3c.py, queries\_3d.py
These python scripts performs the search queries in the 4 databases created, traversing through the file depending on the different file formats created by the commands.py script.

They perform 3 queries: 

1. Scan for Last Name = 'Rowe', and return their employee id and full name.
2. Equality Search for employees with Emp ID = '181162'
3. Range Search for the employee id and full names of all the employees that have '171800' <= Emp ID <= '171899'

In addition to the return values specified above, the programs also prints the total number of pages read for every page type (header, data, index internal node, index leaf
node), as well as the average time to read one page.
