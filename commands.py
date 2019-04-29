import sqlite3
import csv
import os
from collections import OrderedDict

#################### Input info ####################
csv_filename = "500000 Records.csv"
table_name = "Employee"

# Choose how many columns to store since not all columns are needed for this report
col_limit = 5

# Databases
q3a_db = "q3a_db.db"
q3b_db = "q3b_db.db"
q3c_db = "q3c_db.db"
q3d_db = "q3d_db.db"
####################################################

################# Helper Functions #################
def create_database(file_path, page_size):
	'''
	INPUT: file_path: file path to the database to create
	OUTPUT: cursor to database
	'''
	try: 
		os.remove(file_path)
	except:
		print(file_path + " was not deleted.")

	conn = sqlite3.connect(file_path)
	c = conn.cursor()

	# Set page size = 4KB
	c.execute('PRAGMA page_size = {size}'.format(size=page_size))

	return (conn, c)

def commit_and_close(conn):
	'''
	INPUT: conn: database connection
	'''
	conn.commit()
	conn.close()

def reformat_name(s):
	'''
	INPUT: s: a string that may contain characters such as a space or apostrophe('')
	OUTPUT: String with ' ' and '/'' replaced with underscore '_'
	'''
	reformat = s.replace(" ", "_")
	reformat = reformat.replace("\'", "_")

	return reformat

def pad_string(s, l):
	'''
	INPUT: s: string to be padded with spaces (if necessary)
		l: desired length of string
	OUTPUT: padded string s to desired length
	'''
	return s.ljust(l)

def insert_data(db_file, csv_file, table_name, column_names, max_count):
	'''
	INPUT: db_file: db file to insert contents 
		csv_file: csv file that stores data to be inserted into db_file
	OUTPUT: None, but a database with unique indexes will be created (even if
		db is not indexed)
	'''
	# Set to keep track of indexes seen before
	seen = set()

	conn = sqlite3.connect(db_file)
	c = conn.cursor()

	with open(csv_file) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=',')

		# Skip column names
		next(csv_reader)
		for row in csv_reader:
			if (row[0] not in seen):
				# add Emp_ID to set seen
				seen.add(row[0])
				col = ""
				val = ""
				index = 0
				for content in row[0:col_limit]:
					col += "{col_name}, ".format(col_name=(reformat_name(column_names[index])))
					# only pad if not Emp_ID:
					if (index != 0):
						# print(pad_string(reformat_name(content), max_count[column_names[index]]))
						# print(max_count[column_names[index]])
						val += "'{info}', ".format(info=pad_string(reformat_name(content), max_count[column_names[index]]))
					else:
						val += "'{info}', ".format(info=reformat_name(content))
					# val += "'{info}', ".format(info=reformat_name(content))
					index += 1
				col = col[:-2]
				val = val[:-2]
				# print(val)
				c.execute('INSERT INTO {tn} ({col}) VALUES ({val})'.format(tn=table_name, col=col, val=val))		

	commit_and_close(conn)

####################################################

# Step 1: Find max length of each column (dict of form (column_name, max_length))
# Open csv file to find max length of each column 
with open(csv_filename) as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=',')

	# Keep track of column names
	column_names = next(csv_reader)

	# Create dict to keep track of max length 
	max_count = OrderedDict()

	for col in column_names[:col_limit]:
		max_count[col] = 0

	# Start reading through csv file contents, skipping Emp ID
	for row in csv_reader:
		index = 1
		for content in row[1:col_limit]:
		# for content in row[1:]:
			if len(content) > max_count[column_names[index]]:
				max_count[column_names[index]] = len(content)
			index += 1
csv_file.close()

# Step 2: Create the four databases as outlined in Question 3
# Create database for 3a, 3b
conn_a, c_a = create_database(q3a_db, 4096)
conn_b, c_b = create_database(q3b_db, 16384)

# Create table (first remove them if they exist already)
c_a.execute('DROP TABLE IF EXISTS {tn}'.format(tn=table_name))
c_b.execute('DROP TABLE IF EXISTS {tn}'.format(tn=table_name))

col_declarations = ""

# column Emp_ID
col_declarations += "{nom} INT, ".format(nom=reformat_name(column_names[0]))
# Rest of the columns
for col in column_names[1:col_limit]:
	col_declarations += "{nom} CHAR({l}), ".format(nom=reformat_name(col), l=max_count[col])
col_declarations = col_declarations[:-2]

c_a.execute('CREATE TABLE {tn} ({cols})'
	.format(tn=table_name, cols=col_declarations))
c_b.execute('CREATE TABLE {tn} ({cols})'
	.format(tn=table_name, cols=col_declarations))

commit_and_close(conn_a)
commit_and_close(conn_b)

# Insert data
insert_data(q3a_db, csv_filename, table_name, column_names, max_count)
insert_data(q3b_db, csv_filename, table_name, column_names, max_count)

# Create database for 3c
conn_c, c_c = create_database(q3c_db, 4096)
col_declarations = ""
# column Emp_ID
col_declarations += "{nom} INT PRIMARY KEY, ".format(nom=reformat_name(column_names[0]))
# Rest of the columns
for col in column_names[1:col_limit]:
	col_declarations += "{nom} CHAR({l}), ".format(nom=reformat_name(col), l=max_count[col])
col_declarations = col_declarations[:-2]

c_c.execute('CREATE TABLE {tn} ({cols})'
	.format(tn=table_name, cols=col_declarations))

commit_and_close(conn_c)

# Insert data 
insert_data(q3c_db, csv_filename, table_name, column_names, max_count)

# # Create database for 3d
conn_d, c_d = create_database(q3d_db, 4096)
col_declarations = ""
# Emp_ID
col_declarations += "{nom} INT PRIMARY KEY, ".format(nom=reformat_name(column_names[0]))
# Rest of the columns
for col in column_names[1:col_limit]:
	col_declarations += "{nom} CHAR({l}), ".format(nom=reformat_name(col), l=max_count[col])
col_declarations = col_declarations[:-2]

c_d.execute('CREATE TABLE {tn} ({cols}) WITHOUT ROWID'
	.format(tn=table_name, cols=col_declarations))

commit_and_close(conn_d)

# Insert data
insert_data(q3d_db, csv_filename, table_name, column_names, max_count)