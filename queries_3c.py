# import binascii 
from helper import *
import counter
import timeit
# #################### Input info ####################
# Database
db_file = "q3c_db.db"
# Queries to run
scan_db = True
equality_db = True
range_db = True
# ####################################################

with open(db_file, 'rb') as f:
	page_size = get_page_size(f)

	# print(root_page)

	# ################ Scan operation on db # 3c.###################
	if scan_db:
		counter.counter = 0
		counter.header = 0
		counter.index_leaf = 0
		counter.index_interior = 0
		counter.table_leaf = 0
		counter.table_interior = 0
		start = timeit.default_timer()
		# Step 1: Explore sqlite_master table (located on the first page)
		root_page = get_root_page(f, page=0)
		# Last Name is not indexed, so no need to search through index b-tree root.
		# Just start searching from the table b-tree root
		for page in root_page:
			if (get_b_tree_type(f, page) == TABLE_INTERIOR or get_b_tree_type(f, page) == TABLE_LEAF):
				root = page

		track = set()
		binary_search(f, root, "Last_Name", "Rowe         ", track)
		for record in track:
			print(record)

		stop = timeit.default_timer()
		time = stop - start
		print('Time: ' + str(time) + " seconds")

		page_reads = counter.counter
		print("Page reads: " + str(page_reads))

		print("header reads: " + str(counter.header))
		print("index leaf reads: " + str(counter.index_leaf))
		print("index interior reads: " + str(counter.index_interior))
		print("table leaf reads: " + str(counter.table_leaf))
		print("table interior reads: " + str(counter.table_interior))

		print('Time per page: ' + str(time/page_reads) + " seconds")

	# ################ Equality operation on db # 3c.###################
	if equality_db:
		counter.counter = 0
		counter.header = 0
		counter.index_leaf = 0
		counter.index_interior = 0
		counter.table_leaf = 0
		counter.table_interior = 0
		start = timeit.default_timer()
		# Step 1: Explore sqlite_master table (located on the first page)
		root_page = get_root_page(f, page=0)
		for page in root_page:
			if (get_b_tree_type(f, page) == INDEX_INTERIOR or get_b_tree_type(f, page) == INDEX_LEAF):
				index_root = page
			else: 
				table_root = page

		# First use index pages to find Emp_ID's corresponding rowID to search
		# in table pages afterwards. 
		emp_row_IDs = set()
		binary_search(f, index_root, "Emp_ID", "181162", emp_row_IDs)
		# print(emp_row_IDs)

		# Use the row_IDs that match our query to search in table b-trees
		track = set()
		binary_search(f, table_root, "Emp_ID", None, track, isSearchByRowID=True, row_id_set=emp_row_IDs)
		for record in track:
			print(record)

		stop = timeit.default_timer()
		time = stop - start
		print('Time: ' + str(time) + " seconds")

		page_reads = counter.counter
		print("Page reads: " + str(page_reads))

		print("header reads: " + str(counter.header))
		print("index leaf reads: " + str(counter.index_leaf))
		print("index interior reads: " + str(counter.index_interior))
		print("table leaf reads: " + str(counter.table_leaf))
		print("table interior reads: " + str(counter.table_interior))

		print('Time per page: ' + str(time/page_reads) + " seconds")

	# ################ Range operation on db # 3c.###################
	if range_db:
		counter.counter = 0
		counter.header = 0
		counter.index_leaf = 0
		counter.index_interior = 0
		counter.table_leaf = 0
		counter.table_interior = 0
		start = timeit.default_timer()
		# Step 1: Explore sqlite_master table (located on the first page)
		root_page = get_root_page(f, page=0)
		for page in root_page:
			if (get_b_tree_type(f, page) == INDEX_INTERIOR or get_b_tree_type(f, page) == INDEX_LEAF):
				index_root = page
			else: 
				table_root = page

		# First find corresponding row_ids that match the Emp_IDs falling within the range search
		emp_row_IDs = set()
		binary_search(f, index_root, "Emp_ID", None, emp_row_IDs, isRange=True, range_start="171800", range_end="171899")
	
		# Use the row_IDs that match our query to search in the table b-trees
		track = set()
		binary_search(f, table_root, "Emp_ID", None, track, isSearchByRowID=True, row_id_set=emp_row_IDs, isRange=True)
		for record in track:
			print(record)
		
		stop = timeit.default_timer()
		time = stop - start
		print('Time: ' + str(time) + " seconds")

		page_reads = counter.counter
		print("Page reads: " + str(page_reads))

		print("header reads: " + str(counter.header))
		print("index leaf reads: " + str(counter.index_leaf))
		print("index interior reads: " + str(counter.index_interior))
		print("table leaf reads: " + str(counter.table_leaf))
		print("table interior reads: " + str(counter.table_interior))

		print('Time per page: ' + str(time/page_reads) + " seconds")
	f.close()
