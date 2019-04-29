from helper import *
import counter
import timeit
#################### Input info ####################
# Database
db_file = "q3a_db.db"
# Queries to run
scan_db = True
equality_db = True
range_db = True
####################################################

with open(db_file, 'rb') as f:
	page_size = get_page_size(f)

	################ Scan operation on db # 3a.###################
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

	################ Equality operation on db # 3a.###################
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
			if (get_b_tree_type(f, page) == TABLE_INTERIOR or get_b_tree_type(f, page) == TABLE_LEAF):
				root = page
		track = set()
		binary_search(f, root, "Emp_ID", "181162", track)
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

	################ Range operation on db # 3a.###################
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
			if (get_b_tree_type(f, page) == TABLE_INTERIOR or get_b_tree_type(f, page) == TABLE_LEAF):
				root = page
		track = set()
		binary_search(f, root, "Emp_ID", None, track, isRange=True, range_start="171800", range_end="171899")
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