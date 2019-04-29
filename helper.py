import binascii 
import counter

HEADER_SIZE = 100

# bytes of the b-tree page type
TABLE_INTERIOR = "05"
TABLE_LEAF = "0d"
INDEX_INTERIOR = "02"
INDEX_LEAF = "0a"

# Order of the columns specified in the CSV
COLUMN_NAME = {
	'Emp_ID': 0,
	'Name_Prefix': 1,
	'First_Name': 2,
	'Middle_Initial': 3,
	'Last_Name': 4,
}

# As specified in the "Representation of SQL Indices" section of the SQLite docs, 
# Index pages' payload are of the format (Indexed column (Emp_ID), row key (Row_ID))
INDEX_NAME = {
	'Emp_ID': 0,
	'Row_ID': 1
}

def hex_to_dec(hex):
	'''
	Return the decimal form of a hex
	'''
	return int(hex, 16)

def bin_to_dec(bin):
	'''
	Return the decimal form of a binary
	'''
	return int(bin, 2)

def read_from_hex_offset(file, hex_offset, length):
    '''
    Fetch length bytes from file starting from the hexadecimal offset hex_offset
    '''
    offset = int(hex_offset, base=10)
    file.seek(offset)
    return file.read(length).hex()

def get_page_size(file):
	'''
    Get number of bytes per page (in decimal)
    '''
	return hex_to_dec(read_from_hex_offset(file, "16", 2))

def get_b_tree_type(file, page):
	'''
	Given a page, return whether it is 0x05(table interior)/0x0d(table leaf)/
	0x02(index interior)/0x0a(index leaf).
	'''
	if (page == 0):
		# Root page: type of page is at offset 100
		return str(read_from_hex_offset(file, str(HEADER_SIZE), 1))
	else: 
		# if not root page, type of b-tree is the first byte of the page
		return str(read_from_hex_offset(file, str(get_page_size(file) * (page-1)), 1))

def get_num_pages(file):
	'''
	Return number of pages in a db in decimal.
	'''
	# Information always located at byte 28 of the first page
	return hex_to_dec(read_from_hex_offset(file, "28", 4))

def get_b_tree_hdr_size(page_type):
	'''
	Given 0x05/0x0d/0x02/0x0a, return the number of bytes reserved for b-tree
	header size.
	'''
	if page_type == TABLE_INTERIOR or page_type == INDEX_INTERIOR:
		return 12
	elif page_type == TABLE_LEAF or page_type == INDEX_LEAF:
		return 8

def get_num_cells(file, page):
	'''
	Return the number of cells in decimal on any given page in the db file.
	'''
	if page == 0:
		# root page
		return hex_to_dec(read_from_hex_offset(file, str(HEADER_SIZE+3), 2))
	else:
		return hex_to_dec(read_from_hex_offset(file, str((get_page_size(file) * (page-1)) + 3), 2))

def offset_to_cell_ptr_arr(file, page):
	'''
	Return the offset to the start of the cell pointer array given a page.
	'''
	if page == 0:
		# root page always table leaf page
		return HEADER_SIZE + 8
	else:
		pg_type = get_b_tree_type(file, page)
		return (get_page_size(file) * (page-1)) + get_b_tree_hdr_size(pg_type)

def offset_to_cell_content(file, page):
	'''
	Return the offset to the start of the cell content given a page.
	'''
	if page == 0:
		return hex_to_dec(read_from_hex_offset(file, str(HEADER_SIZE + 5), 2))
	else:
		return str(int(hex_to_dec(read_from_hex_offset(file, str((get_page_size(file) * (page-1)) + 5), 2))) + (page-1)*get_page_size(file))

def get_varint(file, starting_offset):
	'''
	Given some offset where the varint starts, return the binary form.
	In the form (varint in binary, ending offset in decimal)
	'''
	# Keep track of most_significant bit since we have to keep reading if the bit is 1
	most_significant = 1
	varint = ""
	offset = int(starting_offset)

	# keep track of number of bytes read so far (max is 9)
	num_bytes = 1

	while (most_significant == 1) and num_bytes < 10:
		hex_to_bin = format(hex_to_dec(read_from_hex_offset(file, str(offset), 1)), '08b')
		most_significant = int(hex_to_bin[0])
		if num_bytes < 9:
			varint += hex_to_bin[1:]
		else:
			# We keep all 8 bits if we are on the 9th byte
			varint += hex_to_bin
		offset = offset + 1
		num_bytes += 1

	# pad varint with 0s if necessary so that it is of mod8 bits
	missing = 8 - (len(varint)%8)
	varint = str("0" * missing) + varint
	return varint, offset


def decode_varint(varint):
	'''
	Given a varint in binary, return the twos complements in decimal.
	'''
	new_bin = ""
	for bit in range(0, len(varint), 8):
		# Drop every 8th bit except for the 9th byte (if it exists)
		if (bit != 64): #64 is the position of the most sig bit for the 9th byte
			# drop most significant bit
			new_bin += varint[bit+1:bit+8]
		else:
			# Keep most significant bit!
			new_bin += varint[bit:bit+8]
	return new_bin

def get_serial_code_size(varint):
	'''
	Given a varint, return the size of the serial code (as specified in section 2.1 of the sqlite
	file format page)
	'''
	# First convert to decimal
	dec = bin_to_dec(varint)

	if (dec <= 4):
		return dec
	elif (dec == 5):
		return 6
	elif (dec == 6 or dec == 7):
		return 8
	elif (dec == 8 or dec == 9):
		return 0
	elif (dec >= 12 and dec%2 == 0):
		# greater than 12 and even
		return (dec - 12)//2
	elif (dec >= 13 and dec%2 == 1):
		# Greater than 13 and odd
		return (dec - 13)//2
	else:
		exit("Invalid serial code. Exiting.")

def get_root_page(file, page):
	'''
	Get the root page number(s) (in decimal) from the first page of the file
	'''
	counter.header += 1
	counter.table_leaf += 1

	# keep track of root pages in a list since there maybe more than one (depending if the database is 
	# indexed or not)
	root_pages = []

	num_cells = get_num_cells(file, page)
	offset_to_ptr_arr = offset_to_cell_ptr_arr(file, page)
	offset_to_content = offset_to_cell_content(file, page)

	for cell in range(0, num_cells * 2, 2):
		cell_offset = int(hex_to_dec(read_from_hex_offset(file, str(offset_to_ptr_arr + cell), 2))) + get_page_size(file) * (page)

		# Get payload size from cell
		payload_varint, payload_offset = get_varint(file, cell_offset)
		payload_size = bin_to_dec(decode_varint(payload_varint))

		# Get rowID
		row_id, row_id_offset = get_varint(file, payload_offset)
		row_id = bin_to_dec(row_id)

		# Start parsing through intial portion of payload
		hdr_len_varint, hdr_len_offset = get_varint(file, row_id_offset) # number of bytes to find out the length of the payload header (varint)
		payload_hdr_len = hex_to_dec(read_from_hex_offset(file, str(row_id_offset), len(hdr_len_varint) // 8))

		hdr_offset = row_id_offset + len(hdr_len_varint) // 8

		# Since the root page number is located at the fourth serial code offset,
		# keep track of the previous 3 serial code offsets to find root page
		root_pg_offset = 0
		byte = 0
		while byte < payload_hdr_len - 1:
			varint, _ = get_varint(file, hdr_offset + byte)
			# root page is specified at the 4th serial code (ie at offset 3 starting from
			# the header content offset)
			if (byte == 3):
				# Number of bytes to read 
				root_byte_len = get_serial_code_size(varint)

			elif (byte < 3): 
				# Keep track of offsets of columns before rootpage column 
				root_pg_offset += get_serial_code_size(varint)

			if (len(varint) > 8):
				# varint spans more than 1 byte, increment iterator to keep track
				# of the right offset
				byte += 1
			byte += 1

		record_body_offset = hdr_offset + byte
		root_page = read_from_hex_offset(file, str(record_body_offset + root_pg_offset), root_byte_len)
		root_pages.append(hex_to_dec(root_page))

	return root_pages

def get_left_children(file, offset):
	'''
	Return page number of left children pointers given an offset
	'''
	# read 4-bytes to get page number of left child
	return hex_to_dec(read_from_hex_offset(file, str(offset), 4))

def get_right_children(file, page):
	'''
	Return page number of left children pointers given an interior page
	'''
	# right child is the 4 bytes at offset 8 bytes in the b-tree header
	return hex_to_dec(read_from_hex_offset(file, str(get_page_size(file) * (page-1) + 8), 4))

def binary_search(file, page, col, key, track, isWithoutRowID=False, 
	isSearchByRowID=False, row_id_set=None,
	isRange=False, range_start=0, range_end=0):
	'''
	Given a page in the file, perform binary_search to return records matching
	the key

	INPUT:
		file: database file to be read
		page: starting page to recurse through its children pages (if it exists)
		col: Name of column of database
		key: what you want to search for in col
		track: a set to keep track of results
		isWithoutRowID: True if database created with the WITHOUT ROWID specification and want to 
						search the index pages. False otherwise.
		isSearchByRowID: True if database is indexed and isWithoutRowID=False. It will find records based on 
						rowid as opposed to col=key. False otherwise.
		row_id_set: Only used if isSearchByRowID=True. It is the set of Row IDs that need to be found in the db.
		isRange: True if trying to find records matching a range. False otherwise.
		range_start: Only use if isRange=True. Starting value of the range.
		range_end: Only use if isRange=True. Ending value of the range. 
	'''
	counter.counter += 1
	pg_type = get_b_tree_type(file, page)

	if (pg_type == TABLE_LEAF):
		counter.table_leaf += 1
		# Read stored records to see if anything matches the key
		num_cells = get_num_cells(file, page)
		offset_to_ptr_arr = offset_to_cell_ptr_arr(file, page)
		offset_to_content = offset_to_cell_content(file, page)

		# read each cell content in leaf page
		for ptr in range(0, num_cells * 2, 2):
			cell_offset = int(hex_to_dec(read_from_hex_offset(file, str(offset_to_ptr_arr + ptr), 2))) + get_page_size(file) * (page - 1)

			# Get payload size information from the table leaf cell
			payload_varint, payload_offset = get_varint(file, cell_offset)
			payload_size = bin_to_dec(decode_varint(payload_varint))

			# Get rowID
			row_id, row_id_offset = get_varint(file, payload_offset)
			row_id = bin_to_dec(row_id)

			if (isSearchByRowID):
				# Compare rowID to see if anything matches the rowIDs given in row_id_set.
				# Only continue reading the cell if rowID is a match. Otherwise, keep looking
				if (row_id not in row_id_set):
					continue

			# number of bytes to find out the length of the payload header (varint)
			hdr_len_varint, hdr_len_offset = get_varint(file, row_id_offset) 
			# Number of bytes that the serial codes will take up
			payload_hdr_len = hex_to_dec(read_from_hex_offset(file, str(row_id_offset), len(hdr_len_varint) // 8))

			# offset to start of header (after header length)
			hdr_offset = row_id_offset + len(hdr_len_varint) // 8

			byte = 0
			# serial location 
			serial = COLUMN_NAME[col]
			serial_offset = 0
			# Keep track of current potential match's Emp_ID + Name
			name = ""

			# Read through payload contents
			while byte < payload_hdr_len - 1:
				varint, _ = get_varint(file, hdr_offset + byte)
				# Keep track of how many bytes we need to read for each serial 
				length = get_serial_code_size(varint)

				if (byte == serial):
					# Found desired serial in record

					if (byte == 0):
						# Keep track of current potential match's Emp_ID + Name
						# if Emp_ID, stored as hex -> dec. Otherwise, store as hex -> ascii
						potential_match = hex_to_dec(read_from_hex_offset(file, str(hdr_offset + (payload_hdr_len - 1) + serial_offset), length))
						name += str(hex_to_dec(read_from_hex_offset(file, str((hdr_offset) + (payload_hdr_len - 1) + serial_offset), length))) + " "
					else:
						potential_match = str(binascii.unhexlify(read_from_hex_offset(file, str(hdr_offset + (payload_hdr_len - 1) + serial_offset), length)))[2:-1]
				
						field = str(binascii.unhexlify(read_from_hex_offset(file, str((hdr_offset) + (payload_hdr_len - 1) + serial_offset), length)))
						name += field[2:-1] + " "
				else:
					if (byte == 0):
						# Need to keep track of other contents in the payload that's part of the name
						name += str(hex_to_dec(read_from_hex_offset(file, str((hdr_offset) + (payload_hdr_len - 1) + serial_offset), length))) + " "
					else:
						field = str(binascii.unhexlify(read_from_hex_offset(file, str((hdr_offset) + (payload_hdr_len - 1) + serial_offset), length)))
						name += field[2:-1] + " "
				if (len(varint) > 8):
					# varint spans more than 1 byte, increment iterator to keep track
					# of the right offset
					byte += 1
				byte += 1
				# Update offset of the next serial to read in the cell
				serial_offset += length

			if not isRange:
				if not isSearchByRowID:
					if(str(potential_match) == key):
						track.add(name)
				else: 
					# If reached this line, we have already found the matching record.
					track.add(name)
			else: 
				if not isSearchByRowID:
					if(int(str(potential_match)) >= int(range_start) and int(str(potential_match)) <= int(range_end)):			
							track.add(name)
				else: 
					# If reached this line, we have already found the matching record.
					track.add(name)
		
	elif (pg_type == TABLE_INTERIOR):
		counter.table_interior += 1
	
		num_cells = get_num_cells(file, page)
		offset_to_ptr_arr = offset_to_cell_ptr_arr(file, page)

		# Each table interior page has one 4-byte page number for right child
		right_child_pg = get_right_children(file, page)
		binary_search(file, right_child_pg, col, key, track, isWithoutRowID,isSearchByRowID, row_id_set, isRange, range_start, range_end)

		# Iterate through each cell pointer
		for ptr in range(0, num_cells * 2, 2):
			# Get the offset to the cell content
			cell_offset = int(hex_to_dec(read_from_hex_offset(file, str(offset_to_ptr_arr + ptr), 2))) + get_page_size(file) * (page - 1)
			# Recursively search children in left pointer
			left_child_pg = get_left_children(file, cell_offset)

			# each table interior cell holds the page number of its left child 
			binary_search(file, left_child_pg, col, key, track, isWithoutRowID,isSearchByRowID, row_id_set, isRange, range_start, range_end)

	elif (pg_type == INDEX_INTERIOR or pg_type == INDEX_LEAF):
		if (pg_type == INDEX_LEAF):
			counter.index_leaf += 1
		if (pg_type == INDEX_INTERIOR):
			counter.index_interior += 1
			# Each index interior page has one 4-byte page number for right child
			right_child_pg = get_right_children(file, page)
			binary_search(file, right_child_pg, col, key, track, isWithoutRowID, isSearchByRowID, row_id_set, isRange, range_start, range_end)

		num_cells = get_num_cells(file, page)
		offset_to_ptr_arr = offset_to_cell_ptr_arr(file, page)
		offset_to_content = offset_to_cell_content(file, page)
		for ptr in range(0, num_cells * 2, 2):
			cell_offset = int(hex_to_dec(read_from_hex_offset(file, str(offset_to_ptr_arr + ptr), 2))) + get_page_size(file) * (page-1)
			if (pg_type == INDEX_INTERIOR):
				# Each index interior cell has one 4-byte page number for left child
				left_child_pg = get_left_children(file, cell_offset)
				binary_search(file, left_child_pg, col, key, track, isWithoutRowID, isSearchByRowID, row_id_set, isRange, range_start, range_end)

				# Add 4 bytes to cell_offset since 4 bytes are allocated to left pointer child pages
				payload_varint, payload_offset = get_varint(file, cell_offset+4)
			else:
				# index leaf cells do not have children pages
				payload_varint, payload_offset = get_varint(file, cell_offset)

			payload_size = bin_to_dec(decode_varint(payload_varint))

			# Index cells do not contain rowIDs/int keys, unlike table cells
			hdr_len_varint, hdr_len_offset = get_varint(file, payload_offset)

			payload_hdr_len = hex_to_dec(read_from_hex_offset(file, str(payload_offset), len(hdr_len_varint) // 8))

			hdr_offset = payload_offset + len(hdr_len_varint) // 8

			byte = 0
			if (isWithoutRowID == False):
				# payload in INDEX_INTERIOR cells are of format (Emp_ID, rowid)
				serial = INDEX_NAME[col]
			else: 
				serial = COLUMN_NAME[col]
			serial_offset = 0

			# keep track of current potential match's Emp_ID + Name
			name = ""

			while byte < payload_hdr_len - 1:
				varint, _ = get_varint(file, hdr_offset + byte)
				length = get_serial_code_size(varint)

				if (byte == serial):
					if not isWithoutRowID:
						# Keep track of current potential match (i.e. in this case it is Emp_ID)
						potential_match = hex_to_dec(read_from_hex_offset(file, str(hdr_offset + (payload_hdr_len - 1) + serial_offset), length))
					else: 
						# Keep track of current potential match's Emp_ID + Name
						if (byte == 0):
							# if Emp_ID, stored as hex -> dec
							potential_match = hex_to_dec(read_from_hex_offset(file, str(hdr_offset + (payload_hdr_len - 1) + serial_offset), length))
							name += str(hex_to_dec(read_from_hex_offset(file, str((hdr_offset) + (payload_hdr_len - 1) + serial_offset), length))) + " "
						
						else:
							#Else, stored as hex -> ascii
							potential_match = str(binascii.unhexlify(read_from_hex_offset(file, str(hdr_offset + (payload_hdr_len - 1) + serial_offset), length)))[2:-1]
					
							field = str(binascii.unhexlify(read_from_hex_offset(file, str((hdr_offset) + (payload_hdr_len - 1) + serial_offset), length)))
							name += field[2:-1] + " "
				else:
					if not isWithoutRowID:
						if (length > 0): 
							# Keep track of row_ID in case the Emp_ID matches our search
							potential_row_id = hex_to_dec(read_from_hex_offset(file, str(hdr_offset + (payload_hdr_len - 1) + serial_offset), length))
					else:
						# Keep track of current potential match's Emp_ID + Name
						if (byte == 0):
							# if Emp_ID, stored as hex -> dec
							name += str(hex_to_dec(read_from_hex_offset(file, str((hdr_offset) + (payload_hdr_len - 1) + serial_offset), length))) + " "
						else:
							# if not Emp_ID, stored as hex -> ascii
							field = str(binascii.unhexlify(read_from_hex_offset(file, str((hdr_offset) + (payload_hdr_len - 1) + serial_offset), length)))
							name += field[2:-1] + " "
				if (len(varint) > 8):
					# varint spans more than 1 byte, increment interator to keep track of the offset
					byte += 1
				byte += 1
				# Update offset to read next serial
				serial_offset += length

			if not isRange:
				if (str(potential_match) == key):
					if not isWithoutRowID:
						# keep the row_id as we will need to use it to find it in the table b-trees later
						track.add(potential_row_id)

					else:
						track.add(name)
			else: 
				if(str(potential_match)>=range_start and str(potential_match)<=range_end):
					if not isWithoutRowID:
						# keep the row_id as we will need to use it to find it in the table b-trees later
						track.add(potential_row_id)

					else:
						track.add(name)
