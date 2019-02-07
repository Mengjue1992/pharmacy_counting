#pharmacy_counting.py

"""
This python program processes a data file related to prescribed drugs and sales, and
generates a report with details of prescribed drug along with unique number of prescribers 
and the overall cost for the same.

Command line arguments : 
	1. input file (csv file)
	2. output file (csv file)

Format of the input file:
id,prescriber_last_name,prescriber_first_name,drug_name,drug_cost

Format of the output file: Sorted by total drug cost (descending), in case of a tie, sorted by drug_name
drug_name,num_prescriber,total_cost 
"""

import multiprocessing as mp
import datetime
import sys
import csv
import datetime
import os


def create_part_files(input_file, part_file_name, num_cpus):
	"""
	Splits the input data file into chunks to enable multiprocessing.
	The function receives info on how many CPU cores are available in the system. The number of
	part files created is equal to the num of cores available. For eg., if there are 
	2 CPU cores, this function creates files input_file_part_1, input_file_part_2. 
	The function also checks how many lines are available in the input file and determines the
	chunk size - number of lines per part file.
	The function returns a list of part files it has thus created
		
    Args:
    	input_file - Input data file retrieved from command line
		part_file_name - Part file name is appended by number to make each part unique
		num_cpus - number of CPU cores
    Returns:
		list_of_part_files - list of part file names that it has created
    """

    # Load the entire file into memory for just splitting. Gradually free up memory as we process
    # Create a list from the input data file
	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Loading input file:', input_file
	dataRaw=open(input_file).read().splitlines()
	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Successfully loaded input file:', input_file


	# Initializations and counters
	max_lines = len(dataRaw)
	num=1
	from_lines = 0

	# Chunk size is the number of lines per part file
	chunk = max_lines/num_cpus
	to_lines = chunk
	list_of_part_files=[]

	# Create one part file per core
	while num <= num_cpus:
		# Part file has base "input_file_part_" and to make it unique, append a number to each filename
		file_name = part_file_name+str(num)

		# The last part may have fewer lines than the earlier part files
		if (num == num_cpus):
			to_lines = max_lines

		# Create the new part file and write the chunk  
		with open(file_name, 'w') as fh:
			fh.write("\n".join(dataRaw[from_lines:to_lines]))
			# Add the part file name created to the list	
			list_of_part_files.append(file_name)   
		
		# Remove the same chunk (corresponding records) from the original list to clear up memory. 
		# However, retain the header			
		del dataRaw[1:to_lines]

		fh.close()	
		num += 1
	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Created the following part files :', list_of_part_files
 	return list_of_part_files



def read_part_file(path):
	"""
	This function will read the part file and return one record at a time. A csv.DictReader
	is used. A generator pattern has been used that makes it very efficient.

    Args:
    	path - part file name 	
    Returns:
		row - one record from the file
    """
	with open(path, 'rU') as data:
		reader = csv.DictReader(data)
		for row in reader:
			yield row



def process_part_file(input_file):
	"""
	This function will process the part file, line by line, creates and returns a dictionary with 
	relevant info - drug name, sum of drug cost, set of unique prescribers in the format 'lastname,firstname'

    Args:
    	input_file - part file name 	
    Returns:
		part_file_result_dict - dictionary containing the results from processing one part file
		The dictionary has the following (key, value) structure:
		{drug_name: (sum of drug_cost, [set of unique prescribers])}  
    """
   	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Processing part file :', input_file

	part_file_result_dict={} 
	for row in read_part_file(input_file):
		# Prescriber name is concatenated as 'lastname,firstname'
		new_prescriber_name =  row['prescriber_last_name']+','+row['prescriber_first_name']

		try:
			# Check and get values if the key (drug_name) already exists in the dictionary 
			(cost, prescribers_set) = part_file_result_dict[row['drug_name']]
			# Add new prescriber to prescriber set. If element is repeated, set will ignore the add operation
			prescribers_set.add(new_prescriber_name)
			# Aggregate the cost, and update the value for the key in the dictionary	
			part_file_result_dict[row['drug_name']] =(cost+ float(row['drug_cost']), prescribers_set)			
		except KeyError:
			# If this is the first entry for the drug, add value as tuple with cost and set of prescriber
			part_file_result_dict[row['drug_name']] = (float(row['drug_cost']), set([new_prescriber_name]))
	# Return the dictionary 
	return part_file_result_dict



def combine_results (result):
	"""
	This function receives a list of multiple dictionaries, each dictionary corresponding to the output 
	generated by the worker process by means of multiprocessing.
	This function merges dictionaries into one single dictionary, and sorts it by drug_cost (desc) and if there
	is a tie, then sort by drug_name. 

	Args:
	results - list of dictionaries. Has a format [{...},{...}, ...]

	Returns: 
	sorted_pharma_result_dict - dictionary with format {drug_name: (total drug_cost, set of unique prescribers} 
	"""
	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']','Generating Pharma report..'
	
	# Create a new dictionary 
	pharma_dict = {}

	# For each of the dictionaries in the list add it to the new output dictionary
	for d in result:
		for key, value in d.items():
			# If key exists then consolidate
			try:
				(cost, prescribers_set) = pharma_dict[key]
				# Merge prescribers sets 
				prescribers_set = prescribers_set.union(value[1])
				# Aggregate drug costs. update the value against the key
				pharma_dict[key] =(cost+ value[0], prescribers_set)
			except KeyError:
				# If first time, then add
				pharma_dict[key] = value	

	# Sort the entries by drug_cost (descending). If there is a tie, then sort by drug name
	sorted_pharma_result_dict = sorted(pharma_dict.iteritems(), key=lambda (k, v):(-v[0], k))
	return sorted_pharma_result_dict



def create_pharma_report_file (output_file, pharma_results_dict):
	"""
	This function saves the results into the output file 

	Args: 
	output_file - output file name, file to be written into
	pharma_results_dict - dictionary with sorted output

	Returns: None
	"""
	
	# The header format for the output csv
	output_header = 'drug_name,num_prescriber,total_cost'
	with open(output_file, 'w') as out_file:
		# Write the header
		out_file.write(output_header+'\n')
		
		for rec in pharma_results_dict:
			# Write line by line into the output file. 
			# Get unique prescriber count just use len(prescriber set) 
			out_file.write (rec[0]+","+str(len(rec[1][1]))+","+str(round(rec[1][0],2))+'\n')
	out_file.close()
	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Successfully saved the Pharma report:', output_file 

def delete_part_files(file_names_list):
	"""
	This function does the clean up by removing the part files 

	Args: 
	file_names_list - list of part file names

	Returns: None
	"""

	# Check and remove the part files
	for file_name in file_names_list:
		print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Removing part file:', file_name 
		if os.path.exists(file_name):
			os.remove(file_name)
		else:
			print("Sorry, can not remove %s file." % file_name)
	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Part files removed successfully..' 	

if __name__ == "__main__":
	"""
	This is the main function. Reads commandline arguments, calls create_part_files to split huge input data file
	into part files to enable multiprocessing. Invokes combine_results to consolidate all the results returned 
	by multiple worker processes, saves the output data into a file by invoking create_pharma_report_file  and
	finally does a clean up by removing the part files the program has created.

	Args: The following command line arguments 
		input_file - input data file to be read from
		output_file - output data to be written into

	Returns: None
	"""
	# Check if input and output file names are provided
	if (len(sys.argv) != 3):
		print 'Invalid inputs. Please provide input and output file names..Try again..'
		sys.exit()

	# Retrieve input data file name from command line arguments
	input_file = sys.argv[1]
	# Retrieve output file name from command line arguments
	output_file = sys.argv[2]

	# Check if input file exists
	if not os.path.exists(input_file):
		print 'Input file does not exist. Please provide a valid input file..Try again..'
		sys.exit()

	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Pharma counting begins..'
	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Initializing the setup for multiprocessing..'

	# Prefix for part file name - use the input file name as the base
	part_file_name = input_file+'_part_'

	# Get the number of cores in the system
	num_cpus = mp.cpu_count()	

	# Split the large input data file into part files based on # of cores
	part_file_list = create_part_files(input_file, part_file_name, num_cpus)
	
	# Offers a convenient means of parallelizing the execution by creating pool of multiple worker processes 
	p = mp.Pool(num_cpus)

	# This enables processing of part files by the worker processes in parallel. 
	# Makes use of all the available cores providing the best processing capacity
	# Every worker process processes one part file and returns a dictionary
	# result is a list containing output from individual worker processes. 
	# For eg., if num_cpus = 4, then result will have the format [{...},{...},{...},{...}]   
	result = p.map(process_part_file,part_file_list)

	# Combine the part file results obtained (see prev comments)
	pharma_results_dict = combine_results (result)

	# Save the desired results into output file 
	create_pharma_report_file(output_file, pharma_results_dict)

	# Clean up by removing part files
	delete_part_files(part_file_list)
	print '[',datetime.datetime.now().strftime("%Y.%m.%d %H:%M:%S"),']', 'Pharma counting complete.' 	