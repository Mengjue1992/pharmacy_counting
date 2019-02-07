# Table of Contents
1. [Analysis](README.md#analysis)
2. [Solution](README.md#solution)
3. [Design](README.md#design)
4. [Results](README.md#results)
5. [Assumptions and Dependencies](README.md#assumptions)

# Analysis

The problem statement seemed to be simple and straightforward. However the complexity arises considering the following
- the volume of input data to be processed
- obtain unique prescriber count per drug

# Solution 

<b>1. Data Structure :</b>
    Since pandas were not permitted for use, the following datastructure was adopted. Prescriber name is stored as 'prescriber_last_name,prescriber_first_name' throughout the processing and finally the count of prescribers is written to file.
    
    {drug_name:(drug_cost, [set of unique prescribers])}
    
A set is used to store list of prescribers per drug, because the set stores only unique elements unlike thhe list. 
Thereby eliminated the lookup time for every record and improved the performance significantly. 


<b>2. Approach : </b>
    Several alternatives were tried out. In case of sequential processing, only one core is used even if the system has multiple CPU cores. The idea was to kick off multiple processes, utilizing all cores and leverage maximum processing capacity. 
Hence, a multiprocessing approach was adopted. 

Programming Language chosen is Python. Python provides multiprocessing package as part of Python Standard Library.
Using this library, we can use Pool class and initiate process pool workers. If the system has 8 cores, we can initiate a worker pool of 8 processes. 

Sample code for using the multiprocessing package is provided below -
    
    from multiprocessing import Pool
    def function_name(x):
        return ...

    p = Pool(processes=4)
    res = p.map(function_name, [...]))
    
The results from each process pool worker is finally consolidated and merged.


# Design

 The following are the high level steps to accomplish the results: 
 <br>   1. Load the input data file into memory.
 <br>   2. Split dataset into parts and store them as part files in the input folder. Number of part files = number of cores in the system.
        Efficiently free up the original data loaded in memory as and when part files are created.
 <br>   3. Create the worker pool, each process handles one part file.
 <br>   4. Generators used to read the part file line by line for effective memory management.
 <br>   5. Consolidate the result set returned by each of the process pool workers. While consolidating, the prescriber sets containing 
        unique prescribers are merged, and the drug costs are aggregated.
 <br>   6. Sort the consolidated results by drug_cost (descending), and if in case of tie, by drug_name.
 <br>   7. Write row by row into output file. To get a count of unique prescribers, get the size of the set. Set contains only unique elements.
 <br>   8. Clean up by removing part files
</p>

# Results

The results accomplished using this multiprocessing approach was very satisfactory. A sample output is provided below. 
    <br>[ 2018.07.16 13:22:37 ] Pharma counting begins.. 
    <br>[ 2018.07.16 13:22:37 ] Initializing the setup for multiprocessing.. 
    <br>[ 2018.07.16 13:22:37 ] Loading input file: ./input/itcont.txt
    <br>[ 2018.07.16 13:22:37 ] Successfully loaded input file: ./input/itcont.txt
    <br>[ 2018.07.16 13:22:37 ] Created the following part files : ['./input/itcont.txt_part_1', './input/itcont.txt_part_2']
    <br>[ 2018.07.16 13:22:37 ] Processing part file : ./input/itcont.txt_part_1 
    <br>[ 2018.07.16 13:22:37 ] Processing part file : ./input/itcont.txt_part_2 
    <br>[ 2018.07.16 13:22:37 ] Generating Pharma report.. 
    <br>[ 2018.07.16 13:22:37 ] Successfully saved the Pharma report: ./output/top_cost_drug.txt 
    <br>[ 2018.07.16 13:22:37 ] Removing part file: ./input/itcont.txt_part_1 
    <br>[ 2018.07.16 13:22:37 ] Removing part file: ./input/itcont.txt_part_2 
    <br>[ 2018.07.16 13:22:37 ] Part files removed successfully.. 
    <br>[ 2018.07.16 13:22:37 ] Pharma counting complete. 


# Assumptions and Dependencies
1. The following packages of the Standard library have been used
    <br>multiprocessing
    <br>datetime
    <br>sys
    <br>csv
    <br>datetime
    <br>os

2. Write and Delete permissions on input folder is required since the program creates part files. After writing the output, the program does a clean up by removing the part files. Part files are created in the same directory where the source file exists.  

