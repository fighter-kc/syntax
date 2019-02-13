import os, sys
import hashlib
from pathlib import Path
import time
from os import listdir
from os.path import isfile, join

# It calculates the hash value for each file ; decrease the block size if input file size is more
def hashfile(path, blocksize=65536):
    afile = open(path, 'rb')
    hasher = hashlib.md5()
    buf = afile.read(blocksize)
    while len(buf) > 0:
        hasher.update(buf)
        buf = afile.read(blocksize)
    afile.close()
    return hasher.hexdigest()


'''
comapres if the hashvalue of dict1 is already present or not. For every unique values 
in dict1, it will add them to dict_unique
'''
def find_dups(dic_unique, dict1, dict2):
    for key in dict1.keys():
        if key not in dict2 and key not in dic_unique:
            dic_unique[key] = dict1[key]




# main function in this file which calls all other function and process inputs
def rmv_dup_process(input_files, archive_files):
    archive_hash = {}
    inps_hash={}
    unique_inps={}
    unique_inps_flag=0

    # calculates hash values for all the archive files
    for file_path in archive_files:
        if Path(file_path).exists():
           files_hash = hashfile(file_path)
           archive_hash[files_hash] = file_path
        else:
            print('%s is not a valid path, please verify' % file_path)
            sys.exit()
    # calculates hash value for all the input files
    for file_path in input_files:
        if Path(file_path).exists():
           files_hash = hashfile(file_path)
           inps_hash[files_hash]=file_path
        else:
            print('%s is not a valid path, please verify' % file_path)
            sys.exit()
    '''
    checks whether the hashes of input and archive files matches. if the hash values of input files are not present in 
    archive files hash values. it will then classify them as unique inputs
    '''
    find_dups(unique_inps,  inps_hash, archive_hash)

    # this helps to eliminate if there are duplicates in given inputs itself i.e., if two same files are present in the given inputs
    for inp_f in input_files:
        curr_key = hashfile(inp_f)
        if curr_key in unique_inps and inp_f!=unique_inps[curr_key] or curr_key not in unique_inps:
            # print("inp",inp_dups[curr_key])
            os.remove(inp_f)
            # print(inp_f," removed")

    if len(unique_inps)>0:
        unique_inps_flag =1

    return unique_inps_flag
