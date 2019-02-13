import numpy
import xlrd
import os, sys
from os import listdir
from os.path import isfile, join
import pandas as pd
import pyodbc
import sqlalchemy
import urllib
import re
import datetime
import requests
import json
import nuance_conversion
import extract_data_from_xml
import time
import shutil
from nuance_conversion import main_process
from datetime import date, timedelta
from azure.storage.file import FileService
from azure.storage.blob import BlockBlobService
from rmv_duplicates import rmv_dup_process

# consider writing regex "LOCATION EXCEPTION ---"\n"here--EXCEPTION TO 165:10-3-28(C)(2)(B) -"
date_type = 0
files = []
block_blob_service = BlockBlobService(account_name='syntax',
                                      account_key='NRdz4AAsiGv5Hnug1dEHWJJvI6tT7q9H0MBJnL1kcRlPwbVw+EKTFpLZvd3SlJXqsSylTknfps/R3aCyenh7Lg==')


def process_dockets_code():
    cwd = os.getcwd()
    current_directory = os.getcwd()
    inputs_directory = os.path.join(current_directory, r'inputs')
    # creating run stats file and the temporary input, nunace outpout and archive directories locally
    t = time.localtime()
    timestamp = time.strftime('%b-%d-%Y_%H%M', t)
    run_stats_file_name = 'run_stats' + '_' + timestamp + '.txt'
    current_directory = os.getcwd()
    stats_file_directory = os.path.join(current_directory, r'run_stats')
    if not os.path.exists(stats_file_directory):
        os.makedirs(stats_file_directory)
    stats_output_filename = stats_file_directory + '\\' + run_stats_file_name
    f = open(stats_output_filename, "w")
    f.write("\t\t\tRun statistics\n")
    # to make new directories

    cwd = os.getcwd()
    current_directory = os.getcwd()
    inputs_directory = os.path.join(current_directory, r'inputs')
    if not os.path.exists(inputs_directory):
        os.makedirs(inputs_directory)

    current_directory = os.getcwd()
    outputs_directory = os.path.join(current_directory, r'nuance_outputs')
    if not os.path.exists(outputs_directory):
        os.makedirs(outputs_directory)

    current_directory = os.getcwd()
    archives_directory = os.path.join(current_directory, r'archives')
    if not os.path.exists(archives_directory):
        os.makedirs(archives_directory)

    # try:

    input_files_count = 0
    # read input files from the storage account. copies files to locally and deletes the files in storage account
    generator = block_blob_service.list_blobs('cg-input')
    for file_or_dir in generator:
        files_name = file_or_dir.name
        # writing input files to folder
        if 'dockets/daily_weekly/' in files_name and 'placeholder.xlsx' not in files_name and '.synoemptyblob' not in files_name:
            input_files_count += 1
            # print(files_name)
            file_name = files_name.split("/")[-1]
            # files.append(files_name)
            # block_blob_service.get_blob_to_path('docket-inputs-del', files_name, "C:/Users/kiran/PycharmProjects/files_storage/test_outputs/"+files_name)
            block_blob_service.get_blob_to_path('cg-input', files_name, os.path.join(inputs_directory, file_name))
            # to delete_blob
            block_blob_service.delete_blob('cg-input', files_name)
    # print("blobs moved")
    f.write(timestamp + ":  \t" + "number of input blobs found - " + str(input_files_count) + "\n")

    # reading pdf files from cg-archive to local archoves folder. this is used to removed duplicates
    generator = block_blob_service.list_blobs('cg-archive')
    for file_or_dir in generator:
        files_name = file_or_dir.name
        if 'dockets/' in files_name and 'placeholder.xlsx' not in files_name and '.synoemptyblob' not in files_name:
            # input_files_count+=1
            # print(files_name)
            file_name = files_name.split("/")[-1]
            # files.append(files_name)
            # block_blob_service.get_blob_to_path('docket-inputs-del', files_name, "C:/Users/kiran/PycharmProjects/files_storage/test_outputs/"+files_name)
            block_blob_service.get_blob_to_path('cg-archive', files_name,
                                                os.path.join(archives_directory, file_name))

    # If there is atleast one input blob found in cg-archives we will pass it through duplcate checker or else the program terminates
    if input_files_count > 0:
        # input_files_path ="C:\\Users\kiran\\Desktop\\codes\\extract_docket_pdf_to_db\\inputs"
        # archive_files_path ="C:\\Users\kiran\\Desktop\\codes\\extract_docket_pdf_to_db\\archives"

        input_files = [f for f in listdir(inputs_directory) if isfile(join(inputs_directory, f))]
        archive_files = [f for f in listdir(archives_directory) if isfile(join(archives_directory, f))]

        input_files = [os.path.join(inputs_directory, x) for x in input_files]
        archive_files = [os.path.join(archives_directory, x) for x in archive_files]
        uniq_inp_flag = rmv_dup_process(input_files, archive_files)
        print("duplicates_removed")
        t = time.localtime()
        timestamp = time.strftime('%b-%d-%Y_%H%M', t)
        f.write(timestamp + ":  \t" + "successfully run the duplicate_checker on the inputs" + "\n")
    else:
        t = time.localtime()
        timestamp = time.strftime('%b-%d-%Y_%H%M', t)
        f.write(timestamp + ":  \t" + "run successful and no input blobs found" + "\n")
        uniq_inp_flag = 0

    # uniq_inp_flag gives the details about files that successfully passed through a duplicate checker(i.e. files that are not duplicates)
    if uniq_inp_flag != 0:
        t = time.localtime()
        timestamp = time.strftime('%b-%d-%Y_%H%M', t)
        f.write(timestamp + ":  \t" + "Unique_files found and are currently under nuance processing" + "\n")
        main_process("200")
        t = time.localtime()
        timestamp = time.strftime('%b-%d-%Y_%H%M', t)
        f.write(timestamp + ":  \t" + "Nuance processing successful" + "\n")

        t = time.localtime()
        timestamp = time.strftime('%b-%d-%Y_%H%M', t)
        f.write(timestamp + ":  \t" + "extracting data from nuance generated xml files" + "\n")
        result_rows = extract_data_from_xml.main_process()
        t = time.localtime()
        timestamp = time.strftime('%b-%d-%Y_%H%M', t)
        f.write(timestamp + ":  \t" + "run successful and data is written to db" + "\n")
        f.write(timestamp + ":  \t" + str(result_rows) + " rows newly added  to db" + "\n")
    else:
        t = time.localtime()
        timestamp = time.strftime('%b-%d-%Y_%H%M', t)
        f.write(timestamp + ":  \t" + "No Unique_files found " + "\n")
        print("no unique files")

    # extract_data_from_xml.main_process()


    #
    # except:
    #     t = time.localtime()
    #     timestamp = time.strftime('%b-%d-%Y_%H%M', t)
    #     f.write(timestamp + ":  \t" + "run unsuccessul because of errors" + "\n")

    # once the process finishes and the locally created temporary input blobs will be deleted
    if os.path.exists(outputs_directory):
        shutil.rmtree(outputs_directory)

    if os.path.exists(inputs_directory):
        shutil.rmtree(inputs_directory)

    if os.path.exists(archives_directory):
        shutil.rmtree(archives_directory)

    f.close()
    block_blob_service.create_blob_from_path('syntax', "occ/dockets/run_stats/daily_weekly/" + run_stats_file_name,
                                             stats_output_filename)
    if os.path.exists(stats_file_directory):
        shutil.rmtree(stats_file_directory)

    print("done")


process_dockets_code()
