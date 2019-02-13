import xml.etree.ElementTree as ET
import re
import csv
import datetime
import pandas as pd
import os, sys
import datetime
import string
import sqlalchemy
import urllib
import pyodbc
from datetime import date, timedelta
from os import listdir
from os.path import isfile, join
from azure.storage.blob import BlockBlobService

# initialising variables
dates_list = []
final_df = pd.DataFrame()


# code to connect to db
server = 'syntax-dev-sql.database.windows.net'  # 'syntax-dev-sql.database.windows.net'
database = 'stg_cg_export'
username = 'syntax-admin'
password = 'OneuptopAhk2018'  # 'cR5ULPxIVdxG'
driver = '{ODBC Driver 13 for SQL Server}'

cnxn = pyodbc.connect(
    'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password,
    autocommit=True)
cursor = cnxn.cursor()
cursor.fast_executemany = True
params = urllib.parse.quote_plus(
    'DRIVER=' + driver + ';SERVER=' + server + ';PORT=1443;DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
# xml_files = ['docket_thu.xml']  # ['docket_mon.xml']


# list of possible order descriptions
order_description = ['DECLARATORY RULING','REMEDIATION PLAN','SHUT IN', '?? (UPDATE REQUIRED)','CHANGE OPERATOR', 'DETERMINATION', 'FINES', 'RULES - EXCEPTION', 'INCREASED DENSITY', 'ORDER - EXCEPTION',
                     'ORDER - VACATE',
                     "ORDER - AMEND",
                     'ORDER - DETERMINE', 'ORDER - CLARIFY', "ORDER - MODIFY", 'VACATE PERMIT/INTENT',
                     'HORIZONTAL SPACING',
                     'LOCATION EXCEPTION', 'HORIZ LOC EXCEPTION', "INCREASED DENSITY", "POOLING",
                     "MULTIUNIT HORIZ WELL",
                     "VACATE PERMIT/INTENT", "COMMERCIAL DISP WELL", "MODIFY ORDER",
                     "ALLOWABLE", "SPACING", "CHANGE OPERATOR", "DISPOSAL WELL", "COMPLAINT - CONTEMPT", "CONTEMPT",
                     "PROPOSED RULEMAKING", "ACCESS GATHERING", "ACCESS OFFSITE PRTY", "RULES - VARIANCE",
                     "RECYCLE FACILITY", "SHUT IN",
                     "VACATE DRILL PERMIT", "HORIZONTAL WELL", "MULTIUNIT HORIZ WELL", "UNITIZATION",
                     "REVOKE DRILL PERMIT",
                     "WAIVER PIT CLOSURE", "STATE FUNDS PLUGGING", "ENHANCED RECOVERY",
                     'ACCESS & DETERMINATION OF FEE/TERMS', 'UNDERGROUND STORAGE', 'ACCESS GATHERING SYS']

# counties_list
counties_list = ["STATEWIDE", 'NOT DENOTED', 'MULTI CO-LOC', 'Adair', 'Alfalfa', 'Atoka', 'Beaver', 'Beckham', 'Blaine',
                 'Bryan', 'Caddo', 'Canadian', 'Carter', 'Cherokee', 'Choctaw',
                 'Cimarron', 'Cleveland', 'Coal', 'Comanche', 'Cotton', 'Craig', 'Creek', 'Custer', 'Delaware', 'Dewey',
                 'Ellis', 'Garfield', 'Garvin', 'Grady', 'Grant', 'Greer', 'Harmon', 'Harper', 'Haskell', 'Hughes',
                 'Kingfisher',
                 'Jefferson', 'Johnston', 'Kay', 'Kiowa', 'Latimer', 'Le Flore', 'Lincoln', 'Logan',
                 'Love', 'McClain', 'McCurtain', 'McIntosh', 'Major', 'Marshall', 'Mayes', 'Murray', 'Muskogee',
                 'Noble', 'Nowata', 'Okfuskee', 'Oklahoma', 'Okmulgee', 'Pottawatomie',
                 'Osage', 'Ottawa', 'Pawnee', 'Payne', 'Pittsburg', 'Pontotoc', 'Pushmataha',
                 'Roger Mills', 'Rogers', 'rogermills', 'Seminole', 'Sequoyah', 'Stephens', 'Texas', 'Tillman', 'Tulsa',
                 'Wagoner', 'Washington', 'Washita', 'Woods', 'Woodward', 'Jackson']
counties_list = [item.upper() for item in counties_list]


# creating a list of any size an keeping the default given value
def pad(column, content, length):
    column.extend([content] * (length - len(column)))
    return column

# formates a string in a dataframe column
def apply_format_df(inp_string):
    inp_string = inp_string.replace("/", "-")
    for dt in dates_list:
        if inp_string in dt:
            return dt

# converting date from any format to YYYY-MM-DD
def format_date(string):
    for fmt in ["%m/%d/%Y", "%m-%d-%Y", "%Y%m%d"]:
        date = datetime.datetime.strptime(string, fmt)
        date = date.strftime('%Y-%m-%d')
        return date


# SECTION 25 & 36/7N/6W & SECTION 1 &
# handle this case
# date_type = 0
# find the date from a given input line
def find_date(date_str):
    global date_type
    date_str = date_str.lower().strip()
    if re.match("[a-z\s]+\s*[0-9]{1,2},\s*[0-9]{4}$", date_str) is not None:
        date1 = date_str.split(" ", 1)[1].strip()
        fmt_date = datetime.datetime.strptime(date1, '%B %d, %Y')
        fmt_date = [fmt_date.strftime('%Y-%m-%d')]
        date_type = 1
        return fmt_date
    if re.match(r'^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}\s+[a-z]{1,}\s+[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}',
                date_str) is not None:
        dates = date_str.split("to")
        date_begin = format_date(dates[0].strip())
        date_end = format_date(dates[1].strip())
        date_1 = date_begin.split("-")
        date_2 = date_end.split("-")
        d1 = date(int(date_1[0]), int(date_1[1]), int(date_1[2]))  # start date
        d2 = date(int(date_2[0]), int(date_2[1]), int(date_2[2]))  # end date
        delta = d2 - d1  # timedelta
        fmt_date = []
        for k in range(delta.days + 1):
            fmt_date.append((d1 + timedelta(k)).isoformat())
        return fmt_date
    return ""


# this  is specially for the cases where the addoitional section details are present along with order description
def identify_extra_sections(curr_row):
    # if curr_row =="SECTION 31/1N/3W GARVIN COUNTY & SE":
    #     print("debugger")
    # print(curr_row)
    sections_list = []
    # the below loop is for the case "SECTION 04/15N/09W & SECTION 33/16N"
    if curr_row.count("SECTION") > 0:
        curr_row = curr_row.replace("\n", "")
        sections_list = re.findall(r"[SEC]*[TION]*[S]*\s+[0-9]{1,2}[/][0-9]{1,2}[A-Z]+[/][0-9]{1,2}[A-Z]+",
                                   curr_row)
        # print(sections_list, " seclis")

    # second half regex to handle SECTION 30 & W/2 SECTION 31
    #         SECTION 31 / 1N / 3W
    if len(sections_list) == 0:
        sections_list = re.findall(r"SEC[TION]*[S]*\s+[0-9]{1,2}\s+&*,*[&*\s*\w*[/]*\w*\s*]*[SEC]*[TION]*\s*[0-9]{0,2}",
                                   curr_row)
        if len(sections_list) > 0:
            str_loc = ''.join(sections_list)
            sections_list = [s for s in str_loc.split() if s.isdigit()]
            # print(sections_list," fmt")
    if len(sections_list) == 0:
        sections_list = re.findall(r"SECTION\s+[[0-9]+,\s*[0-9]*\s*]+", curr_row)
        # print(sections_list)
    # CTION 6 / 1S / 3W
    if len(sections_list) == 0:
        curr_row = curr_row.replace("\n", "")
        sections_list = re.findall(r"[SEC]*[TION]*[S]*\s+[0-9]{1,2}[/][0-9]{1,2}[A-Z]+[/][0-9]{1,2}[A-Z]+", curr_row)
    if len(sections_list) > 0:
        sections_list = ','.join(str(e) for e in sections_list)
    else:
        sections_list = ""
    # print(sections_list)
    return sections_list


# function applied on dataframe
# generating trsm value for all the rows in a dtaframe by taking one row at a time
def trsm(row_data):
    inp_row = row_data[len(row_data) - 4:len(row_data)]
    # tr, sections, extra sections, county
    intial_section = inp_row[1]
    t_r = inp_row[0].upper()
    county = inp_row[3]
    section_data = inp_row[2]
    result = ""
    if section_data != "":
        sections = section_data.split(",")
        if intial_section not in section_data:
            sections.extend(intial_section)
    else:
        sections = [intial_section]
    result = ""
    temp = ""
    # if '35/10N/9W' in sections[0]:
    #     print("debugger")
    '''
    note: some townships will be of form T6N and some ranges will be of form R6W. so care should be taken to remove
    T and R from them. In the below code we will be replacing T and R from thr township and range values
    '''
    for s_no in range(0, len(sections)):

        if re.match("^[A-Z]{0,}\s*[0-9]{1,2}/[0-9]{1,2}[A-Z]+", sections[s_no]) is not None:
            data_start = re.search("\d", sections[s_no])
            sections[s_no] = sections[s_no][data_start.start():].strip()
            temp_var = sections[s_no].split(" ")[0]
            t_row = temp_var.split("/")
            curr = t_row[1].replace('T','').zfill(3) + "-" + t_row[2].replace('R','').zfill(3) + "," + t_row[0].zfill(2)
        else:
            if t_r.strip()!="":
                # print(t_r," ",sections[s_no])
                t_row2 = t_r.split("-")
                # print(t_row2)
                curr = t_row2[0].replace('T','').zfill(3) + "-" + t_row2[1].replace('R','').zfill(3) + "," + sections[s_no].strip().zfill(2)
            else:
                # print(row_data)
                if sections[s_no].strip()!="":
                   curr = 'NULL' + "," + sections[s_no].strip().zfill(2)
                else:
                    curr="NULL"
        if county.lower() == "texas" or county.lower() == "cimarron" or county.lower() == "beaver":
            curr = curr + " " + "GM"
        else:
            if county.upper!='STATEWIDE' or county.upper!='NOT DENOTED' or county.strip()!="" and "NULL" not in curr:
             if "NULL" in curr:
                curr = "NULL"
             else:
               curr = curr + " " + "IM"
            else:
                # if "NULL" in curr:
                    curr = "NULL"
                # else:
                #     curr = curr + " " + "NULL"
        result = temp + curr
        temp = curr + "|"
    # print(result)
    return result


"""
Split the values of a trsm_heh column if more than one values present  and expand so the new DataFrame has one split
value per row. Filters rows where the column is missing.

Params
df : pandas.DataFrame
    dataframe with the column to split and expand
column : str
    the column to split and expand
sep : str
    the string used to split the column's values
keep : bool
    whether to retain the presplit value as it's own row

Returns
pandas.DataFrame
    Returns a dataframe with the same columns as `df`.
"""
def tidy_split(df, column, sep='|', keep=False):
    # split_trsm = df_modify[column].str.split('|').apply(pd.Series, 1).stack()
    # split_trsm.index = split_trsm.index.droplevel(-1)  # to line up with df's index
    # split_trsm.name = column  # needs a name to join
    # del df_modify[column]
    # df_modify = df_modify.join(split_trsm)
    # return df_modify
    indexes = list()
    new_values = list()
    df = df.dropna(subset=[column])
    for i, presplit in enumerate(df[column].astype(str)):
        values = presplit.split(sep)
        if keep and len(values) > 1:
            indexes.append(i)
            new_values.append(presplit)
        for value in values:
            indexes.append(i)
            new_values.append(value)
    new_df = df.iloc[indexes, :].copy()
    new_df[column] = new_values
    return new_df


# to remove punctuations from dataframe except '&' and '|'
def remove_punctuations(text):
    for punctuation in string.punctuation:
        if punctuation == "&":
            text = text.replace(punctuation, ' & ')
        elif punctuation == "|":
            text = text.replace(punctuation, ' | ')
        if punctuation != "&" and punctuation != "|":
            text = text.replace(punctuation, ' ')
    text = text.replace("L L C", "LLC")
    text = ' '.join(text.split())
    return text



def process_logic():
    global final_df, dates_list
    dataframe_flag = 0
    current_directory = os.getcwd()
    folder_path = os.path.join(current_directory, 'nuance_outputs')
    files_list = [f for f in listdir(folder_path) if isfile(join(folder_path, f))]
    xml_files = list(filter(lambda x: x[-4:] == '.xml', files_list))
    txt_files = list(filter(lambda x: x[-4:] == '.txt', files_list))

    pdf_folder_path = os.path.join(current_directory, 'inputs')
    files_list_pdf = [f for f in listdir(pdf_folder_path) if isfile(join(pdf_folder_path, f))]
    pdf_files = list(filter(lambda x: x[-4:] == '.pdf', files_list_pdf))
    for i in range(len(xml_files)):
        old_dates_list = []
        print(xml_files[i])
        current_doc = xml_files[i]  # 'docket_thu.xml'
        tree = ET.parse(join(folder_path, current_doc))  # 'LEMC (16).xml')
        root = tree.getroot()
        count = 0
        for pages in root:
            page_header = ""
            page_header_flag = 0
            date_list_flag = 0
            page_rows_count = 0
            date_row_flag = 0
            section_flag = 0
            page_header_column = []
            date_column = []
            cause_type_column = []
            cause_number_column = []
            protest_doc_number_column = []
            applicant_column = []
            order_desc_column = []
            tr_column = []
            section_column = []
            continuance_number_column = []
            county_column = []
            representative_column = []
            comments_column = []
            document_url_column = []
            entire_applicant_column = []
            entire_order_description_column = []
            doc_cadence_column=[]
            protest_doc_number_count = 0
            cause_type_count = 0
            cause_number_count = 0
            order_desc_count = 0
            row_count = 0
            applicant_name_flag = 0
            section3_rows_count = 0
            legals_regex_miscatch_flag = 0
            error_flag = 0

            # pro_doc_num_flag=0
            # non_pro_doc_num_flag=0
            unwanted_pages = 0
            for zones in pages:
                # to remove public utility pages
                if unwanted_pages == 1:
                    break
                for textZone in zones:
                    if unwanted_pages == 1:
                        break
                    for ln in textZone:
                        line = ""
                        current_baseline = int(ln.attrib.get('baseLine'))
                        current_b_value = int(ln.attrib.get('b'))
                        # reading all the text data present in one line to variable line
                        for data_1 in ln.itertext():
                            line = line + data_1.strip() + " "
                        line = line.strip().upper()
                        line = " ".join(line.split())
                        # debugger point
                        # if "1 11N 06W IM 8 ? 08:30 AM NO JUDGE SPECIFIED" in line.strip():
                        #     print("debugger0")
                        # if count==115:
                        #    print(line+" ", count)
                        # we will be eliminating the pages of type public utility in a given document
                        if line == "PUBLIC UTILITY":  # or line =="POLLUTION AND ENFORCEMENT / GAS GATHERING":
                            unwanted_pages = 1
                            break

                        if date_list_flag == 0:
                            curr_date = find_date(line)
                            if curr_date != "":
                                if curr_date not in dates_list:
                                    dates_list.extend(curr_date)
                                date_list_flag = 1

                        # first_set of columns
                        if section_flag == 0:
                            # if count == 1:
                            #     print("debugger0")
                            # extract date
                            if re.match("^\s*[0-9]{1,2}/[0-9]{1,2}$", line) is not None:
                                date_column.append(line)
                                row_count += 1
                                if date_row_flag == 0:
                                    page_header = prev_line
                                    count += 1
                                date_row_flag = 1
                            # extract cause type
                            elif date_row_flag == 1 and re.match("^\s*[A-Z]{1,3}$", line) is not None:
                                cause_type_column.append(line)
                                cause_type_count += 1
                            # extract cause number
                            elif re.match("^[0-9]{4,}\s*#*\s*[0-9]{0,3}$", line) is not None:
                                cause_pro_number = line.split("#")
                                cause_number_column.append(cause_pro_number[0])
                                cause_number_count += 1
                                # extract protest docket number
                                if len(cause_pro_number) > 1:
                                    protest_doc_number_column.append(cause_pro_number[1])
                                    protest_doc_number_count += 1
                            # this is special case for protest_dockets
                            elif protest_doc_number_count == row_count and row_count > 0:
                                extra_sections_column = ["" for leng in range(row_count)]
                                section_flag += 1
                            elif cause_type_count == row_count and protest_doc_number_count == 0 and row_count > 0:
                                extra_sections_column = ["" for leng in range(row_count)]
                                section_flag += 1

                        # second set of columns
                        if section_flag == 1:
                            # if count==2:
                            #     print("debugger1")
                            if applicant_name_flag == 0:
                                applicant_column.append(line)
                                entire_applicant_column.append(line)
                                applicant_name_flag += 1
                            elif applicant_name_flag == 1:
                                line = " ".join(line.split())
                                entire_applicant_column[len(entire_applicant_column) - 1] = entire_applicant_column[len(
                                    entire_applicant_column) - 1] + "|" + line
                                # gets order description
                                if line.upper() in order_description:
                                    order_desc_column.append(line)
                                    applicant_name_flag += 1
                                    order_desc_count += 1
                                # if exists two applicants
                                else:
                                    applicant_column[len(applicant_column) - 1] = applicant_column[len(
                                        applicant_column) - 1] + "|" + line
                            elif re.match("^[0-9]{1,2}\s+[T,R]{0,1}[0-9]{1,2}[A-Z]{1,2}\s+[T,R]{0,1}[0-9]{1,2}[A-Z]{1,2}\s+",
                                          line) is not None:
                                section_flag = 2
                            # to identify the start of new  data point for the same column
                            elif applicant_name_flag >= 2 and (current_baseline - prev_baseline > 400) and (
                                    current_b_value - prev_b_value > 400):
                                # print(line)
                                # end of a current rows column data
                                entire_applicant_column.append(line)
                                applicant_column.append(line)
                                applicant_name_flag = 1
                                # print(ln.a)
                            else:
                                # to identify 3set of rows in case of legal is missing in data
                                if applicant_name_flag >= 2 and order_desc_count == row_count and (
                                        re.match("^[0-9]{1,2}\s+[?]*[0-9,A-Z]{0,2}\s+[0-9]{1,2}",
                                                 line) is not None or re.match("^[0-9]{1,2}$",
                                                                               line) is not None):
                                    section_flag = 2
                                else:
                                    entire_applicant_column[len(entire_applicant_column) - 1] = entire_applicant_column[
                                                                                                    len(
                                                                                                        entire_applicant_column) - 1] + "|" + line
                                    sec_result = identify_extra_sections(line)
                                    if len(sec_result) != 0:
                                        extra_sections_column[len(order_desc_column) - 1] = sec_result
                                # print(line)


                        # third set of columns
                        if section_flag == 2:
                            # if count==115:
                            #     print("debugger2")
                            # extract twnship, range, section, continuance number
                            if re.match("^[0-9]{1,2}\s+[T,R]{0,1}[0-9]{1,2}[A-Z]{1,2}\s+[T,R]{0,1}[0-9]{1,2}[A-Z]{1,2}\s+",
                                        line) is not None:
                                if legals_regex_miscatch_flag == 1:
                                    try:
                                        entire_order_description_column[len(entire_order_description_column) - 1] = line
                                        tr_column = tr_column[0:len(tr_column) - 1]
                                        print(section_column)
                                        section_column = trsm_split[0:len(section_column) - 1]
                                        continuance_number_column = continuance_number_column[
                                                                    0:len(continuance_number_column) - 1]
                                        legals_regex_miscatch_flag = 0
                                    except:
                                        legals_regex_miscatch_flag = 0
                                else:
                                    entire_order_description_column.append(line)
                                trsm_split = line.split()
                                tr_value = trsm_split[1] + "-" + trsm_split[2]
                                tr_column.append(tr_value)
                                section_column.append(trsm_split[0])
                                if trsm_split[3].isdigit():
                                    continuance_number_column.append(trsm_split[3])
                                else:
                                    try:
                                        continuance_number_column.append(trsm_split[4])
                                    except:
                                        continuance_number_column.append("NULL")
                                        # print(trsm_split, " continuance_number_error")
                                section3_rows_count = 1
                            # extract county and represnetative
                            elif section3_rows_count == 1:
                                entire_order_description_column[len(entire_order_description_column) - 1] = \
                                    entire_order_description_column[len(
                                        entire_order_description_column) - 1] + "|" + line
                                legals_regex_miscatch_flag = 0
                                cr = line.split(" ", 1)

                                if cr[0] not in counties_list:
                                    county_name = [x1 for x1 in counties_list if x1 in line]
                                    cr = line.split(county_name[0])
                                    county_column.append(county_name[0])
                                else:
                                    county_column.append(cr[0])
                                if len(cr) > 1:
                                    representative_column.append(cr[1])
                                else:
                                    representative_column.append("NULL")
                                section3_rows_count += 1
                            # extract comment
                            elif section3_rows_count == 2:
                                entire_order_description_column[len(entire_order_description_column) - 1] = \
                                    entire_order_description_column[len(
                                        entire_order_description_column) - 1] + " | " + line
                                comments_column.append(line)
                                section3_rows_count += 1
                            # if entire legals info is not present and only partly present
                            elif re.match("^[0-9]{1,2}\s+[?]*[0-9,A-Z]{0,2}\s+[0-9]{1,2}",
                                          line) is not None or re.match(
                                "^[0-9]{1,2}$", line) is not None:
                                entire_order_description_column.append(line)
                                legals_regex_miscatch_flag = 1
                                tr_column.append(" ")
                                section_column.append(" ")
                                continuance_number_column.append(line.split()[0])
                                section3_rows_count = 1
                            elif section3_rows_count > 2:
                                entire_order_description_column[len(entire_order_description_column) - 1] = \
                                    entire_order_description_column[len(
                                        entire_order_description_column) - 1] + " | " + line
                                comments_column[len(comments_column) - 1] = comments_column[
                                                                                len(comments_column) - 1] + " | " + line

                        prev_line = line
                        prev_baseline = int(ln.attrib.get('baseLine'))
                        prev_b_value = int(ln.attrib.get('b'))

            if date_column:  # print(line)
                # print(page_header)
                # print(len(page_header))
                # print(date_column, len(date_column), "  count: ", count)
                # print(cause_type_column, len(cause_type_column))
                # print(cause_number_column, len(cause_number_column))
                # print(protest_doc_number_column, len(protest_doc_number_column))
                # print(applicant_column, len(applicant_column))
                # print(order_desc_column, len(order_desc_column))
                # print(tr_column, len(tr_column))
                # print(section_column, len(section_column))
                # print(county_column, len(county_column))
                # print(continuance_number_column, len(continuance_number_column))
                # print(representative_column, len(representative_column))
                # print(comments_column, len(representative_column))
                # since protest_doc_number is only present for protest hearing dockets. we will adding null value for all other dockets
                protest_doc_number_column = pad(protest_doc_number_column, "NULL", len(date_column))
                page_header_column = pad(page_header_column, page_header, len(date_column))
                doc_cadence_column = pad(doc_cadence_column, "daily_weekly", len(date_column))
                dockets_storing_address = folder_path + "\\" + xml_files[i].split(".")[0] + ".pdf"
                document_url_column = pad(document_url_column, dockets_storing_address, len(date_column))
                # tr_column = pad(tr_column," ",len(date_column))
                # section_column = pad(section_column," ",len(date_column))
                # continuance_number_column = pad(continuance_number_column," ",len(date_column))
                # if count == 199:
                #     print("debugger")
                #     print(page_header)
                #     print(len(page_header))
                #     print(date_column, len(date_column), "  count: ", count)
                #     print(cause_type_column, len(cause_type_column))
                #     print(cause_number_column, len(cause_number_column))
                #     print(protest_doc_number_column, len(protest_doc_number_column))
                #     print(applicant_column, len(applicant_column))
                #     print(order_desc_column, len(order_desc_column))
                #     print(tr_column, len(tr_column))
                #     print(section_column, len(section_column))
                #     print(county_column, len(county_column))
                #     print(continuance_number_column, len(continuance_number_column))
                #     print(representative_column, len(representative_column))
                #     print(comments_column, len(representative_column))

                try:
                    # print(cause_number_column)
                    df1 = pd.DataFrame(
                        {'docket_type': page_header_column, 'cause_number': cause_number_column,
                         'cause_type': cause_type_column, 'date': date_column, 'cause': cause_type_column,
                         'applicant': applicant_column, 'order_desc': order_desc_column,
                         'continuance_number': continuance_number_column,
                         'representative': representative_column, 'comment': comments_column,
                         'protest_docket_number': protest_doc_number_column, 'document_url': document_url_column,'doc_cadence':doc_cadence_column,
                         'applicant/respondent': entire_applicant_column,
                         'order_description/relief/title': entire_order_description_column, 'tr': tr_column,
                         'sections': section_column, 'extra_sections': extra_sections_column, 'county': county_column})
                    # print( count)
                except:

                    # error occurs if there is any mismatch list size. the main possible reason for error is presence of unexopected characters or
                    # complete change in format6
                    #     error_flag = 1
                    print("error ", count)
                    continue

                if dataframe_flag == 0:
                    dataframe_flag = 1
                    final_df = df1
                    # print(final_df.shape, " ", xml_files[i])
                else:
                    final_df = pd.concat([final_df, df1], ignore_index=True)
                    # print(df1.shape, " ", xml_files[i])
                error_flag = 0
        inp_file_name = xml_files[i]
        inp_file_name = inp_file_name.split(".")[0]
        # basing on the unique dates, we will classify input dockets to daily and weekly
        # curr_doc_dates = list(set(old_dates_list)- set(dates_list))
        # print(curr_doc_dates)
        final_df['date'] = final_df['date'].apply(apply_format_df)
        curr_doc_dates = final_df[final_df["document_url"]==dockets_storing_address]['date'].unique()
        curr_doc_dates = sorted(curr_doc_dates, key=lambda x: datetime.datetime.strptime(x, '%Y-%m-%d'))
        print(curr_doc_dates)
        if len(set(curr_doc_dates)) > 2:
            blob_folder = 'occ/dockets/outputs/weekly'
            archive_blob_folder = 'dockets/weekly/'
            docket_name = "weekly_docket_"+curr_doc_dates[0]
            final_df["doc_cadence"] = final_df['doc_cadence'].replace(['daily_weekly'], 'weekly')
            print(docket_name)
            # rename_date = dates_list[0].split("-")
            # db_table_name = "occ_dockets_weekly_"+rename_date[0]+"_"+rename_date[1]+"_"+rename_date[2]
        else:
            blob_folder = 'occ/dockets/outputs/daily'
            archive_blob_folder = 'dockets/daily/'
            final_df["doc_cadence"] = final_df['doc_cadence'].replace(['daily_weekly'], 'daily')
            docket_name = "daily_docket_" + curr_doc_dates[0]
        block_blob_service = BlockBlobService(account_name='syntax',account_key='NRdz4AAsiGv5Hnug1dEHWJJvI6tT7q9H0MBJnL1kcRlPwbVw+EKTFpLZvd3SlJXqsSylTknfps/R3aCyenh7Lg==')

        inp_folder =  os.path.join(current_directory, 'inputs')
        nuance_op_folder = os.path.join(current_directory, 'nuance_outputs')
        curr_xml_file_path = os.path.join(nuance_op_folder, xml_files[i])
        curr_txt_file_path = os.path.join(nuance_op_folder, txt_files[i])
        curr_pdf_file_path = os.path.join(inp_folder, pdf_files[i])
        block_blob_service.create_blob_from_path('syntax', blob_folder + "/xml_files/" + docket_name + ".xml",
                                                 curr_xml_file_path)

        block_blob_service.create_blob_from_path('syntax', blob_folder + "/txt_files/" + docket_name + ".txt",
                                                 curr_txt_file_path)

        block_blob_service.create_blob_from_path('cg-archive', archive_blob_folder  + docket_name + ".pdf",
                                                 curr_pdf_file_path)

        # print("total Pages ", count)
        blob_url = block_blob_service.make_blob_url('cg-archive', archive_blob_folder + docket_name + ".pdf")
        # print(dockets_storing_address)
        final_df["document_url"] = final_df['document_url'].replace([dockets_storing_address], blob_url)
        print(final_df.shape, " ", xml_files[i])

    final_df['order_desc'] = final_df['order_desc'].replace(
        ['MULTIUNIT HORIZ WELL', 'HORIZ LOC EXCE-PTION', 'HORIZONTAL SPACING'],
        ['MULTI UNIT', 'LOCATION EXCEPTION', 'SPACING'])

    final_df["applicant"] = final_df['applicant'].apply(remove_punctuations)
    final_df["representative"] = final_df['representative'].apply(remove_punctuations)
    final_df["docket_type"] = final_df['docket_type'].apply(remove_punctuations)
    final_df["order_desc"] = final_df['order_desc'].apply(remove_punctuations)
    # print(dates_list)
    # print(final_df['date'].head(10))
    final_df['date'] = final_df['date'].apply(apply_format_df)
    final_df["trsm_heh"] = final_df.apply(trsm, axis=1)
    final_df['legal_from_source'] = final_df['tr'].astype(str) + final_df['sections']
    final_df = tidy_split(final_df, 'trsm_heh', sep='|')
    final_df = final_df[
        ['docket_type', 'date', 'cause_number', 'cause_type', 'applicant', 'order_desc', 'trsm_heh',
         'continuance_number', 'county', 'representative', 'comment', 'legal_from_source', "protest_docket_number",
         'document_url','doc_cadence']]
    common_cols = ['docket_type', 'date', 'cause_number', 'cause_type', 'applicant', 'order_desc', 'trsm_heh',
                   'continuance_number', 'county', 'representative', 'comment', 'legal_from_source',
                   'protest_docket_number']  # , 'applicant/respondent','order_description/relief/title'
    final_df['duplicates_count'] = final_df.groupby(common_cols)['docket_type'].transform('size')
    final_df['duplicates_count'] = final_df['duplicates_count'] - 1
    final_df = final_df.drop_duplicates(common_cols, keep='first').reset_index(drop=True)
    final_df.loc[final_df.duplicates_count == 1, 'doc_cadence'] = "daily,weekly"
    # final_df = final_df.astype(str)
    # final_df = final_df.groupby(final_df.columns.tolist()).size().reset_index().rename(columns={0:'count'})
    # print(final_df['date'])

    # code to write data to sql
    stmt = "SELECT TABLE_NAME FROM stg_cg_export" + ".INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'r_occ_dockets'"
    cursor.execute(stmt)
    result = cursor.fetchone()
    #print(final_df.shape)
    #print(final_df.columns)
    added_rows=0
    # final order of columns
    final_df = final_df[
        ['docket_type', 'date', 'cause_number', 'cause_type', 'applicant', 'order_desc', 'trsm_heh',
         'continuance_number', 'county', 'representative', 'comment', 'legal_from_source', "protest_docket_number",
         'document_url','doc_cadence','duplicates_count']]
    if not result:
        cursor.execute("CREATE TABLE r_occ_dockets" + "( docket_type VARCHAR(255), date VARCHAR(25), cause_number VARCHAR(25), cause_type VARCHAR(155), applicant VARCHAR(255), order_desc VARCHAR(255), trsm_heh VARCHAR(255),continuance_number VARCHAR(255), county VARCHAR(255), representative VARCHAR(255), comment VARCHAR(255), legal_from_source VARCHAR(255), protest_docket_number VARCHAR(255), document_url  VARCHAR(255),  doc_cadence  VARCHAR(50), duplicate_flag  VARCHAR(50))")
    for row_count in range(5, final_df.shape[0]):
        chunk = final_df.iloc[row_count:row_count + 1, :].values.tolist()
        tuple_of_tuples = tuple(tuple(x) for x in chunk)
        chunk1 = chunk[0]
        # 'comment=' + "'" + chunk1[10].replace("'", "") + "'" + ' AND ' +
        stmt = "SELECT * FROM r_occ_dockets where " + 'docket_type='+"'"+chunk1[0]+"'"+' AND '+ 'date='+"'"+chunk1[1]+"'"+' AND '+ 'cause_number='+"'"+chunk1[2]+"'"+' AND '+ 'cause_type='+"'"+ chunk1[3]+"'"+' AND '+'applicant='+"'"+ chunk1[4]+"'"+' AND '+'order_desc='+"'"+chunk1[5]+ "'"+' AND '+'trsm_heh='+"'"+chunk1[6]+"'"+' AND '+'continuance_number='+"'"+ chunk1[7]+"'"+' AND '+'county='+"'"+chunk1[8]+ "'"+' AND '+'representative='+"'"+ chunk1[9]+"'"+' AND '+'legal_from_source='+"'"+chunk1[11]+"'"+' AND '+'protest_docket_number='+"'"+chunk1[12]+"'"
        # print(stmt)
        cursor.execute(stmt)
        exist = cursor.fetchone()
        if not exist:
            cursor.executemany(
                "insert into r_occ_dockets"+ " (docket_type,date,cause_number,cause_type,applicant,order_desc,trsm_heh,continuance_number,county,representative,comment,legal_from_source,protest_docket_number,document_url,doc_cadence, duplicate_flag) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple_of_tuples)
            # print(stmt)
            added_rows = added_rows + 1
    return added_rows
    # if section_flag==2:

def main_process():
     op_result= process_logic()
     return op_result

    # cursor.executemany(
    #     "insert into occ_dockets" + " (docket_type,date,cause_number,cause_type,applicant,order_desc,trsm_heh,continuance_number,county,representative,comment,legal_from_source,protest_docket_number,document_url, duplicate_flag) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
    #     tuple_of_tuples)
