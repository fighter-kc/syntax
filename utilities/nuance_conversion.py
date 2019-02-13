import requests
import json
import os
import joblib
from os import listdir
from os.path import isfile, join
from joblib import Parallel, delayed







serviceBaseURL = "http://172.16.0.109/Nuance.OmniPage.Server.Service/"
inputFileNames =[""for i in range(2)]
file_formats = {"200":"xml", "1":"txt","8":"XLS", "10":"DOC","34":"XLSX"}




def parallel_processing(inp, op_folder,job_id_type,workflow_files, op_file_types):
    # print(workflow_files)
    workflow_files_count = len(workflow_files)
    count=0
    while(count<workflow_files_count):
        if count ==0:
         inputFileNames[0] = workflow_files[0]
        else:
         inputFileNames[count] = workflow_files[count]
        count+=1
    inputFileNames.append(inp)
    # create_job function with job type id as varuiable
    request_url="http://172.16.0.109/Nuance.OmniPage.Server.Service/api/Job/CreateJob?jobTypeId="+job_id_type
    x=requests.get(request_url, headers={"Content-Type": "application/json"})
    job_id=x.text.replace('"',"")
    print(job_id," nuance_job_id")



    # get a list of upload urls
    upload_url = "http://172.16.0.109/Nuance.OmniPage.Server.Service/api/Job/GetUploadUrls?jobId="+job_id+"&count="+str(len(workflow_files)+1)
    # print(upload_url)
    upload_url_list=requests.get(upload_url, headers={"Content-Type": "application/json;charset=utf-8"})
    urls_string=upload_url_list.text
    # print(urls_string,"  urlsstring")
    # output is a string which is a list, eleiminatinng the columns and converting back to list of urls
    urls_list=urls_string[2:-1].replace('"',"").split(",")
    # print(inp," urls_list")


    # uploading workflow and xml files to the url path
    for j in range(0, len(urls_list)):
        if job_id_type!="200":
            with open(inputFileNames[1].strip(),'rb') as filedata:
              r = requests.post(urls_list[j], files={'file': filedata})
            break
        else:
            with open(inputFileNames[j].strip(),'rb') as filedata:
              r = requests.post(urls_list[j], files={'file': filedata})

    # setting flag for workflow xml files
    if job_id_type=="200":
        url = "http://172.16.0.109/Nuance.OmniPage.Server.Service/api/job/SetJobDataDescription?jobId="+job_id
        
        json_data = {"FileDescriptions":[{"DocumentId":0,"FileIndex":0,"FileType":10,"PageId":0}],"Warnings":[{"Code": 0,"Message": "string","InputDocumentId": 0,"InputPageId": 0,"OutputDocumentId": 0,"OutputPageId": 0}]}
        # changing file type basing on the type of input file
        json_data["FileDescriptions"][0]['FileType'] = 10
        headers = {'Content-type': 'application/json'}
        r = requests.post(url, data=json.dumps(json_data),headers=headers)

        url = "http://172.16.0.109/Nuance.OmniPage.Server.Service/api/job/SetJobDataDescription?jobId="+job_id
        json_data = {"FileDescriptions":[{"DocumentId":0,"FileIndex":1,"FileType":10,"PageId":0}],"Warnings":[{"Code": 0,"Message": "string","InputDocumentId": 0,"InputPageId": 0,"OutputDocumentId": 0,"OutputPageId": 0}]}
        # changing file type basing on the type of input file
        json_data["FileDescriptions"][0]['FileType'] = 6
        headers = {'Content-type': 'application/json'}
        r = requests.post(url, data=json.dumps(json_data),headers=headers)




    # start job
    url = "http://172.16.0.109/Nuance.OmniPage.Server.Service/api/Job/StartJob?jobId="+job_id+"&timeToLiveSec=1000000000&priority=0"
    start_job=requests.get(url)

    #get job status
    url= "http://172.16.0.109/Nuance.OmniPage.Server.Service/api/Job/GetJobsStatus?jobIds="+job_id
    headers = {'Accept': 'application/json;charset=utf-8'}
    response =requests.get(url, headers={"Content-Type": "application/json"})
    json1_data = json.loads(response.text)[0]
    curr_state = json1_data['State']
    while(curr_state<3):
        response = requests.get(url, headers={"Content-Type": "application/json"})
        json1_data = json.loads(response.text)[0]
        curr_state = json1_data['State']



    # get a list of urls to download
    url='http://172.16.0.109/Nuance.OmniPage.Server.Service/api/Job/GetDownloadUrls?jobId='+job_id
    download_urls_string=requests.get(url, headers={"Content-Type": "application/json"})
    # print(download_urls_string.text, " download url for "+inp)
    download_urls = download_urls_string.text
    download_urls_list = download_urls[2:-1].replace('"', "").split(",")
    for j in range(2):
        download_url = download_urls_list[j]
        # download_url = download_url.split("id=")[1]
        #
        #
        # # download url to a file
        # url = "http://nuance.eastus.cloudapp.azure.com:80/Nuance.OmniPage.Server.Service/api/storage/DownloadFile?id="+download_url
        r = requests.get(download_url, headers=headers)
        inp=inp.replace("\\","/")
        output_file_name =  inp.split("/")[-1].split(".")[0]+"."+op_file_types[j]
        print(output_file_name," op_file_name")
        path = os.path.join(op_folder, output_file_name)
        with open(path, 'wb') as f:
            f.write(r.content)
        # print(inp+" successfully downloaded")

# main process where the function starts
def main_process(job_type_id):

    folder_path = os.getcwd()
    inp_folder_path =os.path.join(folder_path,'inputs')
    # print(inp_folder_path)
    files_list = [f for f in listdir(inp_folder_path) if isfile(join(inp_folder_path, f))]
    pdf_files = list(filter(lambda x: x[-4:] == '.pdf', files_list))
    pdf_files=[inp_folder_path+"\\"+s for s in pdf_files]
    op_types=['xml', 'txt']

    xml_files=["UsingZonesTwoOutput.xml","Zones.zon"]
    files_list = [f for f in xml_files if isfile(join(inp_folder_path, f))]
    op_folder=os.path.join(folder_path,'nuance_outputs')
    Parallel(n_jobs=len(pdf_files))(delayed(parallel_processing)(pdf_files[i],op_folder,str(job_type_id),xml_files, op_types) for i in range(len(pdf_files)))




