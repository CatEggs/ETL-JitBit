import requests
import boto3
from time import time
from multiprocessing.pool import ThreadPool
import config




def url_response(url,filename):
 
    path = r'C:\Users\cegboh\Desktop\PythonProjects\TicketProject\FD_Attachment_Folder\\'+filename
    url = url
 
    r = requests.get(url, stream = True)
 
    with open(path, 'wb') as f:
 
        for chunk in r.iter_content(chunk_size = 1024):
            if chunk:
                f.write(chunk)

    return path



def s3_filetransfer(url,filename,id):
    s3_path=str(id)+"/"+filename
    r = requests.get(url, stream = True)
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=config.s3_bucketname,
        Key=s3_path,
        Body=''
    )
    bucket_location = s3.get_bucket_location(Bucket=config.s3_bucketname+"/"+s3_path)
    # with open(s3_path, 'wb') as f:

    # for chunk in r.iter_content(chunk_size = 1024):
    #     if chunk:
    #         f.write(chunk)
    filecontent = r.content
    with open(bucket_location, "wb") as f:
        s3.upload_fileobj(f.write(filecontent), config.s3_bucketname, filename)
    return s3.download_file(config.s3_bucketname,s3_path, filename)


#s3_filetransfer('test.xlsx', 'ok.xlsx', 9922)