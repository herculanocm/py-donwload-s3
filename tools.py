# Desenvolvido por Herculano Cunha - Area de BI - (34) 3218 - 1452 / (34) 9 9794 - 6294

import logging
from datetime import datetime
import re
import os
import shutil
from smb.SMBConnection import  SMBConnection
from smb import smb_structs

class ObjectS3AWS:
    def __init__(self, objectId, nome, data):
        self.objectId = objectId
        self.nome = nome
        self.data = data

    def __str__(self):
        return "ObjectAWS: [objectId : "+self.objectId+", nome: "+self.nome+", data: "+self.data+"]"

def filterbyvalue(seq, value):
   for el in seq:
       if value in el:
            return el

def verificaPastasTemporarias(pathDir):
    if os.path.exists(pathDir) == False:
        os.makedirs(pathDir + "/downloads")
        os.makedirs(pathDir + "/log")

    if os.path.exists(pathDir + "/downloads") == False:
        os.makedirs(pathDir + "/downloads")

    if os.path.exists(pathDir + "/log") == False:
        os.makedirs(pathDir + "/log")

def apagaPastastemporarias(pathDir):
    shutil.rmtree(pathDir)

def getdatebyname(name, file_name_ini, file_ext_name):
    result = re.search(file_name_ini + '(.+?)' + file_ext_name, name)
    return result.group(1)

def generateObjectS3AWSList(objectS3AWSList, s3, bucket_name, prefix_name, file_name_ini, file_ext_name):
    list=s3.list_objects(Bucket=bucket_name, Prefix=prefix_name)['Contents']
    # print('Printando a lista')
    # print(list)
    for s3_key in list:
        s3_object = s3_key['Key']
        # print('Printando o objeto')
        # print(s3_object)
        if not s3_object.endswith("/"):
            s3_file_name_split = s3_object.split('/')
            # print('Printando file name split')
            # print(s3_file_name_split)
            s3_file_name = filterbyvalue(s3_file_name_split, file_ext_name)
            # print('Printando file name')
            # print(s3_file_name)
            data_objeto = getdatebyname(s3_file_name, file_name_ini, file_ext_name)
            # print (data_objeto)
            objectS3AWS = ObjectS3AWS(s3_object,s3_file_name,data_objeto)
            # print('Printando objectS3AWS')
            # print(objectS3AWS)
            objectS3AWSList.append(objectS3AWS)
    return objectS3AWSList

def config_logger(file_path, file_name):
    logger = logging.getLogger('exportacao1')
    logger.setLevel(logging.DEBUG)
    formato = '%(asctime)s - %(lineno)d - %(levelname)-8s - %(message)s'

    ch = logging.StreamHandler() #StreamHandler logs to console
    ch.setLevel(logging.DEBUG)
    ch_format = logging.Formatter(formato)
    ch.setFormatter(ch_format)
    logger.addHandler(ch)


    fh = logging.FileHandler("{0}.log".format(file_path + file_name))
    fh.setLevel(logging.DEBUG)
    fh_format = logging.Formatter(formato)
    fh.setFormatter(fh_format)
    logger.addHandler(fh)

    return logger

def shutdownLogger():
    logging.shutdown()

def filterList(objectS3AWSList, strDtaInicial, strDtaFinal):
    dtaInicial = datetime.strptime(strDtaInicial, '%Y-%m-%d')
    dtaFinal = datetime.strptime(strDtaFinal, '%Y-%m-%d')
    # print("data inicial {}, data final {}".format(str(dtaInicial), str(strDtaFinal)))
    objectS3AWSListFilt = []
    for obj in objectS3AWSList:
        # print(obj)
        dtaComp = datetime.strptime(obj.data, '%Y-%m-%d')
        # print("data comparacao {}".format(str(dtaComp)))
        if dtaComp >= dtaInicial and dtaComp <= dtaFinal:
            objectS3AWSListFilt.append(obj)
    return objectS3AWSListFilt

def connect(username, password, my_name, remote_name, domain):
  smb_structs.SUPPORT_SMB2 = True
  conn = SMBConnection(username, password, my_name, remote_name, domain, use_ntlm_v2 = True, sign_options=SMBConnection.SIGN_WHEN_SUPPORTED, is_direct_tcp=True)
  try:
    conn.connect(remote_name, 445) #139=NetBIOS / 445=TCP
  except Exception, e:
    print e
  return conn

def getServiceName(conn):
  if conn:
    shares = conn.listShares()
    for s in shares:
      if s.type == 0:  # 0 = DISK_TREE
        return s.name
    conn.close()
  else:
    return ''

def getListDownloadFiles():
    files_download = []
    for files in os.listdir("./temp/downloads"):
        files_download.append(files)
    return files_download

def getListLogFiles():
    files_log = []
    for files in os.listdir("./temp/log"):
        files_log.append(files)
    return files_log
