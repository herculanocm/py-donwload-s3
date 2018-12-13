import logging
from datetime import datetime

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

def generateObjectS3AWSList(objectS3AWSList, s3, bucket_name, file_ext_name):
    list=s3.list_objects(Bucket=bucket_name)['Contents']
    for s3_key in list:
        s3_object = s3_key['Key']
        if not s3_object.endswith("/"):
            s3_file_name_split = s3_object.split('/')
            s3_file_name = filterbyvalue(s3_file_name_split, file_ext_name)
            objectS3AWS = ObjectS3AWS(s3_object,s3_file_name,s3_file_name[-14:][:10])
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
        dtaComp = datetime.strptime(obj.data, '%Y-%m-%d')
        # print("data comparacao {}".format(str(dtaComp)))
        if dtaComp >= dtaInicial and dtaComp <= dtaFinal:
            objectS3AWSListFilt.append(obj)
    return objectS3AWSListFilt


