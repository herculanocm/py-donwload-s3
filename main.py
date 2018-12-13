import boto3
import botocore
import sys
from tools import *
from conf import *

f_logger = config_logger(file_path_log, file_name_log)
f_logger.log(10, 'Iniciando execução')

iterator = 0
strDtaInicial = '2018-11-01'
strDtaFinal = '2020-11-01'
for arg in sys.argv:
    iterator += 1
    if iterator == 2:
        strDtaInicial = arg
    if iterator == 3:
        strDtaFinal = arg

strLog = "Data Inicial {} e final {} obtidos".format(strDtaInicial, strDtaFinal)
f_logger.log(10, strLog)

objectS3AWSList = []

f_logger.log(10, 'Configurando cliente com key id e secret key')
s3=boto3.client(
    's3',
    # Hard coded strings as credentials, not recommended.
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)



try:
    f_logger.log(10, 'Listando objetos bucket : %s', bucket_name)
    objectS3AWSList = generateObjectS3AWSList(objectS3AWSList, s3, bucket_name, file_ext_name)
    f_logger.log(10, 'Objetos do bucket listados com sucesso')
except botocore.exceptions.ClientError as e:
    f_logger.log(40, 'Erro ao listar objetos')

f_logger.log(10, 'Filtrando objetos entre as datas informadas')
objectS3AWSList = filterList(objectS3AWSList, strDtaInicial, strDtaFinal)

for l in objectS3AWSList:
    f_logger.log(10, 'Arquivo informado para download : {}'.format(l))
    try:
        f_logger.log(10, 'Realizando download... : {}'.format(l.nome))
        s3.download_file(bucket_name, l.objectId, file_path_downloads + l.nome)
        f_logger.log(10, 'Download realizado do arquivo : {}, pasta : {}'.format(l.nome, file_path_downloads))
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            f_logger.log(40, 'Arquivo não existe : {}'.format(l))
        else:
            raise

f_logger.log(10, 'Finalizado execução')
shutdownLogger()

