# Desenvolvido por Herculano Cunha - Area de BI - (34) 3218 - 1452 / (34) 9 9794 - 6294

import boto3
import botocore
import sys
from tools import *
from conf import *

def copiaArquivosDownloadCSV(conn):
    list_files_download = getListDownloadFiles()
    for file_download in list_files_download:
        file = "./temp/downloads/" + file_download
        f_logger.log(10, 'Lendo arquivo de csv para mover : {}'.format(file))
        with open(file,'r') as file_obj:
            f_logger.log(10, 'Movendo arquivo csv para DFS... : {}'.format(file_download))
            numBytes = conn.storeFile(name_share, file_path_downloads + file_download ,file_obj)
            f_logger.log(10, 'Arquivo csv movido com sucesso... : {}'.format(file_path_downloads + file_download))

def copiaArquivosLog(conn):
    list_files_log = getListLogFiles()
    for file_log in list_files_log:
        file = './temp/log/' + file_log
        f_logger.log(10, 'Lendo arquivo de log para mover : {}'.format(file))
        with open(file,'r') as file_obj_log:
            f_logger.log(10, 'Movendo arquivo log para DFS e finalizando... : {}'.format(file_log))
            shutdownLogger()
            numBytes = conn.storeFile(name_share, file_path_log + file_log ,file_obj_log)

# apagaPastastemporarias('./temp')

temp_path = "./temp"
verificaPastasTemporarias(temp_path)
f_logger = config_logger(temp_path + "/log/", file_name_log)
f_logger.log(10, 'Iniciando execucao')

try:
    f_logger.log(10, 'Conectando ao DFS....')
    conn = connect(username, password, 'Visitante', hostname_dfs, domain)
    f_logger.log(10, 'DFS conectado! ainda falta autenticacao...')
except Exception, e:
    f_logger.log(40, 'Erro ao conectar ao DFS')
    f_logger.log(40, e)
    shutdownLogger()
    sys.exit()


try:
    iterator = 0
    strDtaInicial = '2018-01-01'
    strDtaFinal = '2020-01-01'
    for arg in sys.argv:
        iterator += 1
        if iterator == 2:
            strDtaInicial = arg
        if iterator == 3:
            strDtaFinal = arg

    f_logger.log(10, "Obtendo parametros de data informados no shell")
    strLog = "Data Inicial {} e final {} obtidos".format(strDtaInicial, strDtaFinal)
    f_logger.log(10, strLog)

    objectS3AWSList = []

    f_logger.log(10, 'Configurando cliente com key id e secret key')
    s3=boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )


    try:
        f_logger.log(10, 'Listando objetos bucket : %s', bucket_name)
        objectS3AWSList = generateObjectS3AWSList(objectS3AWSList, s3, bucket_name, prefix_name, file_name_ini, file_ext_name)
        f_logger.log(10, 'Objetos do bucket listados com sucesso')
    except botocore.exceptions.ClientError as e:
        f_logger.log(40, 'Erro ao listar objetos')
        f_logger.log(40, e)
        copiaArquivosLog(conn)
        conn.close()
        sys.exit()

    f_logger.log(10, 'Filtrando objetos entre as datas informadas')
    objectS3AWSList = filterList(objectS3AWSList, strDtaInicial, strDtaFinal)

    for l in objectS3AWSList:
        f_logger.log(10, 'Arquivo informado para download : {}'.format(l))

        try:
            f_logger.log(10, 'Realizando download... : {}'.format(l.nome))
            s3.download_file(bucket_name, l.objectId, temp_path + "/downloads/" + l.nome)
            f_logger.log(10, 'Download realizado do arquivo : {}, pasta : {}'.format(l.nome, file_path_downloads))
        except botocore.exceptions.ClientError as e:
            f_logger.log(40, 'Erro ao Realizar Download de Arquivo: {}'.format(l))
            if e.response['Error']['Code'] == "404":
                f_logger.log(40, 'Arquivo nao existe : {}'.format(l))

            f_logger.log(40, e)
            copiaArquivosLog(conn)
            conn.close()
            sys.exit()

    f_logger.log(10, 'Finalizado loop de Downloads do S3')




    try:

        copiaArquivosDownloadCSV(conn)

    except Exception, e:
        f_logger.log(40, 'Erro ao mover arquivos CSV para o DFS: {}'.format(e))
        copiaArquivosLog(conn)
        conn.close()
        sys.exit()

    f_logger.log(10, 'Finalizado execucao total com sucesso!...')



except Exception, e:
    f_logger.log(40, 'Erro ao executar carga, Exception principal')
    f_logger.log(40, e)
    copiaArquivosLog(conn)
    conn.close()
    sys.exit()


copiaArquivosLog(conn)
conn.close()
apagaPastastemporarias('./temp')
sys.exit()
