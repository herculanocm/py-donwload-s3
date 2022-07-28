# Desenvolvido por Herculano Cunha - Area de BI - (34) 3218 - 1452 / (34) 9 9794 - 6294

# Config Log
file_path_log =  'Sistemas/Tresbanco/TransacoesPF/Log/' #Nao alterar
file_name_log = 'transacoesPfBatch' #Nao alterar
# =========================================================================
#Config Amazon S3
bucket_name = 'hml-pd-trb-quicksight'
# bucket_name = 'dsv-bi-tresbanco'
file_ext_name = '.csv'  #Nao alterar
prefix_name='exports-bi-tresbanco/'
# credenciais
aws_access_key_id=''
aws_secret_access_key=''
# credencias de dsv
# aws_access_key_id='AKIAJXLV6FNK4IX4USSA'
# aws_secret_access_key='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'
file_path_downloads = 'Sistemas/Tresbanco/TransacoesPF/'  #Nao alterar
# Usado para captura de data
file_name_ini = 'transacions-' #Nao alterar
# =========================================================================
# Config DFS
name_share = 'SCSI'  #Nao alterar
hostname_dfs = 'trbdfsprd01' #Atual host PRD
domain = 'TRBW2K'  #Nao alterar
username = ''
password = ''
