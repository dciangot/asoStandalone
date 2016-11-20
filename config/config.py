from WMCore.Configuration import Configuration

config = Configuration()

TEST = True 

getter = config.section_('Getter')
getter.opsProxy = "/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy"
getter.oracleDB = "cmsweb-testbed.cern.ch"
getter.oracleFileTrans = "/crabserver/preprod/filetransfers"
getter.oracleUserFileTrans = "/crabserver/preprod/fileusertransfers"
getter.asoworker = "asodciangot1"
getter.max_threads_num = 10
getter.pool_size = 100
getter.files_per_job = 200
getter.credentialDir = '/data/srv/asyncstageout/state/asyncstageout/creds'
getter.serverDN = 'asotest2.cern.ch'
getter.cache_area = 'https://cmsweb-testbed.cern.ch/crabserver/preprod/filemetadata'
getter.serviceCert = '/data/certs/hostcert.pem'
getter.serviceKey = '/data/certs/hostkey.pem'
getter.serverFTS = 'https://fts3.cern.ch:8446'
getter.cooloffTime = 7200
getter.TEST = TEST

monitor = config.section_('Monitor')
monitor.max_threads_num = 10
monitor.TEST = TEST
