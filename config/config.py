from WMCore.Configuration import Configuration

config = Configuration()

getter = config.section_('Getter')
getter.opsProxy = "/data/srv/asyncstageout/state/asyncstageout/creds/OpsProxy"
getter.oracleDB = "cmsweb-testbed.cern.ch"
getter.oracleFileTrans = "/crabserver/preprod/filetransfers"
getter.asoworker = "asodciangot1"
getter.max_threads_num = 10
