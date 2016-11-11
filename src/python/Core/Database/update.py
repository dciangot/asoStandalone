from ServerUtilities import encodeRequest, oracleOutputMapping

# TODO: fix marking

class update(object):
    def __init__(self, logger, oracleDB, config):
        self.oracleDB = oracleDB
        self.config = config
        self.logger = logger

    def acquired(self, files=[]):
        """
        Mark the list of files as tranferred
        """
        lfn_in_transfer = []
        dash_rep = ()
        for lfn in files:
            if lfn['value'][0].find('temp') == 7:
                self.logger.debug("Marking acquired %s" % lfn)
                docId = lfn['key'][5]
                self.logger.debug("Marking acquired %s" % docId)
                try:
                    docbyId = self.oracleDB.get(self.config.oracleFileTrans.replace('filetransfers',
                                                                                    'fileusertransfers'),
                                                data=encodeRequest({'subresource': 'getById', 'id': docId}))
                    document = oracleOutputMapping(docbyId, None)[0]
                    fileDoc = dict()
                    fileDoc['asoworker'] = self.config.asoworker
                    fileDoc['subresource'] = 'updateTransfers'
                    fileDoc['list_of_ids'] = docId
                    fileDoc['list_of_transfer_state'] = "SUBMITTED"

                    result = self.oracleDB.post(self.config.oracleFileTrans,
                                                data=encodeRequest(fileDoc))
                except Exception as ex:
                    self.logger.error("Error during status update: %s" %ex)

                lfn_in_transfer.append(lfn)
                dash_rep = (document['jobid'], document['job_retry_count'], document['taskname'])
                self.logger.debug("Marked acquired %s of %s" % (docId, lfn))
        return lfn_in_transfer, dash_rep

    def mark_failed(self, files=[], force_fail=False, submission_error=False):
        """
        Something failed for these files so increment the retry count
        """
        updated_lfn = []
        for lfn in files:
            data = {}
            if not isinstance(lfn, dict):
                if 'temp' not in lfn:
                    temp_lfn = lfn.replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn
            else:
                if 'temp' not in lfn['value'][0]:
                    temp_lfn = lfn['value'][0].replace('store', 'store/temp', 1)
                else:
                    temp_lfn = lfn['value'][0]

            # Load document and get the retry_count
            if self.config.isOracle:
                docId = getHashLfn(temp_lfn)
                self.logger.debug("Marking failed %s" % docId)
                try:
                    docbyId = self.oracleDB.get(self.config.oracleFileTrans,
                                                data=encodeRequest({'subresource': 'getById', 'id': docId}))
                except Exception as ex:
                    self.logger.error("Error updating failed docs: %s" % ex)
                    continue
                document = oracleOutputMapping(docbyId, None)[0]
                self.logger.debug("Document: %s" % document)

                fileDoc = dict()
                fileDoc['asoworker'] = self.config.asoworker
                fileDoc['subresource'] = 'updateTransfers'
                fileDoc['list_of_ids'] = docId

                if force_fail or document['transfer_retry_count'] + 1 > self.max_retry:
                    fileDoc['list_of_transfer_state'] = 'FAILED'
                    fileDoc['list_of_retry_value'] = 1
                else:
                    fileDoc['list_of_transfer_state'] = 'RETRY'
                if submission_error:
                    fileDoc['list_of_failure_reason'] = "Job could not be submitted to FTS: temporary problem of FTS"
                    fileDoc['list_of_retry_value'] = 1
                elif not self.valid_proxy:
                    fileDoc['list_of_failure_reason'] = "Job could not be submitted to FTS: user's proxy expired"
                    fileDoc['list_of_retry_value'] = 1
                else:
                    fileDoc['list_of_failure_reason'] = "Site config problem."
                    fileDoc['list_of_retry_value'] = 1

                self.logger.debug("update: %s" % fileDoc)
                try:
                    updated_lfn.append(docId)
                    result = self.oracleDB.post(self.config.oracleFileTrans,
                                                data=encodeRequest(fileDoc))
                except Exception as ex:
                    msg = "Error updating document"
                    msg += str(ex)
                    msg += str(traceback.format_exc())
                    self.logger.error(msg)
                    continue
            self.logger.debug("failed file updated")
            return updated_lfn