from ServerUtilities import encodeRequest, oracleOutputMapping
from Core import getHashLfn


class update(object):

    def __init__(self, logger, oracleDB, config):
        self.oracleDB = oracleDB
        self.config = config
        self.logger = logger

    def transferred(self, files):
        """
        Mark the list of files as tranferred
        """
        lfn_done = []
        dash_rep = ()
        for lfn in files:
            lfn = lfn[0]
            document = dict()
            if lfn.find('temp') == 7:
                docId = getHashLfn(lfn)
                self.logger.debug("Marking done %s" % lfn)
                self.logger.debug("Marking done %s" % docId)
                try:
                    docbyId = self.oracleDB.get(self.config.oracleFileTrans.replace('filetransfers',
                                                                                    'fileusertransfers'),
                                                data=encodeRequest({'subresource': 'getById', 'id': docId}))
                    document = oracleOutputMapping(docbyId, None)[0]
                    fileDoc = dict()
                    fileDoc['asoworker'] = self.config.asoworker
                    fileDoc['subresource'] = 'updateTransfers'
                    fileDoc['list_of_ids'] = docId
                    fileDoc['list_of_transfer_state'] = "DONE"

                    result = self.oracleDB.post(self.config.oracleFileTrans,
                                                data=encodeRequest(fileDoc))
                except Exception as ex:
                    self.logger.error("Error during status update: %s" %ex)

                lfn_done.append(lfn)
                dash_rep = (document['jobid'], document['job_retry_count'], document['taskname'])
                self.logger.debug("Marked acquired %s of %s" % (docId, lfn))
        return lfn_done, dash_rep

    def acquired(self, files):
        """
        Mark the list of files as tranferred
        """
        lfn_in_transfer = []
        dash_rep = ()
        for lfn in files:
            lfn = lfn[0]
            if lfn.find('temp') == 7:
                docId = getHashLfn(lfn)
                self.logger.debug("Marking acquired %s" % lfn)
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

    def failed(self, files, failures_reasons=[], max_retry=3, force_fail=False, submission_error=False):
        """
        Something failed for these files so increment the retry count
        """
        updated_lfn = []
        for Lfn in files:
            lfn = Lfn[0]
            # Load document and get the retry_count
            docId = getHashLfn(lfn)
            self.logger.debug("Marking failed %s" % docId)
            try:
                docbyId = self.oracleDB.get(self.config.oracleUserFileTrans,
                                            data=encodeRequest({'subresource': 'getById', 'id': docId}))
                document = oracleOutputMapping(docbyId, None)[0]
                self.logger.debug("Document: %s" % document)
            except Exception as ex:
                self.logger.error("Error updating failed docs: %s" % ex)
                continue

            fileDoc = dict()
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'updateTransfers'
            fileDoc['list_of_ids'] = docId
            if not len(failures_reasons) == 0:
                try:
                    fileDoc['list_of_failure_reason'] = failures_reasons[files.index(Lfn)]
                except:
                    self.logger.exception("missing reasons")

            if force_fail or document['transfer_retry_count'] + 1 > max_retry:
                fileDoc['list_of_transfer_state'] = 'FAILED'
                fileDoc['list_of_retry_value'] = 1
            else:
                fileDoc['list_of_transfer_state'] = 'RETRY'

            if submission_error:
                fileDoc['list_of_failure_reason'] = "Job could not be submitted to FTS: temporary problem of FTS"
                fileDoc['list_of_retry_value'] = 1
            else:
                # fileDoc['list_of_failure_reason'] = "Site config problem."
                fileDoc['list_of_retry_value'] = 1

            self.logger.debug("update: %s" % fileDoc)
            try:
                updated_lfn.append(docId)
                result = self.oracleDB.post(self.config.oracleFileTrans,
                                            data=encodeRequest(fileDoc))
            except Exception as ex:
                self.logger.exception()
                continue
        self.logger.debug("failed file updated")
        return updated_lfn
