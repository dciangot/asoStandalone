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
        good_ids = list()
        updated_lfn = list()
        for lfn in files:
            lfn = lfn[0]
            if lfn.find('temp') == 7:
                docId = getHashLfn(lfn)
                good_ids.append(docId)
                updated_lfn.append(lfn)
                self.logger.debug("Marking done %s" % lfn)
                self.logger.debug("Marking done %s" % docId)
        try:
            data = dict()
            data['asoworker'] = self.config.asoworker
            data['subresource'] = 'updateTransfers'
            data['list_of_ids'] = good_ids
            data['list_of_transfer_state'] = ["DONE" for x in good_ids]
            result = self.oracleDB.post(self.config.oracleFileTrans,
                                        data=encodeRequest(data))
            self.logger.debug("Marked good %s" % good_ids)
        except Exception as ex:
            self.logger.exception("Error updating document")

        return 0

    def acquired(self, files):
        """
        Mark the list of files as tranferred
        """
        lfn_in_transfer = []
        dash_rep = ()
        id_list = list()
        docId = ''
        for lfn in files:
            lfn = lfn[0]
            if lfn.find('temp') == 7:
                self.logger.debug("Marking acquired %s" % lfn)
                docId = getHashLfn(lfn)
                self.logger.debug("Marking acquired %s" % docId)
                try:
                    docbyId = self.oracleDB.get(
                        self.config.oracleFileTrans.replace('filetransfers', 'fileusertransfers'),
                        data=encodeRequest({'subresource': 'getById', 'id': docId}))
                    document = oracleOutputMapping(docbyId, None)[0]
                    id_list.append(docId)
                    lfn_in_transfer.append(lfn)
                except Exception as ex:
                    self.logger.error("Error during status update: %s" % ex)

            lfn_in_transfer.append(lfn)
            # TODO: add dashboard stuff
            dash_rep = (document['jobid'], document['job_retry_count'], document['taskname'])
            self.logger.debug("Marked acquired %s of %s" % (docId, lfn))
        try:
            fileDoc = dict()
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'updateTransfers'
            fileDoc['list_of_ids'] = id_list
            fileDoc['list_of_transfer_state'] = ["SUBMITTED" for x in id_list]

            result = self.oracleDB.post(self.config.oracleFileTrans,
                                        data=encodeRequest(fileDoc))
            self.logger.debug("Marked acquired %s" % (id_list))
        except Exception as ex:
            self.logger.error("Error during status update: %s" % ex)

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
                    fileDoc['list_of_failure_reason'] = "unexcpected error, missing reasons"
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
                fileDoc['list_of_retry_value'] = 1

            self.logger.debug("update: %s" % fileDoc)
            try:
                updated_lfn.append(docId)
                result = self.oracleDB.post(self.config.oracleFileTrans,
                                            data=encodeRequest(fileDoc))
            except Exception:
                self.logger.exception('ERROR updating failed documents')
                continue
        self.logger.debug("failed file updated")
        return updated_lfn
