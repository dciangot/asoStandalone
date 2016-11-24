"""
Define all the interations with the ASO DB
"""
from ServerUtilities import encodeRequest, oracleOutputMapping
from Core import getHashLfn
from RESTInteractions import HTTPRequests


class update(object):

    def __init__(self, logger, config):
        """
        Initialize connection to the db and logging/config

        :param logger: pass the logging
        :param config: refer to the configuration file
        """
        self.oracleDB = HTTPRequests(config.oracleDB,
                                     config.opsProxy,
                                     config.opsProxy)

        self.config = config
        self.logger = logger

    def retry(self):
        """
        Retry documents older than self.config.cooloffTime
        :return:
        """
        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'retryTransfers'
        fileDoc['time_to'] = self.config.cooloffTime
        self.logger.debug('fileDoc: %s' % fileDoc)

        results = dict()
        try:
            results = self.oracleDB.post(self.config.oracleFileTrans,
                                         data=encodeRequest(fileDoc))
        except Exception:
            self.logger.exception("Failed to get retry transfers in oracleDB: %s")
        self.logger.info("Retried files in cooloff: %s" % str(results))

        return 0

    def acquire(self):
        """
        Get a number (1k for current oracle rest) of documents and bind them to this aso
        NEW -> ACQUIRED (asoworker NULL -> config.asoworker)
        :return:
        """
        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquireTransfers'

        self.logger.debug("Retrieving transfers from oracleDB")

        result = dict()
        try:
            result = self.oracleDB.post(self.config.oracleFileTrans,
                                        data=encodeRequest(fileDoc))
        except Exception as ex:
            self.logger.error("Failed to acquire transfers \
                              from oracleDB: %s" % ex)

        return str(result)

    def getAcquired(self):
        """
        Get a number of documents to be submitted (in ACQUIRED status) and return results of the query for logs
        :return:
        """
        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquiredTransfers'
        fileDoc['grouping'] = 0

        self.logger.debug("Retrieving users from oracleDB")

        documents = list()
        try:
            results = self.oracleDB.get(self.config.oracleFileTrans,
                             data=encodeRequest(fileDoc))
            documents = oracleOutputMapping(results)
        except Exception as ex:
            self.logger.error("Failed to get acquired transfers \
                              from oracleDB: %s" % ex)
            pass

        return documents

    def submitted(self, files):
        """
        Mark the list of files as submitted once the FTS submission succeeded
        ACQUIRED -> SUBMITTED
        Return the lfns updated successfully and report data for dashboard
        :param files: tuple (source_lfn, dest_lfn)
        :return:
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
        #            docbyId = self.oracleDB.get(
        #                self.config.oracleFileTrans.replace('filetransfers', 'fileusertransfers'),
        #                data=encodeRequest({'subresource': 'getById', 'id': docId}))
        #            document = oracleOutputMapping(docbyId, None)[0]
                    id_list.append(docId)
                    lfn_in_transfer.append(lfn)
                except Exception as ex:
                    self.logger.error("Error getting id: %s" % ex)
                    raise

            lfn_in_transfer.append(lfn)
            # TODO: add dashboard stuff
            # dash_rep = (document['jobid'], document['job_retry_count'], document['taskname'])
        try:
            fileDoc = dict()
            fileDoc['asoworker'] = self.config.asoworker
            fileDoc['subresource'] = 'updateTransfers'
            fileDoc['list_of_ids'] = id_list
            fileDoc['list_of_transfer_state'] = ["SUBMITTED" for x in id_list]

            self.oracleDB.post(self.config.oracleFileTrans,
                               data=encodeRequest(fileDoc))
            self.logger.debug("Marked acquired %s" % (id_list))
        except Exception as ex:
            self.logger.error("Error during status update: %s" % ex)
        return lfn_in_transfer, dash_rep

    def transferred(self, files):
        """
        Mark the list of files as tranferred
        """
        good_ids = list()
        updated_lfn = list()
        try:
            for lfn in files:
                lfn = lfn[0]
                if lfn.find('temp') == 7:
                    docId = getHashLfn(lfn)
                    good_ids.append(docId)
                    updated_lfn.append(lfn)
                    self.logger.debug("Marking done %s" % lfn)
                    self.logger.debug("Marking done %s" % docId)

            data = dict()
            data['asoworker'] = self.config.asoworker
            data['subresource'] = 'updateTransfers'
            data['list_of_ids'] = good_ids
            data['list_of_transfer_state'] = ["DONE" for x in good_ids]
            self.oracleDB.post(self.config.oracleFileTrans,
                               data=encodeRequest(data))
            self.logger.debug("Marked good %s" % good_ids)
        except Exception:
            self.logger.exception("Error updating documents")
            return 1
        return 0

    def failed(self, files, failures_reasons=[], max_retry=3, force_fail=False, submission_error=False):
        """

        :param files: tuple (source_lfn, dest_lfn)
        :param failures_reasons: list(str) with reasons of failure
        :param max_retry: number of retry before giving up
        :param force_fail: flag for triggering failure without retry
        :param submission_error: error during fts submission
        :return:
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
                return 1

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
                self.oracleDB.post(self.config.oracleFileTrans,
                                   data=encodeRequest(fileDoc))
            except Exception:
                self.logger.exception('ERROR updating failed documents')
                return 1
        self.logger.debug("failed file updated")
        return 0

    def acquirePub(self):
        """

        :return:
        """
        fileDoc = dict()
        fileDoc['asoworker'] = self.config.asoworker
        fileDoc['subresource'] = 'acquirePublication'

        self.logger.debug("Retrieving publications from oracleDB")

        try:
            self.oracleDB.post(self.config.oracleFileTrans,
                    data=encodeRequest(fileDoc))
        except Exception as ex:
            self.logger.error("Failed to acquire publications \
                              from oracleDB: %s" % ex)

    def getPub(self):
        """

        :return:
        """
        to_pub_docs = list()
        filedoc = dict()
        filedoc['asoworker'] = self.config.asoworker
        filedoc['subresource'] = 'acquiredPublication'
        filedoc['grouping'] = 0

        try:
            results = self.oracleDB.get(self.config.oracleFileTrans,
                                        data=encodeRequest(filedoc))
            to_pub_docs = oracleOutputMapping(results)
        except Exception as ex:
            self.logger.error("Failed to get acquired publications \
                              from oracleDB: %s" % ex)
            return to_pub_docs

        return to_pub_docs

    def pubDone(self, files, workflow):
        """

        :param files:
        :param workflow:
        :return:
        """
        wfnamemsg = "%s: " % workflow
        data = dict()
        id_list = list()
        for lfn in files:
            source_lfn = lfn
            docId = getHashLfn(source_lfn)
            id_list.append(docId)
            msg = "Marking file %s as published." % lfn
            msg += " Document id: %s (source LFN: %s)." % (docId, source_lfn)
            self.logger.info(wfnamemsg + msg)
        data['asoworker'] = self.config.asoworker
        data['subresource'] = 'updatePublication'
        data['list_of_ids'] = id_list
        data['list_of_publication_state'] = ['DONE' for x in id_list]
        try:
            self.oracleDB.post(self.config.oracleFileTrans,
                               data=encodeRequest(data))
            self.logger.debug("updated done: %s " % id_list)
        except Exception as ex:
            self.logger.error("Error during status update for published docs: %s" % ex)

    def pubFailed(self, files, failure_reasons=list(), force_failure=False):
        """

        :param files:
        :param failure_reasons:
        :return:
        """
        id_list = list()
        for Lfn in files:
            source_lfn = Lfn[0]
            docId = getHashLfn(source_lfn)
            id_list.append(docId)
            self.logger.debug("Marking failed %s" % docId)

        fileDoc = dict()
        fileDoc['asoworker'] = 'asodciangot1'
        fileDoc['subresource'] = 'updatePublication'
        fileDoc['list_of_ids'] = id_list
        fileDoc['list_of_publication_state'] = ['FAILED' for x in id_list]

        #if force_failure or document['publish_retry_count'] > self.max_retry:
        #else:
        #    fileDoc['list_of_publication_state'] = 'RETRY'
        # TODO: implement retry, publish_retry_count missing from input?

        fileDoc['list_of_retry_value'] = [1 for x in id_list]
        fileDoc['list_of_failure_reason'] = failure_reasons

        try:
            self.oracleDB.post(self.config.oracleFileTrans,
                                data=encodeRequest(fileDoc))
            self.logger.debug("updated failed: %s " % id_list)
        except Exception:
            msg = "Error updating failed documents"
            self.logger.exception(msg)
