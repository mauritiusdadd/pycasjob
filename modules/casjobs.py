#
# Copyright (C) 2016-2017  Maurizio D'Addona <mauritiusdadd@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.



import os
import sys
import urllib.request
import urllib.parse
import urllib.error


def _str2bool(val):
    if val.lower() == 'false':
        return False
    else:
        return True


class CJJob(object):

    def __init__(self):
        self.JobID = 0
        self.Rows = 0
        self.WebServiceID = 0
        self.TimeSubmit = ""
        self.TimeStart = ""
        self.TimeEnd = ""
        self.Status = -1
        self.Queue = -1
        self.TaskName = ""
        self.OutputLoc = ""
        self.Error = ""
        self.Query = ""
        self.Context = ""
        self.Type = ""


class CjQueue(object):

    def __init__(self):
        self.Context = ""
        self.Timeout = ""


class CasJobsInterface(object):

    DEFAULT_SERVICE_ENDPOINT = "http://skyserver.sdss3.org/casjobs/services/jobs.asmx"

    CONFIG_FILE = "CasJobs.config"

    DATA_TARGETS = [
        'DR9', 'DR10', 'DR11', 'DR12', 'DR13'
    ]

    JOB_STATUS = {
        0 : 'READY',
        1 : 'STARTED',
        2 : 'CANCELING',
        3 : 'CANCELLED',
        4 : 'FAILED',
        5 : 'FINISHED'
    }

    FMT_TYPES = [
        'CSV', 'FITS',
        'DATASET', 'VOTABLE'
    ]    

    STATE_RUNNING = 'RUNNING'
    STATE_SUSPENDING = 'SUSPENDING'
    STATE_SUSPENDED = 'SUSPENDED'
    STATE_WAKING = 'WAKING'

    def __init__(self):
        self.wsid = None
        self.pswd = None
        self.queue = 1
        self.days = 1
        self.target = self.DATA_TARGETS[-1]
        self.verbose = False
        self.debug = False
        self.endpoint = self.DEFAULT_SERVICE_ENDPOINT

    def setlogin(self, wsid, pw):
        self.wsid = int(wsid)
        self.pswd = str(pswd)

    def loadconfig(self, filename):
        with open(filename, 'r') as cfgfile:
            for line in cfgfile.readlines():
                key, val = line[:-1].split('=')
                if key == 'wsid':
                    self.wsid = int(val)
                elif key == 'password':
                    self.pswd = val
                elif key == 'dafault_target':
                    self.settarget(val)
                elif key == 'default_queue':
                    self.queue=int(val)
                elif key == 'default_days':
                    self.days=int(val)
                elif key == 'verbose':
                   self.verbose = _str2bool(val)
                elif key == 'debug':
                    self.debug = _str2bool(val)

    def log(self, msg, lvl=0):
        # TODO: use logging
        if self.verbose:
            print(msg)

    def _httppostcall(self, func, **args):
        post_fmt_str = "{0:s}/{1:s}?{2:s}"

        post_args = urllib.parse.urlencode(args)
        post_cmd = post_fmt_str.format(self.endpoint, func, post_args)

        with urllib.request.urlopen(post_cmd) as resp:
            return resp.read().decode('UTF-8')

    def canceljob(self, jid):
        result = self._httppostcall("CangelJob",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    jobId=jid)

    def quickrun(self, query, ctx="", name="", system=False):
        result = self._httppostcall("ExecuteQuickJob",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    qry=query,
                                    context=ctx,
                                    taskname=name,
                                    isSystem=system)
        return result

    def quickrunds(self, query, ctx="", name="", system=False):
        result = self._httppostcall("ExecuteQuickJobDS",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    qry=query,
                                    context=ctx,
                                    taskname=name,
                                    isSystem=system)
        return result

    def quickrunoid(self, query, ctx="", name="", system=False):
        """
        No description available... wtf -_-
        """
        result = self._httppostcall("ExecuteQuickJobOID",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    qry=query,
                                    context=ctx,
                                    taskname=name,
                                    isSystem=system)
        return result


    def getjobstatus(self, jid):
        result = self._httppostcall("GetJobStatus",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    jobId=int(jid))
        return int(result)

    def getjobtypes(self):
        result = self._httppostcall("GetJobTypes",
                                    wsid=self.wsid,
                                    pw=self.pswd)
        types = ()  # TODO
        return types


    def getjobs(self, condstr, system=False):
        result = self._httppostcall("GetJobs",
                                    owner_wsid=self.wsid,
                                    owner_pw=self.pswd,
                                    conditions=condstr,
                                    includeSystem=system)
        cjjobs = []  # TODO
        return result

    def getjobsjob(self, condstr, system=False):
        result = self._httppostcall("GetJobsJob",
                                    owner_wsid=self.wsid,
                                    owner_pw=self.pswd,
                                    conditions=condstr,
                                    includeSystem=system)
        jobs = []  # TODO
        return jobs

    def getqueues(self):
        result = self._httppostcall("GetQueues",
                                    wsid=self.wsid,
                                    pw=self.pswd)
        cjqueues = []  # TODO
        return cjqueues

    def getservicestate(self, service_name):
        result = self._httppostcall('GetServiceState',
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    serviceName=service_name)
        return result

    def setservicestate(self, service_name, new_state):
        if state not in (STATE_WAKING, STATE_SUSPENDING):
            raise ValueError("Job state can be only set to WAKING or SUSPENDING")

        result = self._httppostcall("SetServiceState",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    serviceName=service_name,
                                    state=new_tate)

    def submitextractjob(self, table_name, output_type):
        result = self._httppostcall("SubmitExtractJob",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    tableName=table_name,
                                    type=output_type)

    def submitjob(self, query, ctx=None, task_name=None, estimated_time=None):
        result = self._httppostcall("SubmitJob",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    qry=query,
                                    taskname=task_name,
                                    estimate=estimated_time)
        return int(result)

    def submitjobinto(self, query, table_name, ctx=None, task_name=None, estimated_time=None):
        result = self._httppostcall("SubmitJobInto",
                                    wsid=self.wsid,
                                    pw=self.pswd,
                                    qry=query,
                                    table=table_name,
                                    taskname=task_name,
                                    estimate=estimated_time)
        return int(result)

    def uploaddata(self, table_name, table_data):
        result = self._httppostcall("UploadData",
                                    wsid=self.wsid,
                                    pw=self.pw,
                                    tableName=table_name,
                                    data=table_data)

