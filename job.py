from collections import Counter
from sys import argv
import csv
import os
import time
import calendar
import glob
import re
import argparse


class Job:
    """This class contains all relevant information of a job processed
    by the Torque batch system, parsed from accounting information in
    $PBS_SPOOL/server_priv/accounting
    """
    def __init__(self):
        self.timestamp = ''
        self.jobid = ''
        self.user = ''
        self.group = ''
        self.status = ''
        self.exitcode = 0
        self.owner = ''
        self.queue = ''
        self.ctime = 0
        self.qtime = 0
        self.etime = 0
        self.start = 0
        self.end = 0
        self.nodes = {}
        self.reqcpus = 0
        self.reqnodes = 0
        self.rucputime = 0
        self.rumemory = ''
        self.ruwalltime = 0
        self.usage = {}

    def update(self, entry):
        """decides whether the job needs to be updated with the information
        from the (split) accounting line entry. If yes, then the line will
        be parsed.
        """
        # If the job already has exited, don't change anything.
        if self.status == 'E':
            return
        # If the job has started, the job can only be deleted or exited.
        if self.status == 'S':
            if entry[1] == 'Q':
                return
        # All other statuses need to be processed.
        self.parse(entry)
        return

    def parse(self, entry):
        """parses the accounting line entry for properties and sets member
        vartiables accordingly
        """
        message = ''
        try:
            timestamp = time.strptime(entry[0], "%m/%d/%Y %H:%M:%S")  # localtime
            self.timestamp = calendar.timegm(timestamp)

            self.status = entry[1]
            self.jobid = entry[2]

            message = ''.join(entry[3:])
        except IndexError:
            print("Too few entries in job line!")

        # these are the properties of each job entry
        # the regexp matches all nonwhitespace characters before an equal sign
        props = re.findall(r'\S*=', message)
        vals = re.split(r'\S*=', message)

        # make sure the lists do not contain a ''
        props = list(filter(None, props))
        vals = list(filter(None, vals))
        
        # remove equal signs
        prop = list(i[:-1] for i in props)
        # clear trailing spaces
        val = list(i.rstrip() for i in vals)
        # make a dictionary
        jobdict = dict(zip(prop, val))

        self.user = jobdict.get('user', '')
        self.group = jobdict.get('group', '')
        self.exitcode = jobdict.get('Exit_status', 0)
        if self.status == 'D':
            # deleted jobs don't have an owner but a requestor
            self.owner = jobdict.get('requestor', '')
        else:
            self.owner = jobdict.get('owner', '')
        self.queue = jobdict.get('queue', '')
        self.ctime = jobdict.get('ctime', 0)
        self.qtime = jobdict.get('qtime', 0)
        self.etime = jobdict.get('etime', 0)
        self.start = jobdict.get('start', 0)
        self.end = jobdict.get('end', 0)
        
        # This parses the nodestring for allocated core-slots on nodes
        nodes = jobdict.get('exec_host', '')
        # the regexp matches all digits after a '/' or any '+' character
        nodes = re.split('/\d*|\+', nodes)
        # make sure the lists do not contain a ''
        nodes = list(filter(None, nodes))
        # count the dictionary occurences for each individual node,
        # leaving you with a new dict with entries: 'nodename' = #ofcores
        self.nodes = Counter(nodes)

        # self.reqcpus = jobdict.get('total_execution_slots', 0)
        self.reqcpus = len(nodes) 
        # self.reqnodes = jobdict.get('unique_node_count', 0)
        self.reqnodes = len(self.nodes)
        
        self.rucputime = hms2sec(jobdict.get('resources_used.cput', '0'))
        self.rumemory = jobdict.get('resources_used.mem', '0kb')
        self.ruwalltime = hms2sec(jobdict.get('resources_used.walltime', '0'))
        
        if self.status == 'E':
            # upon exit, when used resources are known, compute node usage by
            # calculating walltime x #ofcores since that is what the system has
            # reserved
            self.usage = {}
            for k in self.nodes:
                self.usage[k] = self.nodes[k] * self.ruwalltime

    def prepare_csv(self):
        """creates a string of member variables to be written out as csv
        """
        return [self.timestamp, self.jobid,
                self.owner if self.status == 'D' else self.user, self.status,
                self.exitcode, self.queue, self.ctime, self.start,
                self.end, self.reqnodes, self.reqcpus, self.rucputime,
                self.rumemory, self.ruwalltime]


def hms2sec(hms):
    """quick oneliner for converting HH:MM:SS into seconds
    """
    return sum(int(x) * 60 ** i for i, x in enumerate(reversed(hms.split(':'))))


def output(joblist, csv_file, csv_usage_file):
    # sort the joblist
    joblist.sort(key=lambda job: job.timestamp)
    for i in joblist:
        csv_file.writerow(i.prepare_csv())

    nodeusage = Counter(joblist[0].usage)
           
    for i in joblist[1:]:
        nodeusage.update(Counter(i.usage))

    # sortednodeusage = dict(sorted(nodeusage.items(), key=lambda item: item[1], reverse=True))
    sortednodeusage = dict(sorted(nodeusage.items(), key=lambda item: item[0]))
    for i in sortednodeusage:
        csv_usage_file.writerow([i, sortednodeusage[i]])


def main():
    parser = argparse.ArgumentParser(
        description='Converts Torque accounting file(s) into CSV ',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', '--pattern', action='store_true',
                        help='treat file argument as a pattern')
    parser.add_argument('-f', '--full', action='store_true',
                        help='output every job status line, not just jobs with status "E"nded ')
    parser.add_argument('file', type=str,
                        help='file or pattern containing Torque accounting')
    args = parser.parse_args(argv[1:])

    torquejobs = {}
    joblist = []
    njobs = -1
    csv_file_fd = open(os.path.basename(args.file) + '.csv', 'w')
    csv_file = csv.writer(csv_file_fd)
    csv_usage_fd = open(os.path.basename(args.file) + '.usage.csv', 'w')
    csv_usage_file = csv.writer(csv_usage_fd)

    for f in glob.glob(args.file+'*' if args.pattern else args.file):
        accounting_file = open(f, 'r')

        for line in accounting_file:
            entry = line.split(';')
            # this is the jobid, but some PBS implementations use this field
            # as license information
            jobid = entry[2]
            # the status entry can be one of [LQSED]:
            # Licensed, Queued, Started, Ended, Deleted
            if entry[1] == 'L':
                continue  # skip licensing information
            # when we don't want all status entries, skip all but "E"nded
            if not args.full:
                if not entry[1] == 'E':
                    continue
            # check if the jobid has been seen already
            if jobid not in torquejobs:
                # No? Then create a new job instance and add it to the joblist
                job = Job()
                njobs += 1
                joblist.append(job)
                # Create a reference index for each jobid, so we can retrieve
                # each job form the joblist quickly
                torquejobs[jobid] = njobs
            # call job.update() to process the entry
            joblist[torquejobs[jobid]].update(entry)

        accounting_file.close()
       
    output(joblist, csv_file, csv_usage_file)
    csv_file_fd.close()
    csv_usage_fd.close()


if __name__ == "__main__":
    main()
