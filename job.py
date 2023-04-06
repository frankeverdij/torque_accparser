from collections import Counter
from sys import argv,exit
import csv
import os
import time
import calendar
import re
import glob
import re
import argparse

class Job():
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
        # If the job has exited, don't change anything.
        if (self.status == 'E'):
            return
        # If the job has started, the job can only be deleted or exited.
        if (self.status == 'S'):
            if (entry[1] == 'Q'):
                return
        # All other statuses need to be processed.
        self.parse(entry)
        return

    def parse(self, entry):
        try:
            timestamp = time.strptime(entry[0], "%m/%d/%Y %H:%M:%S") #localtime
            self.timestamp = calendar.timegm(timestamp)

            self.status = entry[1]
            self.jobid = entry[2]

            message=''.join(entry[3:])
        except:
            raise("Too few entries in job line!")

        # regexp matches all nonwhitespace characters before an equal sign
        # these are the properties of each job entry
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
        if (self.status == 'D'):
            self.owner = jobdict.get('requestor', '')
        else:
            self.owner = jobdict.get('owner', '')
        self.queue = jobdict.get('queue', '')
        self.ctime = jobdict.get('ctime', 0)
        self.qtime = jobdict.get('qtime', 0)
        self.etime = jobdict.get('etime', 0)
        self.start = jobdict.get('start', 0)
        self.end = jobdict.get('end', 0)
        
        nodes = jobdict.get('exec_host', '')
        nodes = re.split('\/\d*|\+', nodes)
        
        nodes = list(filter(None, nodes))
        self.nodes = Counter(nodes)

        #self.reqcpus = jobdict.get('total_execution_slots', 0)
        self.reqcpus = len(nodes) 
        #self.reqnodes = jobdict.get('unique_node_count', 0)
        self.reqnodes = len(self.nodes)
        
        self.rucputime = hms2sec(jobdict.get('resources_used.cput', '0'))
        self.rumemory = jobdict.get('resources_used.mem', '0kb')
        self.ruwalltime = hms2sec(jobdict.get('resources_used.walltime', '0'))
        
        if (self.status == 'E'):
            self.usage = {}
            for k in self.nodes:
                self.usage[k] = self.nodes[k] * self.ruwalltime

    def prepare_csv(self):
        return [self.timestamp, self.jobid,
                self.owner if self.status == 'D' else self.user, self.status,
                self.exitcode, self.queue, self.ctime, self.start,
                self.end, self.reqnodes, self.reqcpus, self.rucputime,
                self.rumemory, self.ruwalltime]
        
def hms2sec(hms):
    return sum(int(x) * 60 ** i for i, x in enumerate(reversed(hms.split(':'))))

def output(joblist, csv_file, csv_usage_file):
    #sort the joblist
    joblist.sort(key=lambda job: job.timestamp)
    for i in joblist:
        csv_file.writerow(i.prepare_csv())

    nodeusage = Counter(joblist[0].usage)
           
    for i in joblist[1:]:
        nodeusage.update(Counter(i.usage))

    #sortednodeusage = dict(sorted(nodeusage.items(), key=lambda item: item[1], reverse=True))
    sortednodeusage = dict(sorted(nodeusage.items(), key=lambda item: item[0]))
    for i in sortednodeusage:
        csv_usage_file.writerow([i, sortednodeusage[i]])

def main():
    parser = argparse.ArgumentParser(
        description = 'Converts Torque accounting file(s) into CSV ',
        formatter_class = argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', '--pattern', action = 'store_true',
        help = 'treat file argument as a pattern')
    parser.add_argument('-c', '--combine', action = 'store_true',
        help = 'combine accounting from pattern match into one CSV')
    parser.add_argument('file', type = str,
        help = 'file or pattern containing Torque accounting')
    args = parser.parse_args(argv[1:])

    if args.combine and not args.pattern:
        parser.error("--combine requires --pattern option")
        exit(-1)

    if args.combine:
        torquejobs = {}
        joblist = []
        timestamp = []
        njobs = -1
        csv_file_fd = open(os.path.basename(args.file) + '.csv', 'w')
        csv_file = csv.writer(csv_file_fd)
        csv_usage_fd = open(os.path.basename(args.file) + '.usage.csv', 'w')
        csv_usage_file = csv.writer(csv_usage_fd)

    for f in glob.glob(args.file+'*' if args.pattern else args.file):
        accounting_file = open(f, 'r')
        accounting_file_name = os.path.basename(f)

        if not args.combine:
            torquejobs = {}
            joblist = []
            njobs = -1
            csv_file_fd = open(accounting_file_name + '.csv', 'w')
            csv_file = csv.writer(csv_file_fd)
            csv_usage_fd = open(accounting_file_name + '.usage.csv', 'w')
            csv_usage_file = csv.writer(csv_usage_fd)

        for line in accounting_file:
            entry = line.split(';')
            jobid = entry[2] #"license" or job id
            if entry[1] == 'L': # one of [LQSED]
                continue #PBS license stats? not associated with a job, skip
            if jobid not in torquejobs:
                job = Job()
                njobs += 1
                joblist.append(job)
                torquejobs[jobid] = njobs
            joblist[torquejobs[jobid]].update(entry)

        accounting_file.close()
       
        if not args.combine:
            output(joblist, csv_file, csv_usage_file)
            csv_file_fd.close()
            csv_usage_fd.close()
                
    if args.combine:
        output(joblist, csv_file, csv_usage_file)       
        csv_file_fd.close()
        csv_usage_fd.close()

if __name__ == "__main__":
    main()
