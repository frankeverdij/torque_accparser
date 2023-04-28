from collections import Counter, defaultdict
from sys import argv
import csv
import os
import time
import calendar
import glob
import re
import argparse


class Users:
    """This class contains user information for billing purposes
    (cpu's x hours) and degree of parallelism (% used / requested)
    """
    def __init__(self, user, usedcpuseconds, reqcpuseconds):
        self.user = user
        self.usedcpuseconds = usedcpuseconds
        self.reqcpuseconds = reqcpuseconds

    def update(self, usedcpuseconds, reqcpuseconds):
        self.usedcpuseconds += usedcpuseconds
        self.reqcpuseconds += reqcpuseconds
    

class Job:
    """This class contains all relevant information of a job processed
    by the Torque batch system, parsed from accounting information in
    $PBS_SPOOL/server_priv/accounting
    """
    def __init__(self):
        self.timestamp = 0
        self.jobid = ''
        self.user = ''
        self.group = ''
        self.status = ''
        self.statuslog = {}
        self.statusstring = ''
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
        self.rucputime = '00:00:00'
        self.rumemory = '0kb'
        self.ruwalltime = '00:00:00'
        self.usage = {}
        self.maxslot = {}

    def update(self, entry):
        """decides whether the job needs to be updated with the information
        from the (split) accounting line entry. If yes, then the line will
        be parsed.
        """
        # Register status
        timestamp = entry[0]
        self.statuslog[timestamp] = entry[1]
        self.statusstring += entry[1]
        # If the job already has exited, don't change anything.
        if self.status == 'E':
            return
        # If the job has started, the job can only be rerun, deleted,
        # aborted or exited.
        if self.status == 'S':
            if entry[1] == 'Q':
                return
        # All other statuses need to be processed.
        self.parse(entry)
        return

    def parse(self, entry):
        """parses the accounting line entry for properties and sets member
        variables accordingly
        """
        message = ''
        try:
            self.timestamp = entry[0]
            self.status = entry[1]
            self.jobid = entry[2]
            message = ''.join(entry[3:])
        except IndexError:
            print("Too few entries in job line!")

        # these are the properties of each job entry
        # the regexp matches all nonwhitespace characters before an equal sign
        props = re.findall(r"\S*=", message)
        vals = re.split(r"\S*=", message)

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
        # the regexp splits on any '/' or any '+' character
        nodes = re.split('[/+]', nodes)
        # make sure the list does not contain a ''
        nodes = list(filter(None, nodes))
        # count the dictionary occurences for each individual node,
        # leaving you with a new dict with entries: 'nodename' = #ofcores
        # note: the even indices have the nodenames, the odd have the coreslots
        self.nodes = Counter(nodes[::2])

        # determine the max coreslot for each node allocated to this job
        # this is used when there is no 'nodes' list available
        for i, n in enumerate(nodes[::2]):
            if n in self.maxslot:
                self.maxslot[n] = max(self.maxslot[n], int(nodes[2*i+1]))
            else:
                self.maxslot[n] = int(nodes[2*i+1])

        self.reqcpus = int(jobdict.get('total_execution_slots', 0))
        # self.reqnodes = jobdict.get('unique_node_count', 0)
        self.reqnodes = len(self.nodes)
        
        self.rucputime = hms2sec(jobdict.get('resources_used.cput', '00:00:00'))
        self.rumemory = int(jobdict.get('resources_used.mem', '0kb').rstrip('kb'))
        self.ruwalltime = hms2sec(jobdict.get('resources_used.walltime', '00:00:00'))
        
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
                self.exitcode, self.queue, self.qtime, self.start,
                self.end, self.reqnodes, self.reqcpus, self.rucputime,
                self.rumemory, self.ruwalltime]


def header_csv():
    """lists the header string for the joblist csv file
    """
    return ['timestamp', 'jobid', 'owner', 'status', 'exitcode', 'queue',
            't_queue', 't_start', 't_end', '#nodes', '#cores',
            'used_cputime', 'used_memory(kb)', 'used_walltime']


def header_users_csv():
    """lists the header string for users csv file
    """
    return ['user', 'used_cpuhours', 'req_cpus*walltime', 'pct_parallel']


def hms2sec(hms):
    """quick oneliner for converting HH:MM:SS into seconds
    """
    return sum(int(x) * 60 ** i for i, x in enumerate(reversed(hms.split(':'))))


def main():
    parser = argparse.ArgumentParser(
        description='Converts Torque accounting file(s) into CSV ',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-p', '--pattern', action='store_true',
                        help='treat file argument as a pattern')
    parser.add_argument('-d', '--directory', type=str,
                        help='location of torque directory')
    parser.add_argument('-f', '--full', action='store_true',
                        help='output every job status line, not just jobs with status "E"nded ')
    parser.add_argument('file', type=str, nargs='*',
                        help='file(s) or pattern(s) containing Torque accounting')
    args = parser.parse_args(argv[1:])

    entries = []
    torquejobs = {}
    joblist = []
    njobs = -1

    accountingdir = args.directory+'/server_priv/accounting/' if args.directory else ''

    # loop over accounting files (or patterns) and build the entries list
    for accfp in args.file:
        for f in glob.glob(accountingdir + accfp + '*' if args.pattern else accountingdir + accfp):
            with open(f, 'r') as accounting_file:
                for line in accounting_file:
                    entry = line.split(';')
                    timestamp = calendar.timegm(time.strptime(entry[0], "%m/%d/%Y %H:%M:%S"))
                    # when we don't want all status entries, record only 'E'nded jobs
                    if not args.full:
                        if entry[1] != 'E':
                            continue
                    # *entry[] unlists a list, otherwise you will create nested lists
                    entries.append([timestamp, *entry[1:]])

    # sort the entries on timestamp in the accounting file(s). (=first element of the entry sublist)
    entries.sort()

    # now that we have sorted all the entries, go through it and build the joblist
    for entry in entries:
        try:
            # this is the jobid, but some PBS implementations use this field
            # as license information
            jobid = entry[2]
            # the status entry can be one of [LQSEDRACT]:
            # Licensed, Queued, Started, Ended, Deleted, Rerun, Aborted,
            # Checkpointed, conTinued
            if entry[1] == 'L' or entry[1] == 'C' or entry[1] == 'T':
                continue  # skip licensing, checkpoint and continue entries
            # check if the jobid has been seen already
            if jobid not in torquejobs:
                # No? Then create a new job instance and add it to the joblist
                job = Job()
                njobs += 1
                joblist.append(job)
                # Create a reference index for each jobid, so we can retrieve
                # each job from the joblist quickly
                torquejobs[jobid] = njobs
                # call job.update() to process the entry
            joblist[torquejobs[jobid]].update(entry)
        except IndexError:
            print("no useful accounting line(s).")
            continue

    # get the name of the masternode
    if args.directory:
        with open(args.directory+'/server_name', 'r') as master_fd:
            masternode = master_fd.readline().rstrip('\n')
    else:
        masternode = next(iter(torquejobs)).split('.')[1]

    # sort the joblist on timestamp in the accounting file(s)
    joblist.sort(key=lambda j: j.timestamp)

    # make a concise filename for the csv output files
    combinedname = os.path.basename(args.file[0]) + '-' + os.path.basename(args.file[-1])\
        if len(args.file) > 1 else os.path.basename(args.file[0])

    # write all job entries to a csv file
    with open(masternode + '.' + combinedname + '.csv', 'w') as csv_fd:
        csv_file = csv.writer(csv_fd)
        csv_file.writerow(header_csv())
        for i in joblist:
            csv_file.writerow(i.prepare_csv())

    # Nodes:
    # obtain number of cores for each node
    if args.directory:
        with open(args.directory + '/server_priv/nodes', 'r') as node_fd:
            nodecpus = {}
            for line in node_fd:
                node = re.split(r"\snp=|\s", line)
                node = list(filter(None, node))[:2]
                nodecpus[node[0]] = node[1]
    else:
        # if the nodes files is missing, do a best guess
        # by determining the maximum core # per node
        nodecpus = {}
        for i in joblist:
            for k, v in i.maxslot.items():
                if k in nodecpus:
                    nodecpus[k] = max(nodecpus[k], v + 1)
                else:
                    nodecpus[k] = v + 1

    # the next block computes node usage as a total of
    # requested cores x walltime in seconds
    nodeusage = Counter(joblist[0].usage)
    for i in joblist[1:]:
        nodeusage.update(Counter(i.usage))

    sortednodeusage = dict(sorted(nodeusage.items(), key=lambda item: item[0]))

    # output node information to a csv
    with open(masternode + '.' + combinedname + '.nodes.csv', 'w') as csv_fd:
        csv_file = csv.writer(csv_fd)
        for i in sortednodeusage:
            csv_file.writerow([i, sortednodeusage[i]])

    # Users:
    # make a dict with usernames, actual cpuseconds used, and requested cpu times the walltime used
    # this allows for computing the degree of parallelisation per user
    userdict = defaultdict(Users)
    for i in joblist:
        if i.user not in userdict:
            userdict[i.user] = Users(i.user, i.rucputime, i.reqcpus * i.ruwalltime)
        else:
            userdict[i.user].update(i.rucputime, i.reqcpus * i.ruwalltime)

    sorteduser = dict(sorted(userdict.items(), key=lambda item: item[1].usedcpuseconds, reverse=True))

    # output users information to a csv
    # convert cpuseconds to cpuhours and calculate percentage of parallelization
    with open(masternode + '.' + combinedname + '.users.csv', 'w') as csv_fd:
        csv_file = csv.writer(csv_fd)
        csv_file.writerow(header_users_csv())
        for k in sorteduser:
            assert(k == sorteduser[k].user)
            u = sorteduser[k].usedcpuseconds
            r = sorteduser[k].reqcpuseconds
            billing = [k, u/3600, r/3600, 100 * u/r if r > 0 else 0]
            billing = [x if type(x) is str else format(x, '.2f') for x in billing]
            csv_file.writerow(billing)


if __name__ == "__main__":
    main()
