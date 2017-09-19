from __future__ import division
import argparse
import docker
import time
import requests

INTERVAL=25 # range between valid values
SLEEP=60 #monitor information each sleep seconds

LAST_USAGE=0
LAST_SYSTEM_USAGE=0

def update_cpu(container, stats, ip_addr, port = 5001):
    global LAST_USAGE
    global LAST_SYSTEM_USAGE

    cpu_usage = stats['cpu_stats']['cpu_usage']['total_usage']
    system_usage = stats['cpu_stats']['system_cpu_usage']

    cores = len(stats['cpu_stats']['cpu_usage']['percpu_usage'])

    if LAST_SYSTEM_USAGE == 0 and LAST_USAGE == 0:
        LAST_USAGE = cpu_usage
        LAST_SYSTEM_USAGE = system_usage
        return

    diff_usage = cpu_usage - LAST_USAGE
    diff_system = system_usage - LAST_SYSTEM_USAGE

    if diff_system <= 0 or diff_usage < 0:
        print "Invalid delta, system: %d, usage: %d" % (diff_system, diff_usage)
        return

    percent = diff_usage/diff_system*cores*100
    addr = 'http://%s:%d/node/resource/cpuAvail' % (ip_addr, port)
    perc_rounded = INTERVAL * round((100 - percent)/INTERVAL)
    data = '{"value": %d}' % perc_rounded
    r = requests.post(addr, data)

    print "Container: %s. Cores: %d, Last Usage: %d, Last System: %d, CPU usage: %d, System: %d, Delta CPU: %d, Delta system: %d, Available CPU percentage %d" % (container, cores, LAST_USAGE, LAST_SYSTEM_USAGE, cpu_usage, system_usage, diff_usage, diff_system, perc_rounded)

    if not r.ok:
        print "Error updating available CPU in csruntime. Reason: " + r.reason
        print addr + " " + data

    LAST_USAGE = cpu_usage
    LAST_SYSTEM_USAGE = system_usage
    

def update_memory(container, stats, ip_addr, port = 5001):
    """
    Update memory available in runtime runnint at container
    container: name or ID of container
    stats: stats read from container
    ip_addr: IP address to access runtime's control API
    port: control port
    """
    usage = stats['memory_stats']['usage']
    limit = stats['memory_stats']['limit']
    percent = (usage/limit)*100

    print 'Container: %s, Memory: usage: %d, limit: %d, percentage usage: %f' % (container, usage, limit, percent)
    
    addr = 'http://%s:%d/node/resource/memAvail' % (ip_addr, port)
    perc_rounded = INTERVAL * round((100 - percent)/INTERVAL)
    data = '{"value": %d}' % perc_rounded
    r = requests.post(addr, data)

    if not r.ok:
        print "Error updating available memory in csruntime. Reason: " + r.reason
        print addr + " " + data

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Reads docker stats and updates CPU/RAM of Calvin runtimes. It is only valid for containers running a single csruntime and so will use only 1 core.')
    parser.add_argument('-c', '--container', type=str, help='Container ID to monitor')
    args = parser.parse_args()

    if not args.container:
        parser.print_usage()
        parser.exit()

    client = docker.APIClient(base_url='unix://var/run/docker.sock')
    ip_addr = client.inspect_container(args.container)['NetworkSettings']['IPAddress']

    data = client.stats(args.container, decode=True, stream=False)
    update_memory(args.container, data, ip_addr)
    update_cpu(args.container, data, ip_addr)
    while True:
        data = client.stats(args.container, decode=True, stream=False)
        update_memory(args.container, data, ip_addr)
        update_cpu(args.container, data, ip_addr)
        time.sleep(SLEEP)
