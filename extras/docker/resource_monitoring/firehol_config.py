import argparse
import requests
import re

INTERVAL=25 # range between valid values
SLEEP=60 #monitor information each sleep seconds

LAST_USAGE=0
LAST_SYSTEM_USAGE=0


def get_node_id(ip_addr, port = 5001):
    data = requests.get('http://' + ip_addr + ':' + str(port) + '/id')
    if not data.ok:
        print "Error getting node id for %s" % ip_addr
        return
    return data.json()['id']

def get_peer_node_ip(ip_addr, rt, port = 5001):
    data = requests.get('http://' + ip_addr + ':' + str(port) + '/node/' + rt)
    if not data.ok:
        print "Error getting IP address for runtime %s" % rt
        return

    ips = []
    for uri in data.json()['uris']:
        res = re.search( r'([0-9]+(?:\.[0-9]+){3}):([0-9]+)', uri)
        ips.append((res.group(1), res.group(2)))
    return ips

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generates a config for FireQOS based on runtimes.')
    parser.add_argument('-a', '--addr', type=str, help='Calvin address', required=True)
    parser.add_argument('-p', '--port', type=int, help='Calvin port', required=False, default=5001)
    parser.add_argument('-i', '--intf', type=str, help='Interface name', required=True)
    parser.add_argument('-r', '--runtimes', type=str, nargs='+', help='List of peers.', required=True)
    args = parser.parse_args()

    ip_addr = args.addr

    print 'server_netdata_ports="tcp/19999"'
    print 'interface ' + args.intf + ' world bidirectional ethernet balanced rate 10000Mbit'
    node_id = get_node_id(ip_addr, args.port)
    for rt in args.runtimes:
        remote_ips = get_peer_node_ip(ip_addr = ip_addr, rt = rt, port = args.port)
        print '\t class calvin' + node_id + '_' + rt
        for remote_ip in remote_ips:
#            print '\t\tmatch host ' + remote_ip[0] + ' port ' + remote_ip[1]
            print '\t\tmatch host ' + remote_ip[0]
