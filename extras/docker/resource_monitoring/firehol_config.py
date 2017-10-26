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
    uri = data.json()['uris'][0]
    ip = re.search( r'[0-9]+(?:\.[0-9]+){3}', uri).group()
    return ip
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generates a config for FireQOS based on runtimes.')
    parser.add_argument('-a', '--addr', type=str, help='Calvin address', required=True)
    parser.add_argument('-i', '--intf', type=str, help='Interface name', required=True)
    parser.add_argument('-r', '--runtimes', type=str, nargs='+', help='List of peers.', required=True)
    args = parser.parse_args()

    ip_addr = args.addr

    print 'server_netdata_ports="tcp/19999"'
    print 'interface ' + args.intf + ' world bidirectional ethernet balanced rate 10000Mbit'
    node_id = get_node_id(ip_addr)
    for rt in args.runtimes:
        print '\t class calvin' + node_id + '_' + rt
        print '\t\tmatch tcp sports 5000'
        remote_ip = get_peer_node_ip(ip_addr = ip_addr, rt = rt)
        print '\t\tmatch input src ' + remote_ip
