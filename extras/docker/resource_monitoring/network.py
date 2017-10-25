import argparse
import requests
import docker
import json
from firehol_config import get_peer_node_ip
import re
import time

SLEEP=60 #monitor information each sleep seconds

def round_latency(lat):
    # valid values in microseconds
    values = [1, 100, 1000, 100000, 1000000]
    values_str = ['1us', '100us', '1ms', '100ms', '1s']

    minus = [abs(i - lat) for i in values]
    return values_str[minus.index(min(minus))]

def update_latency(ip_addr, id1, id2):
    print "Updating latency between nodes %s and %s" % (id1, id2)
    dst_ip = get_peer_node_ip(ip_addr, id2)
    url = 'http://' + ip_addr + ':9115/probe?target=' + dst_ip + '&module=icmp'
    print '- Url used: %s' % url
    data = requests.get(url)
    if not data.ok:
        print "- Error getting latency between %s and %s" % (id1, id2)
        return

    lat = re.search(r'probe_duration_seconds (\d*\.\d+|\d+)', data.text).group(1)
    lat_rounded = round_latency(float(lat)*1000000)
    print '- New value: %s, read %s seconds' % (lat_rounded, lat)

    data = '{"value": "%s"}' % lat_rounded
    r = requests.post('http://%s:%d/link/resource/latency/%s/%s' % (ip_addr, port, id1, id2), data)
    if not r.ok:
        print "- Error on post request"

def round_bandwidth(band):
    # valid values in kbits
    values = [1000, 100000, 1000000, 10000000, 100000000]
    values_str = ['1M', '100M', '1G', '10G', '100G']

    minus = [abs(i - band) for i in values]
    return values_str[minus.index(min(minus))]

def update_bandwidth(ip_addr, id1, id2, band_total):
    print "Updating bandwidth between nodes %s and %s" % (id1, id2)
    url_out = 'http://' + ip_addr + ':19999/api/v1/data?chart=tc.world_out&dimension=calvin' + id1 + id2 + '&points=1&after=-1'
    url_in = 'http://' + ip_addr + ':19999/api/v1/data?chart=tc.world_in&dimension=calvin' + id1 + id2 + '&points=1&after=-1'

    data_out = requests.get(url_out)
    data_in = requests.get(url_in)
    if not data_out.ok or not data_in.ok:
        print "- Error getting bandwidth usage between %s and %s" % (id1, id2)
        return
    usage_out = data_out.json()['data'][0][1]
    usage_in = data_in.json()['data'][0][1]
    usage = usage_out + usage_in
    band_rounded = round_bandwidth(band_total - usage)
    print "- New value: %s, read %f kbits" % (band_rounded, usage)
    data = '{"value": "%s"}' % band_rounded
    r = requests.post('http://%s:%d/link/resource/bandwidth/%s/%s' % (ip_addr, port, id1, id2), data)
    if not r.ok:
        print "- Error on post request"

def get_node_id(ip_addr, port = 5001):
    data = requests.get('http://' + ip_addr + ':' + str(port) + '/id')
    if not data.ok:
        print "Error getting node id for %s" % ip_addr
        return
    return data.json()['id']
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Reads externals tools and updates network bandwidth and latency parameters. It depends on netdata for providing the bandwidth information and on blackbox for latency.')
    parser.add_argument('-c', '--config', type=str, help='Configuration file', required=True)
    args = parser.parse_args()

    with open(args.config) as json_data_file:
        cfg = json.load(json_data_file)
    
    client = docker.APIClient(base_url='unix://var/run/docker.sock')
    ip_addr = client.inspect_container(cfg['container'])['NetworkSettings']['IPAddress']
    if not ip_addr:
        ip_addr = socket.gethostbyname(socket.gethostname())

    print "Container IP address: %s" % ip_addr
    
    port = 5001
    node_id = get_node_id(ip_addr, port)
    print "Node ID: %s" % node_id
    while True:
        for rt in cfg['runtimes']:
            try:
                update_bandwidth(ip_addr, node_id, rt['name'], rt['bandwidth'])
                update_latency(ip_addr, node_id, rt['name'])
            except:
                print "Error, trying again later"
                pass
        time.sleep(SLEEP)


