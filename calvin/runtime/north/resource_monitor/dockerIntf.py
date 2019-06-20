import docker
import subprocess
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

class DockerIntf():
    def __init__(self):
        cmd = "cat /proc/self/cgroup | grep 'docker' | sed 's/^.*\///' | tail -n1"
        self.container_id = output = subprocess.check_output(cmd, shell=True).strip('\n')
        if not self.container_id:
            _log.warning("DockerIntf impossible to get container ID")
        self.client = docker.APIClient(base_url='unix://var/run/docker.sock')

    def get_cpu_usage(self):
        try:
            stats = self.client.stats(container=self.container_id, stream=False)
        except:
            return -1

        pre_usage = float(stats['precpu_stats']['cpu_usage']['total_usage'])
        cpu_usage = float(stats['cpu_stats']['cpu_usage']['total_usage'])
        throttled =  float(stats['cpu_stats']['throttling_data']['throttled_time'])
        pre_thro =  float(stats['precpu_stats']['throttling_data']['throttled_time'])
        system_usage = float(stats['cpu_stats']['system_cpu_usage'])
        pre_system_usage = float(stats['precpu_stats']['system_cpu_usage'])

        cores = 0
        try:
            cores = stats['cpu_stats']['online_cpus']
        except:
            pass

        if cores == 0:
            cores = len(stats['cpu_stats']['cpu_usage']['percpu_usage'])

        diff_usage = cpu_usage - pre_usage
        diff_throttled = throttled - pre_thro
        diff_system = system_usage - pre_system_usage

        if diff_system <= 0 or diff_usage < 0:
            _log.warning("Invalid delta, system: %d, usage: %d" % (diff_system, diff_usage))
            return -1

        percent = ((diff_usage + diff_throttled)/diff_system)*cores*100

        _log.info("Container: %s. Cores: %d, Last Usage: %d, Last System: %d, CPU usage: %d, System: %d, Delta CPU: %d, Delta system: %d, CPU usage percentage %d" % (self.container_id, cores, pre_usage, pre_system_usage, cpu_usage, system_usage, diff_usage, diff_system, percent))

        return percent

    def get_ram_usage(self):
        try:
            stats = self.client.stats(container=self.container_id, stream=False)
        except:
            return -1

        usage = stats['memory_stats']['usage']
        limit = stats['memory_stats']['limit']
        percent = (usage/limit)*100

        _log.info('Container: %s, Memory: usage: %d, limit: %d, percentage usage: %f' % (self.container_id, usage, limit, percent))
        return percent

