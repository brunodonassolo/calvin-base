import random
import math
import numpy
import sys
import time
import collections
from calvin.utilities import calvinconfig
from calvin.utilities.utils import enum
from calvin.utilities import calvinlogger
import calvin.requests.calvinresponse as response
from calvin.runtime.north.appdeployer import ReconfigAlgos

_conf = calvinconfig.get()
_log = calvinlogger.get_logger(__name__)


class UCB(object):
    """ Upper Confidence Bound """

    def __init__(self, app_id):
        self.K = _conf.get("learn", "K")
        self.n = {} # number of times selected
        self.u = {} # mean feedback
        self.k = [] # runtimes
        self.t = 1  # time step
        self.f_max = _conf.get("learn", "f_max")
        self.alpha = _conf.get("learn", "alpha")
        self.burn_id = None
        self.burn_runtime = None
        self.burn_mips = 0
        self.app_id = app_id
        self.algo = _conf.get("global", "reconfig_algorithm")
        self.reconfig = ReconfigAlgos()

    def __str__(self):
        return "n=%s u=%s k=%s burn_id=%s k_t=%s t=%d K=%d alpha=%f f_max=%f" % (str(self.n), str(self.u), str(self.k), self.burn_id, self.burn_runtime, self.t, self.K, self.alpha, self.f_max)


    def state(self):
        state = {}
        state['n'] = self.n
        state['u'] = self.u
        state['k'] = self.k
        state['t'] = self.t
        state['K'] = self.K
        state['algo'] = self.algo
        state['alpha'] = self.alpha
        state['f_max'] = self.f_max
        state['burn_id'] = self.burn_id
        state['burn_mips'] = self.burn_mips
        state['burn_runtime'] = self.burn_runtime
        state['app_id'] = self.app_id
        return state

    def set_state(self, state):
        self.n = state.get('n', {})
        self.u = state.get('u', {})
        self.k = state.get('k', [])
        self.K = state.get('K', _conf.get("learn", "K"))
        self.f_max = state.get('f_max', _conf.get("learn", "f_max"))
        self.algo = state.get('algo', _conf.get("global", "reconfig_algorithm"))
        self.alpha = state.get('alpha', _conf.get("learn", "alpha"))
        self.t = state.get('t', 0)
        self.burn_id = state.get('burn_id', None)
        self.burn_mips = state.get('burn_mips', 0)
        self.burn_runtime = state.get('burn_runtime', None)
        self.app_id = state.get('app_id', None)

    def set_burn(self, burn_id, burn_mips, possible_runtimes, runtime_cpu_total, dump_runtime):
        _log.info("UCB: app_id=%s burn_id=%s burn_mips=%f, possible runtimes init=%s " % (self.app_id, burn_id, burn_mips, str(possible_runtimes)))
        self.burn_id = burn_id
        self.burn_mips = burn_mips
        self.K = min(len(possible_runtimes), self.K)
        self.k = random.sample(possible_runtimes, k=self.K)

        self.n = { i : 0 for i in self.k }
        self.u = { i : 0 for i in self.k }

    def calculate_v(self, elapsed_time):
        f_max = self.f_max
        if elapsed_time > f_max:
            _log.warning("UCB: elapsed_time=%f greater than f_max=%f" % (elapsed_time, f_max))
            f_max = elapsed_time
        return (f_max - elapsed_time)/(f_max)

    def set_feedback(self, elapsed_time, need_migration):
        if self.burn_runtime == None or elapsed_time == 0:
            return

        u_t = self.calculate_v(elapsed_time)
        self.n[self.burn_runtime] += 1
        self.t += 1

        self.u[self.burn_runtime] = (1.0 - 1.0/self.n[self.burn_runtime])*self.u[self.burn_runtime] + (1.0/self.n[self.burn_runtime])*u_t
        _log.info("UCB: Setting feedback: app_id=%s t=%d f=%f v=%f new y=%s n=%s" % (self.app_id, self.t, elapsed_time, u_t, str(self.u), str(self.n)))


    def choose_k(self, need_migrate):
        burn_runtime = None
        upper_bound = 0.0
        bounds = {}
        for k_t, u_t in self.u.iteritems():
            k_upper_bound = 0
            if (self.n[k_t] == 0):
                k_upper_bound = sys.float_info.max
            else:
                k_upper_bound = u_t + math.sqrt(self.alpha*math.log(self.t)/(2.0*self.n[k_t]))
            bounds[k_t] = k_upper_bound
            if k_upper_bound > upper_bound:
                upper_bound = k_upper_bound
                burn_runtime = k_t
        
        _log.info("UCB: Choosing k: app_id=%s t=%d x=%s burn_id=%s burn_runtime=%s, u=%s upper_bound=%f n=%s" % (self.app_id, self.t, str(bounds), self.burn_id, burn_runtime, str(self.u), upper_bound, str(self.n)))
        if burn_runtime != None and burn_runtime != self.burn_runtime:
            self.burn_runtime = burn_runtime
            return self.burn_id, self.burn_runtime
        else:
            return None, None


    def collect_runtime_cpu(self, key, value):
        """ Not used in UCB """
        if not value or value == response.NOT_FOUND:
            value = 0
        return

class UCB2(UCB):
    """ Upper Confidence Bound 2:
        Source: https://github.com/johnmyleswhite/BanditsBook/blob/master/python/algorithms/ucb/ucb2.py """

    def __init__(self, app_id):
        super(UCB2, self).__init__(app_id)
        self.next_t = 1
        self.r = {} # number of times (in blocks) each runtime was selected


    def state(self):
        state = super(UCB2, self).state()
        state['r'] = self.r
        state['next_t'] = self.next_t
        return state

    def set_state(self, state):
        super(UCB2, self).set_state(state)
        self.r = state.get('r', {})
        self.next_t = state.get('next_t', 1)

    def set_burn(self, burn_id, burn_mips, possible_runtimes, runtime_cpu_total, dump_runtime):
        super(UCB2, self).set_burn(burn_id, burn_mips, possible_runtimes, runtime_cpu_total, dump_runtime)
        self.r = { i : 0 for i in self.k }

    def choose_k(self, need_migrate):
        for k_t, u_t in self.u.iteritems():
            if (self.n[k_t] == 0):
                self.__set_arm(k_t)
                _log.info("UCB: Initializing app_id=%s burn_runtime=%s" % (self.app_id, self.burn_runtime))
                return self.burn_id, self.burn_runtime

        if self.next_t > self.t:
            _log.info("UCB: In batch, app_id=%s next_t=%d t=%d" % (self.app_id, self.next_t, self.t))
            return None, None

        burn_runtime = None
        upper_bound = 0.0
        bounds = {}
        for k_t, u_t in self.u.iteritems():
            k_upper_bound = u_t + math.sqrt((1. + self.alpha) * math.log(math.e * float(sum(self.n.values())) / self.__tau(self.r[k_t])) / (2 * self.__tau(self.r[k_t])))
            bounds[k_t] = k_upper_bound
            if k_upper_bound > upper_bound:
                upper_bound = k_upper_bound
                burn_runtime = k_t

        self.__set_arm(burn_runtime)
        _log.info("UCB: Choosing k: app_id=%s t=%d x=%s burn_id=%s burn_runtime=%s, u=%s upper_bound=%f n=%s next_t=%d" % (self.app_id, self.t, str(bounds), self.burn_id, burn_runtime, str(self.u), upper_bound, str(self.n), self.next_t))
        return self.burn_id, self.burn_runtime

    def __set_arm(self, runtime):
        self.burn_runtime = runtime
        self.next_t = self.t + max(1, self.__tau(self.r[runtime] + 1) - self.__tau(self.r[runtime]))
        self.r[runtime] += 1

    def __tau(self, r):
        return int(math.ceil((1 + self.alpha) ** r))

