import random
import math
import numpy
from calvin.utilities import calvinconfig
from calvin.utilities import calvinlogger
import calvin.requests.calvinresponse as response

_conf = calvinconfig.get()
_log = calvinlogger.get_logger(__name__)

POOR_ELAPSED=3.0
GOOD_ELAPSED=0.5

class EwLearning(object):
    """ Exponential Weights Learning """

    def __init__(self, app_id):
        self.K = _conf.get("learn", "K") or 5
        self.eps = _conf.get("learn", "eps") or 0
        self.f_max = _conf.get("learn", "f_max") or 10
        self.lamb = _conf.get("learn", "lambda") or 0
        self.x = {} # prob
        self.y = {} # feedback
        self.k = [] # runtimes
        self.t = 0  # time step
        self.count = {} # count number of times each runtime was selected
        self.burn_id = None
        self.burn_runtime = None
        self.burn_mips = 0
        self.app_id = app_id
        self.runtime_cpu = {}
        self.algo = _conf.get("global", "reconfig_algorithm")

    def __str__(self):
        return "x=%s y=%s k=%s burn_id=%s k_t=%s t=%d count=%s K=%d eps=%f f_max=%f lambda=%f" % (str(self.x), str(self.y), str(self.k), self.burn_id, self.burn_runtime, self.t, self.count, self.K, self.eps, self.f_max, self.lamb)


    def state(self):
        state = {}
        state['x'] = self.x
        state['y'] = self.y
        state['k'] = self.k
        state['t'] = self.t
        state['K'] = self.K
        state['algo'] = self.algo
        state['count'] = self.count
        state['eps'] = self.eps
        state['f_max'] = self.f_max
        state['burn_id'] = self.burn_id
        state['burn_mips'] = self.burn_mips
        state['burn_runtime'] = self.burn_runtime
        state['app_id'] = self.app_id
        return state

    def set_state(self, state):
        self.x = state.get('x', {})
        self.y = state.get('y', {})
        self.count = state.get('count', {})
        self.k = state.get('k', [])
        self.K = state.get('K', _conf.get("learn", "K") or 5)
        self.algo = state.get('algo', _conf.get("global", "reconfig_algorithm"))
        self.eps = state.get('eps', _conf.get("learn", "eps") or 0)
        self.f_max = state.get('f_max', _conf.get("learn", "f_max") or 10)
        self.t = state.get('t', 0)
        self.burn_id = state.get('burn_id', None)
        self.burn_mips = state.get('burn_mips', 0)
        self.burn_runtime = state.get('burn_runtime', None)
        self.app_id = state.get('app_id', None)

    def set_burn(self, burn_id, burn_mips, possible_runtimes, runtime_cpu_total):
        _log.info("EW learn: app_id=%s burn_id=%s burn_mips=%f, possible runtimes init=%s" % (self.app_id, burn_id, burn_mips, str(possible_runtimes)))
        self.burn_id = burn_id
        self.burn_mips = burn_mips
        self.K = min(len(possible_runtimes), self.K)
        self.k = random.sample(possible_runtimes, k=self.K)
        for r in self.k:
            self.runtime_cpu[r] = runtime_cpu_total.get(r, 10000) # high value to avoid filtering

        self.y = { i : 0 for i in self.k }
        self.x = { i : 0 for i in self.k }
        self.count = { i : 0 for i in self.k }

    def estimator(self, x):
        if x == self.burn_runtime:
            return 0
        if self.burn_mips > self.runtime_cpu[x]:
            return ((self.f_max - POOR_ELAPSED)/(self.f_max))*(1/self.x[self.burn_runtime])
        else:
            return ((self.f_max - GOOD_ELAPSED)/(self.f_max))*(1/self.x[self.burn_runtime])

    def _get_vector_v(self, elapsed_time):
        f_max = self.f_max
        if elapsed_time > f_max:
            _log.warning("EW learning: elapsed_time=%f greater than f_max=%f" % (elapsed_time, f_max))
            f_max = elapsed_time
        v_obs = { i : 0 if i != self.burn_runtime else ((f_max - elapsed_time)/(f_max))*(1/self.x[self.burn_runtime]) for i in self.k }
        v_est = { i : self.estimator(i) for i in self.k }
        v = { i : self.lamb*v_obs[i] + (1 - self.lamb)*v_est[i] for i in self.k }
        _log.info("EW learning: Calculating v: app_id=%s t=%d lambda=%f v_obs=%s v_est=%s" % (self.app_id, self.t, self.lamb, str(v_obs), str(v_est)))
        return v

    def set_feedback(self, elapsed_time):
        if self.burn_runtime == None or elapsed_time == 0:
            return

        v = self._get_vector_v(elapsed_time)
        step = .1/math.sqrt(self.t)
        self.y = { i : j + step*v[i] for i,j in self.y.iteritems() }
        _log.info("EW learning: Setting feedback: app_id=%s t=%d f=%f v=%s new y=%s" % (self.app_id, self.t, elapsed_time, str(v), str(self.y)))
        #print "fffffffffffffff"
        #print("EW learning: Setting feedback: app_id=%s t=%d f=%f v=%s new y=%s" % (self.app_id, self.t, elapsed_time, str(v), str(self.y)))


    def choose_k(self):
        for k_t, y_t in self.y.iteritems():
            total = sum([math.exp(j - y_t) for i,j in self.y.iteritems()])
            self.x[k_t] = (1 - self.eps)*(1/(total)) + self.eps*1/self.K
        self.t += 1
        prob = [ self.x[i] for i in self.k ]
        burn_runtime = numpy.random.choice(self.k, p=prob)
        _log.info("EW learning: Choosing k: app_id=%s t=%d x=%s burn_id=%s burn_runtime=%s" % (self.app_id, self.t, str(self.x), self.burn_id, burn_runtime))
        self.count[burn_runtime] += 1
        if burn_runtime != self.burn_runtime:
            self.burn_runtime = burn_runtime
            return self.burn_id, self.burn_runtime
        else:
            return None, None
        #print "kkkkkkkkkkkkkk"
        #print("EW learning: Choosing k: app_id=%s x=%s burn_id=%s burn_runtime=%s" % (self.app_id, str(self.x), self.burn_id, self.burn_runtime))


    def collect_runtime_cpu(self, key, value):
        if not value or value == response.NOT_FOUND:
            value = 0

        self.runtime_cpu[key] = value
