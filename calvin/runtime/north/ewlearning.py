import random
import math
import numpy
from calvin.utilities import calvinconfig
from calvin.utilities import calvinlogger
import calvin.requests.calvinresponse as response

_conf = calvinconfig.get()
_log = calvinlogger.get_logger(__name__)

GOOD_ELAPSED=0.5
TOLERANCE=1.2

class EwLearning(object):
    """ Exponential Weights Learning """

    def __init__(self, app_id):
        self.K = _conf.get("learn", "K")
        self.eps = _conf.get("learn", "epsilon")
        self.f_max = _conf.get("learn", "f_max")
        self.lamb = _conf.get("learn", "lambda")
        self.x = {} # prob
        self.y = {} # feedback
        self.k = [] # runtimes
        self.t = 0  # time step
        self.learn_rate = _conf.get("learn", "learn_rate")
        self.count = {} # count number of times each runtime was selected
        self.burn_id = None
        self.burn_runtime = None
        self.burn_mips = 0
        self.app_id = app_id
        self.runtime_cpu_avail = {}
        self.runtime_cpu_total = {}
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
        state['learn_rate'] = self.learn_rate
        state['lamb'] = self.lamb
        state['burn_id'] = self.burn_id
        state['burn_mips'] = self.burn_mips
        state['burn_runtime'] = self.burn_runtime
        state['runtime_cpu_avail'] = self.runtime_cpu_avail
        state['runtime_cpu_total'] = self.runtime_cpu_total
        state['app_id'] = self.app_id
        return state

    def set_state(self, state):
        self.x = state.get('x', {})
        self.y = state.get('y', {})
        self.count = state.get('count', {})
        self.k = state.get('k', [])
        self.K = state.get('K', _conf.get("learn", "K"))
        self.algo = state.get('algo', _conf.get("global", "reconfig_algorithm"))
        self.eps = state.get('eps', _conf.get("learn", "epsilon"))
        self.f_max = state.get('f_max', _conf.get("learn", "f_max"))
        self.lamb = state.get('lamb', _conf.get("learn", "lambda"))
        self.learn_rate = state.get('learn_rate', _conf.get("learn", "learn_rate"))
        self.t = state.get('t', 0)
        self.burn_id = state.get('burn_id', None)
        self.burn_mips = state.get('burn_mips', 0)
        self.burn_runtime = state.get('burn_runtime', None)
        self.app_id = state.get('app_id', None)
        self.runtime_cpu_avail = state.get('runtime_cpu_avail', {})
        self.runtime_cpu_total = state.get('runtime_cpu_total', {})

    def set_burn(self, burn_id, burn_mips, possible_runtimes, runtime_cpu_total):
        _log.info("EW learn: app_id=%s burn_id=%s burn_mips=%f, possible runtimes init=%s" % (self.app_id, burn_id, burn_mips, str(possible_runtimes)))
        self.burn_id = burn_id
        self.burn_mips = burn_mips
        self.K = min(len(possible_runtimes), self.K)
        self.k = random.sample(possible_runtimes, k=self.K)
        for r in self.k:
            self.runtime_cpu_avail[r] = runtime_cpu_total.get(r, 10000) # high value to avoid filtering
            self.runtime_cpu_total[r] = runtime_cpu_total.get(r, 0)

        self.y = { i : 0 for i in self.k }
        self.x = { i : 0 for i in self.k }
        self.count = { i : 0 for i in self.k }

    def calculate_v(self, elapsed_time, burn_runtime, bandit=True):
        f_max = self.f_max
        if elapsed_time > f_max:
            _log.warning("EW learning: elapsed_time=%f greater than f_max=%f" % (elapsed_time, f_max))
            f_max = elapsed_time
        if bandit:
            return ((f_max - elapsed_time)/(f_max))*(1/self.x[burn_runtime])
        else:
            return (f_max - elapsed_time)/(f_max)

    def estimator(self, x, elapsed_time):
        used_est = (self.runtime_cpu_total[x] - self.runtime_cpu_avail[x]) + self.burn_mips # current use + this app
        if x == self.burn_runtime:
            used_est = (self.runtime_cpu_total[x] - self.runtime_cpu_avail[x]) # considers that CPU usage is updated if app is running on the runtime
        if used_est < self.runtime_cpu_total[x]:
            return self.calculate_v(GOOD_ELAPSED, x, False)
        elif used_est > self.runtime_cpu_total[x]*TOLERANCE:
            return self.calculate_v(self.f_max, x, False)
        else:
            # a = x2 - x1/y2 - y1
            a = (self.f_max - GOOD_ELAPSED)/(TOLERANCE*self.runtime_cpu_total[x] - self.runtime_cpu_total[x])
            # b = y - ax
            b = GOOD_ELAPSED - a*self.runtime_cpu_total[x]
            # y = ax + b
            elapsed = a*used_est + b
            return self.calculate_v(elapsed, x, False)

    def _get_vector_v(self, elapsed_time):
        v_obs = { i : 0 if i != self.burn_runtime else self.calculate_v(elapsed_time, i) for i in self.k }
        v_est = {}
        if self.algo == "app_learn_v1": #semi bandit
            v_est = { i : self.estimator(i, elapsed_time) for i in self.k }
            v = { i : self.lamb*v_obs[i] + (1 - self.lamb)*v_est[i] for i in self.k }
        else:
            v = v_obs
        _log.info("EW learning: Calculating v: app_id=%s t=%d lambda=%f v_obs=%s v_est=%s burn_mips=%f cpu_available=%s algo=%s" % (self.app_id, self.t, self.lamb, str(v_obs), str(v_est), self.burn_mips, str(self.runtime_cpu_avail), self.algo))
        return v

    def set_feedback(self, elapsed_time):
        if self.burn_runtime == None or elapsed_time == 0:
            return

        v = self._get_vector_v(elapsed_time)
        step = self.learn_rate/math.sqrt(self.t)
        self.y = { i : j + step*v[i] for i,j in self.y.iteritems() }
        _log.info("EW learning: Setting feedback: app_id=%s t=%d f=%f v=%s new y=%s learn_rate=%f step=%f" % (self.app_id, self.t, elapsed_time, str(v), str(self.y), self.learn_rate, step))
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

        self.runtime_cpu_avail[key] = value
