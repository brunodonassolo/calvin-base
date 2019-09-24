import random
import math
import numpy
from calvin.utilities import calvinconfig
from calvin.utilities import calvinlogger

_conf = calvinconfig.get()
_log = calvinlogger.get_logger(__name__)


class EwLearning(object):
    """ Exponential Weights Learning """

    def __init__(self, app_id):
        self.K = _conf.get("learn", "K") or 5
        self.eps = _conf.get("learn", "eps") or 0
        self.f_max = _conf.get("learn", "f_max") or 10
        self.x = {} # prob
        self.y = {} # feedback
        self.k = [] # runtimes
        self.t = 0  # time step
        self.count = {} # count number of times each runtime was selected
        self.burn_id = None
        self.burn_runtime = None
        self.app_id = app_id

    def __str__(self):
        return "x=%s\ny=%s\nk=%s\nburn id=%s\nk_t=%s\nt=%d\ncount=%s" % (str(self.x), str(self.y), str(self.k), self.burn_id, self.burn_runtime, self.t, self.count)


    def state(self):
        state = {}
        state['x'] = self.x
        state['y'] = self.y
        state['k'] = self.k
        state['t'] = self.t
        state['K'] = self.K
        state['count'] = self.count
        state['eps'] = self.eps
        state['f_max'] = self.f_max
        state['burn_id'] = self.burn_id
        state['burn_runtime'] = self.burn_runtime
        state['app_id'] = self.app_id
        return state

    def set_state(self, state):
        self.x = state.get('x', {})
        self.y = state.get('y', {})
        self.count = state.get('count', {})
        self.k = state.get('k', [])
        self.K = state.get('K', _conf.get("learn", "K") or 5)
        self.eps = state.get('eps', _conf.get("learn", "eps") or 0)
        self.f_max = state.get('f_max', _conf.get("learn", "f_max") or 10)
        self.t = state.get('t', 0)
        self.burn_id = state.get('burn_id', None)
        self.burn_runtime = state.get('burn_runtime', None)
        self.app_id = state.get('app_id', None)

    def set_burn(self, burn_id, possible_runtimes):
        _log.info("EW learn: app_id=%s possible runtimes init=%s" % (self.app_id, str(possible_runtimes)))
        self.burn_id = burn_id
        self.K = min(len(possible_runtimes), self.K)
        self.k = random.sample(possible_runtimes, k=self.K)
        self.y = { i : 0 for i in self.k }
        self.x = { i : 0 for i in self.k }
        self.count = { i : 0 for i in self.k }

    def set_feedback(self, elapsed_time):
        if self.burn_runtime == None or elapsed_time == 0:
            return

        f_max = self.f_max
        if elapsed_time > f_max:
            _log.warning("EW learning: elapsed_time=%f greater than f_max=%f" % (elapsed_time, f_max))
            f_max = elapsed_time
        v = { i : 0 if i != self.burn_runtime else ((f_max - elapsed_time)/(f_max))*(1/self.x[self.burn_runtime]) for i in self.k }
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
