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
from calvin.runtime.north.ewlearning import EwLearning

_conf = calvinconfig.get()
_log = calvinlogger.get_logger(__name__)

N = 1000 # number of time steps "max"

class SAO(object):
    """ Stochastic and Adversarial Optimal algorithm
    http://sbubeck.com/COLT12_BS.pdf
    """

    def __init__(self, app_id):
        self.K = _conf.get("learn", "K")
        self.k = [] # runtimes
        self.n = {} # number of times selected
        self.p = {} # probability
        self.q = {} # prob on deactivation
        self.tau = {} # time when arm is deactivated
        self.H_tilde = {}
        self.H_tilde_total = {}
        self.H_circumflex = {}
        self.A = set() # active arms
        self.t = 1  # time step
        self.f_max = _conf.get("learn", "f_max")
        self.exp3_active = False
        self.exp3 = None
        self.burn_id = None
        self.burn_runtime = None
        self.burn_mips = 0
        self.beta = _conf.get("learn", "beta")
        self.app_id = app_id
        self.algo = _conf.get("global", "reconfig_algorithm")
        self.reconfig = ReconfigAlgos()
        self.dump_runtime = None
        self.runtime_cpu_total = {}

    def __str__(self):
        return "n=%s k=%s burn_id=%s k_t=%s t=%d K=%d f_max=%f p=%s q=%s tau=%s H_tilde=%s H_circumflex=%s A=%s" % (str(self.n), str(self.k), self.burn_id, self.burn_runtime, self.t, self.K, self.f_max, str(self.p), str(self.q), str(self.tau), str(self.H_tilde), str(self.H_circumflex), str(self.A))


    def state(self):
        state = {}
        state['n'] = self.n
        state['p'] = self.p
        state['q'] = self.q
        state['k'] = self.k
        state['t'] = self.t
        state['K'] = self.K
        state['exp3_active'] = self.exp3_active
        state['A'] = list(self.A)
        state['beta'] = self.beta
        state['tau'] = self.tau
        state['H_tilde'] = self.H_tilde
        state['H_tilde_total'] = self.H_tilde_total
        state['H_circumflex'] = self.H_circumflex
        state['algo'] = self.algo
        state['f_max'] = self.f_max
        state['burn_id'] = self.burn_id
        state['burn_mips'] = self.burn_mips
        state['burn_runtime'] = self.burn_runtime
        state['app_id'] = self.app_id
        state['dump_runtime'] = self.dump_runtime
        state['runtime_cpu_total'] = self.runtime_cpu_total
        return state


    def set_state(self, state):
        self.n = state.get('n', {})
        self.p = state.get('p', {})
        self.q = state.get('q', {})
        self.k = state.get('k', [])
        self.t = state.get('t', 0)
        self.exp3_active = state.get('exp3_active', False)
        self.K = state.get('K', _conf.get("learn", "K"))
        self.A = set(state.get('A', []))
        self.beta = state.get('beta', _conf.get("learn", "beta"))
        self.tau = state.get('tau', {})
        self.H_tilde = state.get('H_tilde', {})
        self.H_tilde_total = state.get('H_tilde_total', {})
        self.H_circumflex = state.get('H_circumflex', {})
        self.algo = state.get('algo', _conf.get("global", "reconfig_algorithm"))
        self.f_max = state.get('f_max', _conf.get("learn", "f_max"))
        self.burn_id = state.get('burn_id', None)
        self.burn_mips = state.get('burn_mips', 0)
        self.burn_runtime = state.get('burn_runtime', None)
        self.app_id = state.get('app_id', None)
        self.dump_runtime = state.get('dump_runtime', None)
        self.runtime_cpu_total = state.get('runtime_cpu_total', {})

    def set_burn(self, burn_id, burn_mips, possible_runtimes, runtime_cpu_total, dump_runtime):

        _log.info("SAO: app_id=%s burn_id=%s burn_mips=%f, possible runtimes init=%s " % (self.app_id, burn_id, burn_mips, str(possible_runtimes)))
        self.burn_id = burn_id
        self.burn_mips = burn_mips
        self.K = min(len(possible_runtimes), self.K)
        self.k = random.sample(possible_runtimes, k=self.K)
        self.dump_runtime = dump_runtime
        self.runtime_cpu_total = runtime_cpu_total

        # initializing - line 1 to 5

        self.tau = { i : N for i in self.k }
        self.A = set(self.k)
        self.n = { i : 0 for i in self.k }
        self.H_tilde = { i : 0.0 for i in self.k }
        self.H_tilde_total = { i : 0.0 for i in self.k }
        self.H_circumflex = { i : 0.0 for i in self.k }
        self.p = { i : 1.0/self.K for i in self.k }
        self.q = { i : self.p[i] for i in self.k }

    def calculate_g(self, elapsed_time):
        f_max = self.f_max
        if elapsed_time > f_max:
            _log.warning("SAO: elapsed_time=%f greater than f_max=%f" % (elapsed_time, f_max))
            f_max = elapsed_time
        return (f_max - elapsed_time)/(f_max)

    def _get_H_tilde_max(self):
        H_tilde_max = 0
        for j in self.A:
            if self.H_tilde[j] > H_tilde_max:
                H_tilde_max = self.H_tilde[j]
        return H_tilde_max

    def _consistency_tests(self): # lines 8 - 19
        for i in self.k:
            # if line 9

            if i in self.A:
                active_test_left = self._get_H_tilde_max() - self.H_tilde[i]
                active_test_right = 6.0*math.sqrt(((4.0*self.K*math.log10(self.beta))/self.t) + 5.0*((self.K*math.log10(self.beta))/self.t)**2)
                _log.info("SAO: Active test app_id: %s, runtime: %s, %f > %f" % (self.app_id, i, active_test_left, active_test_right))
                if active_test_left > active_test_right: 
                    _log.info("SAO: app_id=%s deactivating runtime=%s tau=%d q=%f" % (self.app_id, i, self.t, self.p[i]))
                    self.A.remove(i)
                    self.tau[i] = self.t
                    self.q[i] = self.p[i]

            # first test: line 15
            t_star = min(self.tau[i], self.t)
            if self.n[i] > 0 and abs(self.H_tilde[i] - self.H_circumflex[i]) > math.sqrt((2.0*math.log10(self.beta))/self.n[i]) + math.sqrt(4.0*((self.K*t_star)/(self.t**2) + ((self.t - t_star)/(self.q[i]*self.tau[i]*self.t)))*math.log10(self.beta) + 5.0*((self.K*math.log10(self.beta))/t_star)**2):
                _log.info("SAO: First property satisfied %f > %f. Initializing EXP3" % (abs(self.H_tilde[i] - self.H_circumflex[i]), math.sqrt((2.0*math.log10(self.beta))/self.n[i]) + math.sqrt(4.0*((self.K*t_star)/(self.t**2) + ((self.t - t_star)/(self.q[i]*self.tau[i]*self.t)))*math.log10(self.beta) + 5.0*((self.K*math.log10(self.beta))/t_star)**2)))
                self.exp3_active = True
                self.exp3 = EwLearning(self.app_id)
                self.exp3.set_burn(self.burn_id, self.burn_mips, self.k, self.runtime_cpu_total, self.dump_runtime)

            # second and third tests
            if i not in self.A:
                test2_left = self._get_H_tilde_max() - self.H_tilde[i]
                test2_right = 10.0*math.sqrt(((4.0*self.K*math.log10(self.beta))/(self.tau[i] - 1.0)) + 5.0*((self.K*math.log10(self.beta))/(self.tau[i] - 1.0))**2)
                test3_left = self._get_H_tilde_max() - self.H_tilde[i] 
                test3_right = 2.0*math.sqrt(((4.0*self.K*math.log10(self.beta))/(self.tau[i])) + 5.0*((self.K*math.log10(self.beta))/(self.tau[i]))**2)
                _log.info("SAO: app_id: %s, runtime: %s, second test: %f > %f, third test: %f <= %f" % (self.app_id, i, test2_left, test2_right, test3_left, test3_right))

            # second test: line 16
                if test2_left > test2_right:
                    _log.info("SAO: app_id: %s, Second property satisfied. Initializing EXP3" % (self.app_id))
                    self.exp3_active = True
                    self.exp3 = EwLearning(self.app_id)
                    self.exp3.set_burn(self.burn_id, self.burn_mips, self.k, self.runtime_cpu_total, self.dump_runtime)

            # third test: line 17
                if test3_left <= test3_right:
                    _log.info("SAO: app_id: %s, Third property satisfied. Initializing EXP3" % (self.app_id))
                    self.exp3_active = True
                    self.exp3 = EwLearning(self.app_id)
                    self.exp3.set_burn(self.burn_id, self.burn_mips, self.k, self.runtime_cpu_total, self.dump_runtime)


    def _update_probs(self): # line 20 - 21
        sum_prob = 0.0
        for j in self.k:
            if j not in self.A:
                sum_prob += (self.q[j]*self.tau[j])/(self.t + 1.0)

        for i in self.k:
            if i in self.A:
                self.p[i] = (1.0/len(self.A))*(1.0 - sum_prob)
            else:
                self.p[i] = (self.q[i]*self.tau[i])/(self.t + 1.0)



    def set_feedback(self, elapsed_time, need_migration):
        if self.exp3_active:
            self.exp3.set_feedback(elapsed_time, need_migration)
            return

        if self.burn_runtime == None or elapsed_time == 0:
            return

        g = self.calculate_g(elapsed_time)
        g_tilde = self.calculate_g(elapsed_time)*(1.0/self.p[self.burn_runtime])
        self.n[self.burn_runtime] += 1
        self.t += 1
        self.H_tilde_total[self.burn_runtime] += g_tilde
        self.H_tilde = { i : j/self.t for i, j in self.H_tilde_total.iteritems() }
        self.H_circumflex[self.burn_runtime] = (1.0 - 1.0/self.n[self.burn_runtime]
)*self.H_circumflex[self.burn_runtime] + (1.0/self.n[self.burn_runtime])*g

        self._consistency_tests()
        self._update_probs()

        _log.info("SAO: Setting feedback: app_id=%s t=%d f=%f v=%f new y=%s n=%s g=%f g_tilde=%f H_tilde=%s H_circumflex=%s" % (self.app_id, self.t, elapsed_time, g_tilde, str(self.H_tilde), str(self.n), g, g_tilde, str(self. H_tilde), str(self.H_circumflex)))


    def choose_k(self, need_migrate):
        if self.exp3_active:
            return self.exp3.choose_k(need_migrate)

        prob = [ self.p[i] for i in self.k ]
        burn_runtime = numpy.random.choice(self.k, p=prob) # line 7
        
        _log.info("SAO: Choosing k: app_id=%s t=%d x=%s burn_id=%s burn_runtime=%s, q=%s" % (self.app_id, self.t, str(self.p), self.burn_id, burn_runtime, str(self.q)))
        if burn_runtime != None and burn_runtime != self.burn_runtime:
            self.burn_runtime = burn_runtime
            return self.burn_id, self.burn_runtime
        else:
            return None, None


    def collect_runtime_cpu(self, key, value):
        if self.exp3_active:
            self.exp3.collect_runtime_cpu(key, value)
            return
        """ Not used in SAO """
        if not value or value == response.NOT_FOUND:
            value = 0
        return


