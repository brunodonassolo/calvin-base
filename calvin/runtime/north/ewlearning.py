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

GOOD_ELAPSED=0.5
TOLERANCE=1.2

class TrialAndErrorBase(object):
    class FSM(object):

        def __init__(self, states, initial, transitions, hooks=None, allow_invalid_transitions=False):
            self.states = states
            self._state = initial
            self.transitions = transitions
            self.hooks = hooks or {}
            self.allow_invalid_transitions = allow_invalid_transitions

        def state(self):
            return self._state

        def transition_to(self, new_state):
            if new_state in self.transitions[self._state]:
                hook = self.hooks.get((self._state, new_state), None)
                if hook:
                    hook()
                self._state = new_state
            else:
                msg = "Invalid transition %s -> %s" % (self, self.printable(new_state))
                if self.allow_invalid_transitions:
                    _log.warning("ALLOWING " + msg)
                    self._state = new_state
                else:
                    raise Exception(msg)

        def printable(self, state):
            return self.states.reverse_mapping[state]

        def __str__(self):
            return self.printable(self._state)


    def __init__(self, enabled=True):
        self.enabled = enabled

    def update_v(self, v, burn_runtime):
        if not self.enabled:
            return
        method = getattr(self, str(self.fsm), lambda : _log.error("Invalid state transition"))
        method(v, burn_runtime)

    def should_migrate(self):
        raise Exception("not implemented")

    def set_discontent(self):
        raise Exception("not implemented")

    def has_given_up(self):
        raise Exception("not implemented")


class TrialAndError(TrialAndErrorBase):

    STATE = enum('CONTENT', 'DISCONTENT', 'WATCHFUL')

    VALID_TRANSITIONS = {
        STATE.CONTENT    : [STATE.CONTENT, STATE.WATCHFUL, STATE.DISCONTENT],
        STATE.DISCONTENT : [STATE.CONTENT, STATE.DISCONTENT],
        STATE.WATCHFUL   : [STATE.CONTENT, STATE.WATCHFUL, STATE.DISCONTENT],
    }

    def __init__(self, app_id, enabled=True, n_watch = 10):
        super(TrialAndError, self).__init__(enabled)
        self.fsm = TrialAndErrorBase.FSM(TrialAndError.STATE, TrialAndError.STATE.CONTENT, TrialAndError.VALID_TRANSITIONS)
        self.current_runtime = None
        self.count = 0
        self.n_watch = n_watch
        self.app_id = app_id

    def CONTENT(self, v, burn_runtime):
        best = max(v, key= lambda x: v.get(x))
        self.count = 0
        _log.info("Trial and error: state: content, app_id=%s, current_runtime=%s, best=%s" % (self.app_id, self.current_runtime, best))
        if best != self.current_runtime:
            self.fsm.transition_to(TrialAndError.STATE.WATCHFUL)

    def DISCONTENT(self, v, burn_runtime):
        _log.info("Trial and error: state: discontent, app_id=%s, current_runtime=%s, new=%s" % (self.app_id, self.current_runtime, burn_runtime))
        self.current_runtime = burn_runtime
        self.fsm.transition_to(TrialAndError.STATE.CONTENT)

    def WATCHFUL(self, v, burn_runtime):
        self.count += 1
        best = max(v, key= lambda x: v.get(x))
        _log.info("Trial and error: state: watchful, app_id=%s, current_runtime=%s, best=%s" % (self.app_id, self.current_runtime, best))
        if best == self.current_runtime:
            self.fsm.transition_to(TrialAndError.STATE.CONTENT)
        elif self.count >= self.n_watch:
            self.fsm.transition_to(TrialAndError.STATE.DISCONTENT)

    def should_migrate(self):
        return self.fsm.state() == TrialAndError.STATE.DISCONTENT

    def set_discontent(self):
        if not self.enabled:
            return
        self.fsm.transition_to(TrialAndError.STATE.DISCONTENT)

    def has_given_up(self):
        return False

class NiceTrialAndError(TrialAndErrorBase):

    STATE = enum('CONTENT', 'DISCONTENT', 'WATCHFUL', 'GIVEUP')

    VALID_TRANSITIONS = {
        STATE.CONTENT    : [STATE.CONTENT, STATE.WATCHFUL, STATE.DISCONTENT],
        STATE.DISCONTENT : [STATE.CONTENT, STATE.DISCONTENT, STATE.GIVEUP],
        STATE.WATCHFUL   : [STATE.CONTENT, STATE.WATCHFUL, STATE.DISCONTENT],
        STATE.GIVEUP     : [STATE.CONTENT, STATE.GIVEUP],
    }

    def __init__(self, app_id, enabled=True, n_watch = 10, n_giveup=5, time_giveup=300):
        super(NiceTrialAndError, self).__init__(enabled)
        self.fsm = TrialAndErrorBase.FSM(NiceTrialAndError.STATE, NiceTrialAndError.STATE.CONTENT, NiceTrialAndError.VALID_TRANSITIONS)
        self.current_runtime = None
        self.count = 0
        self.n_giveup = n_giveup
        self.time_giveup = time_giveup
        self.init_giveup = 0
        self.n_watch = n_watch
        self.app_id = app_id
        self.discontent_timestamps = collections.deque([0]*self.n_giveup, maxlen=self.n_giveup)

    def CONTENT(self, v, burn_runtime):
        best = max(v, key= lambda x: v.get(x))
        self.count = 0
        _log.info("Trial and error: state: content, app_id=%s, current_runtime=%s, best=%s" % (self.app_id, self.current_runtime, best))
        if best != self.current_runtime:
            self.fsm.transition_to(NiceTrialAndError.STATE.WATCHFUL)

    def DISCONTENT(self, v, burn_runtime):
        _log.info("Trial and error: state: discontent, app_id=%s, current_runtime=%s, new=%s" % (self.app_id, self.current_runtime, burn_runtime))
        self.current_runtime = burn_runtime
        self.discontent_timestamps.append(time.time())
        if (self.discontent_timestamps[0] != 0 and (self.discontent_timestamps[-1] - self.discontent_timestamps[0]) < self.time_giveup):
            self.init_giveup = time.time()
            self.fsm.transition_to(NiceTrialAndError.STATE.GIVEUP)
        else:
            self.fsm.transition_to(NiceTrialAndError.STATE.CONTENT)

    def WATCHFUL(self, v, burn_runtime):
        self.count += 1
        best = max(v, key= lambda x: v.get(x))
        _log.info("Trial and error: state: watchful, app_id=%s, current_runtime=%s, best=%s" % (self.app_id, self.current_runtime, best))
        if best == self.current_runtime:
            self.fsm.transition_to(NiceTrialAndError.STATE.CONTENT)
        elif self.count >= self.n_watch:
            self.fsm.transition_to(NiceTrialAndError.STATE.DISCONTENT)

    def GIVEUP(self, v, burn_runtime):
        elapsed = time.time() - self.init_giveup 
        _log.info("Trial and error: state: giveup, app_id=%s, current_runtime=%s, elapsed=%f" % (self.app_id, self.current_runtime, elapsed))
        if elapsed >= self.time_giveup:
            self.fsm.transition_to(NiceTrialAndError.STATE.CONTENT)
        if elapsed > 60 and max(v.values()) > 0.9:
            self.fsm.transition_to(NiceTrialAndError.STATE.CONTENT)

    def should_migrate(self):
        return self.fsm.state() == NiceTrialAndError.STATE.DISCONTENT

    def set_discontent(self):
        if not self.enabled:
            return
        self.fsm.transition_to(NiceTrialAndError.STATE.DISCONTENT)

    def has_given_up(self):
        return self.fsm.state() == NiceTrialAndError.STATE.GIVEUP

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
        self.reconfig = ReconfigAlgos()
        self.trial = getattr(sys.modules[__name__],self.reconfig.get_trial_and_error_version())(self.app_id, self.reconfig.is_trial_and_error())
        self.dump_runtime = None

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
        state['dump_runtime'] = self.dump_runtime
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
        self.dump_runtime = state.get('dump_runtime', None)
        self.app_id = state.get('app_id', None)
        self.runtime_cpu_avail = state.get('runtime_cpu_avail', {})
        self.runtime_cpu_total = state.get('runtime_cpu_total', {})

    def set_burn(self, burn_id, burn_mips, possible_runtimes, runtime_cpu_total, dump_runtime):
        _log.info("EW learn: app_id=%s burn_id=%s burn_mips=%f, possible runtimes init=%s, dump_runtime=%s" % (self.app_id, burn_id, burn_mips, str(possible_runtimes), dump_runtime))
        self.burn_id = burn_id
        self.burn_mips = burn_mips
        self.dump_runtime = dump_runtime
        self.K = min(len(possible_runtimes), self.K)
        self.k = random.sample(possible_runtimes, k=self.K)
        for r in self.k:
            self.runtime_cpu_avail[r] = runtime_cpu_total.get(r, 10000) # high value to avoid filtering
            self.runtime_cpu_total[r] = runtime_cpu_total.get(r, 0)

        self.y = { i : 0 for i in self.k }
        self.x = { i : 0 for i in self.k }
        self.count = { i : 0 for i in self.k }
        self.count[dump_runtime] = 0

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

    def estimator_v2_single_token(self, avail_cpu):
        burn_mips = self.burn_mips
        if burn_mips >= 100:
            burn_mips = self.burn_mips/5 # hitting the rock bottom... :/
        return (0.25 + burn_mips/avail_cpu)

    def estimator_v2(self, x, elapsed_time):
        used_est = (self.runtime_cpu_total[x] - self.runtime_cpu_avail[x]) + self.burn_mips # current use + this app
        if x == self.burn_runtime:
            used_est = (self.runtime_cpu_total[x] - self.runtime_cpu_avail[x]) # considers that CPU usage is updated if app is running on the runtime
        if used_est < self.runtime_cpu_total[x]:
            return self.calculate_v(self.estimator_v2_single_token(self.runtime_cpu_avail[x]), x, False)
        elif used_est > self.runtime_cpu_total[x]*TOLERANCE:
            return self.calculate_v(self.f_max, x, False)
        else:
            # a = x2 - x1/y2 - y1
            a = (self.f_max - self.estimator_v2_single_token(self.burn_mips))/(TOLERANCE*self.runtime_cpu_total[x] - self.runtime_cpu_total[x])
            # b = y - ax
            b = self.estimator_v2_single_token(self.burn_mips) - a*self.runtime_cpu_total[x]
            # y = ax + b
            elapsed = a*used_est + b
            return self.calculate_v(elapsed, x, False)

    def _get_vector_v(self, elapsed_time):
        v_obs = { i : 0 if i != self.burn_runtime else self.calculate_v(elapsed_time, i) for i in self.k }
        method = getattr(self, str(self.reconfig.get_estimator()))
        v_est = { i : method(i, elapsed_time) for i in self.k }
        v = { i : self.lamb*v_obs[i] + (1 - self.lamb)*v_est[i] for i in self.k }
        _log.info("EW learning: Calculating v: app_id=%s t=%d lambda=%f v_obs=%s v_est=%s burn_mips=%f cpu_available=%s algo=%s" % (self.app_id, self.t, self.lamb, str(v_obs), str(v_est), self.burn_mips, str(self.runtime_cpu_avail), self.algo))
        return v

    def set_feedback(self, elapsed_time):
        if self.burn_runtime == None or elapsed_time == 0:
            return

        v = self._get_vector_v(elapsed_time)
        step = self.learn_rate/math.sqrt(self.t)
        self.y = { i : j + step*v[i] for i,j in self.y.iteritems() }
        _log.info("EW learning: Setting feedback: app_id=%s t=%d f=%f v=%s new y=%s learn_rate=%f step=%f" % (self.app_id, self.t, elapsed_time, str(v), str(self.y), self.learn_rate, step))
        v[self.burn_runtime] = self.calculate_v(elapsed_time, self.burn_runtime, bandit=False)
        self.trial.update_v(v, self.burn_runtime)
        #print "fffffffffffffff"
        #print("EW learning: Setting feedback: app_id=%s t=%d f=%f v=%s new y=%s" % (self.app_id, self.t, elapsed_time, str(v), str(self.y)))


    def choose_k(self, need_migration):
        for k_t, y_t in self.y.iteritems():
            total = sum([math.exp(j - y_t) for i,j in self.y.iteritems()])
            self.x[k_t] = (1 - self.eps)*(1/(total)) + self.eps*1/self.K
        self.t += 1
        prob = [ self.x[i] for i in self.k ]
        burn_runtime = self.burn_runtime
        if need_migration or burn_runtime == None or self.trial.should_migrate():
            if self.trial.has_given_up():
                burn_runtime = self.dump_runtime
            else:
                burn_runtime = numpy.random.choice(self.k, p=prob)
                self.trial.set_discontent()
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
