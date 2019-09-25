import os
import time
import collections
import copy
import random
import heapq
from heapq import heappush, heapify, heappop
from calvin.runtime.south.async import async
from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import calvinconfig
from calvin.utilities import dynops
from calvin.utilities import calvinlogger
from calvin.utilities.requirement_matching import ReqMatch
import calvin.requests.calvinresponse as response

_log = calvinlogger.get_logger(__name__)
_conf = calvinconfig.get()

COST_LINK=0.01

from requests_futures.sessions import FuturesSession
session = FuturesSession(max_workers=10)

class Application(object):

    """ Application class """

    def __init__(self, id, name, origin_node_id, actor_manager, actors=None, deploy_info=None, links=None):
        self.id = id
        self.name = name or id
        self.ns = os.path.splitext(os.path.basename(self.name))[0]
        self.am = actor_manager
        self.actors = {} if actors is None else actors
        self.links = {} if links is None else links #links described by user application
        self.origin_node_id = origin_node_id
        self._track_actor_cb = None
        self.actor_placement = None
        # node_info contains key: node_id, value: list of actors
        # Currently only populated at destruction time
        self.node_info = {}
        self.components = {}
        self.deploy_info = deploy_info
        self._collect_placement_cb = None
        self.cpu_raw = {}
        self.ram_raw = {}
        self.app_update_used_resources()


    def app_update_used_resources(self):
        for a_id, a in self.actors.iteritems():
            req = self.get_req(a)
            cpu_raw = 0.0
            ram_raw = 0.0
            for i in req:
                if i["op"] == "node_attr_match":
                    if "cpuRaw" in i["kwargs"]["index"]:
                        cpu_raw += float(i["kwargs"]["index"]["cpuRaw"])
                    if "ramRaw" in i["kwargs"]["index"]:
                        ram_raw += float(i["kwargs"]["index"]["ramRaw"])
            self.cpu_raw[a_id] = cpu_raw
            self.ram_raw[a_id] = ram_raw

    def add_actor(self, actor_id):
        # Save actor_id and mapping to name while the actor is still on this node
        if not isinstance(actor_id, list):
            actor_id = [actor_id]
        for a in actor_id:
            self.actors[a] = self.am.actors[a].name if a in self.am.actors else None
        self.app_update_used_resources()

    def add_link(self, link_id, link_name):
        """ Add links to the application structure
        link_id: Link identifiers (UUID)
        link_name: link name
        """
        self.links[link_id] = link_name

    def remove_actor(self, actor_id):
        try:
            self.actors.pop(actor_id)
        except:
            pass

    def get_actors(self):
        return self.actors.keys()

    def get_links(self):
        """ Recovery all link ids of this application """
        return self.links.keys()

    def get_actor_name_map(self, ns):
        actors = {v: [k] for k, v in self.actors.items() if v is not None}
        # Collect all actors under top component name
        components = {}
        l = (len(ns)+1) if ns else 0
        for name, _id in actors.iteritems():
             if name.find(':',l)> -1:
                # This is a component
                # component name including optional namespace
                component = ':'.join(name.split(':')[0:(2 if ns else 1)])
                if component in components.keys():
                    components[component] += _id
                else:
                    components[component] = _id
        actors.update(components)
        return actors

    def __str__(self):
        s = "id: " + self.id + "\n"
        s += "name: " + self.name + "\n"
        for _id, name in self.actors.iteritems():
            s += "actor: " + _id + ", " + (name if name else "<UNKNOWN>") + "\n"
            if self.actor_placement and _id in self.actor_placement and self.actor_placement[_id]:
                s += "\t" + str(list(self.actor_placement[_id])) + "\n"
            elif self.actor_placement and _id in self.actor_placement:
                s += "\t" + str(self.actor_placement[_id]) + "\n"
        return s

    def clear_node_info(self):
        self.node_info = {}

    def update_node_info(self, node_id, actor_id):
        """ Collect information on current actor deployment """
        if node_id in self.node_info:
            self.node_info[node_id].append(actor_id)
        else:
            self.node_info[node_id] = [actor_id]

    def complete_node_info(self):
        return sum([len(a) for a in self.node_info.itervalues()]) == len(self.actors)

    def group_components(self):
        self.components = {}
        l = (len(self.ns)+1) if self.ns else 0
        for name in self.actors.values():
             if name.find(':',l)> -1:
                # This is part of a component
                # component name including optional namespace
                component = ':'.join(name.split(':')[0:(2 if self.ns else 1)])
                if component in self.components:
                    self.components[component].append(name)
                else:
                    self.components[component] = [name]


    def get_req(self, actor_name):
        """
        Start searching from the most specific requirement,
        then advance higher up in the component hierarchy until a requirement is found.
        """
        # N.B. self.ns should always exist (= script name)
        # Check for existence of deploy info
        if not self.deploy_info or 'requirements' not in self.deploy_info:
            # print "Application::get_req({}) -> [] NO INFO".format(actor_name)
            return []
        # Trim of script name
        _, name = actor_name.split(':', 1)
        parts = name.split(':')
        req = []
        while parts and not req:
            current = ':'.join(parts)
            req = self.deploy_info['requirements'].get(current, [])
            parts = parts[:-1]
        # print "Application::get_req({}) -> ".format(actor_name), req
        return req


class ActorPlacement():
    def __init__(self, runtime, link, phys_link):
        self.runtime = runtime
        self.link = link
        self.phys_link = phys_link
    def __str__(self):
        return '(Runtime: %s, Link: %s, Phys: %s)' % (self.runtime, self.link, self.phys_link)
    def __repr__(self):
        return self.__str__()
    def __eq__(self, other):
        return self.runtime == other.runtime

class ReconfigAlgos():
    def __init__(self):
        self.algo = _conf.get("global", "reconfig_algorithm")
        self.algos = {
                "app_cooldown": {
                    "greedy": False,
                    "lazyUpdate": False,
                    "random": 1,
                    "fake_centralized": False,
                    "centralized": False,
                    "learn": False
                    }, # cooldown
                "app_greedy": {
                    "greedy": True,
                    "lazyUpdate": False,
                    "random": 1,
                    "fake_centralized": False,
                    "centralized": False,
                    "learn": False
                    }, # greedy
                "app_v0": {
                    "greedy": True,
                    "lazyUpdate": True,
                    "random": 1,
                    "fake_centralized": False,
                    "centralized": False,
                    "learn": False
                    },    # lazy
                "app_v1": {
                    "greedy": True,
                    "lazyUpdate": True,
                    "random": 1,
                    "fake_centralized": True,
                    "centralized": False,
                    "learn": False
                    },     # fake centralized
                "app_central": {
                    "greedy": True,
                    "lazyUpdate": True,
                    "random": 1,
                    "fake_centralized": False,
                    "centralized": True,
                    "learn": False
                    }, # real centralized
                "app_central_nogreedy": {
                    "greedy": False,
                    "lazyUpdate": True,
                    "random": 1,
                    "fake_centralized": False,
                    "centralized": True,
                    "learn": False
                    }, # real centralized
                "app_farseeing": {
                    "greedy": False,
                    "lazyUpdate": True,
                    "random": 0,
                    "fake_centralized": False,
                    "centralized": False,
                    "learn": False
                    }, # farseeing
                "app_none": {
                    "greedy": False,
                    "lazyUpdate": False,
                    "random": -1,
                    "fake_centralized": False,
                    "centralized": False,
                    "learn": False
                    }, # none
                "app_learn_v0": {
                    "greedy": False,
                    "lazyUpdate": False,
                    "random": -1,
                    "fake_centralized": False,
                    "centralized": False,
                    "learn": True
                    } # learn
                }

    def is_fake_centralized(self):
        centralized = False
        try:
            centralized = self.algos[self.algo]["fake_centralized"]
        except KeyError:
            pass
        return centralized

    def is_centralized(self):
        centralized = False
        try:
            centralized = self.algos[self.algo]["centralized"]
        except KeyError:
            pass
        return centralized

    def is_greedy(self):
        greedy = False
        try:
            greedy = self.algos[self.algo]["greedy"]
        except KeyError:
            pass
        return greedy

    def is_lazy(self):
        lazy = False
        try:
            lazy = self.algos[self.algo]["lazyUpdate"]
        except KeyError:
            pass
        return lazy

    def is_learn(self):
        learn = False
        try:
            learn = self.algos[self.algo]["learn"]
        except KeyError:
            pass
        return learn

    def get_random(self):
        number = 1
        try:
            number = self.algos[self.algo]["random"]
        except KeyError:
            pass
        return number

class AppDeployer(object):

    def __init__(self, node, storage):
        self._node = node
        self.storage = storage
        self.reconfig = ReconfigAlgos()
        self.actor_by_runtime = {}
        self.phys_link_placement_runtimes = {}  # This information is quite stable, save it here to avoid collecting it each app deployment. Clean placement slate, saves the runtimes that the physical link connects
        self.farseeing_active_apps = set() # set active apps
        self.farseeing_placement = {} # actor: runtime


    def farseeing_set_app_state(self, app_id, active):
        if active:
            self.farseeing_active_apps.add(app_id)
        else:
            self.farseeing_active_apps.discard(app_id)

    ### DEPLOYMENT REQUIREMENTS ###

    def execute_requirements(self, app, cb, move, migration):
        """ Build dynops iterator to collect all possible placements,
            then trigger migration.

            For initial deployment (all actors on the current node)
        """
        application_id = app.id

        if hasattr(app, '_org_cb'):
            # application deployment requirements ongoing, abort
            cb(status=response.CalvinResponse(False))
            return
        app._org_cb = cb

        _log.analyze(self._node.id, "+ APP REQ", {}, tb=True)

        app.move = move
        app.migration = migration
        app.start_time = time.time()
        _log.info("Deployment: app: %s: start deploying: time %d" % (application_id, app.start_time))
        app.actor_placement = {}  # Clean placement slate
        actor_ids = app.get_actors()
        app.actor_placement_nbr = len(actor_ids)
        app.actor_storage = {}
        app.link_storage = {}
        app.port_storage = {}
        app.port_nbr = set()

        app.link_placement = {}  # Clean placement slate, saves possible physical links that satifies requirements
        app.phys_link_placement_runtimes = {}  # Clean placement slate, saves the runtimes that the physical link connects
        link_ids = app.get_links()

        app.cost_link_band = {}      # sum of requested cost by user for link, cache memory, it will be update in cost_for_link
        app.cost_link_lat = {}      # sum of requested cost by user for link, cache memory, it will be update in cost_for_link
        app.cost_runtime_cpu = {}   # sum of requested cost by user for runtime, update in cost_for_runtime
        app.cost_runtime_ram = {}   # sum of requested cost by user for runtime, update in cost_for_runtime
        app.runtime_cpu = {}    # available CPU in runtimes, runtime -> MIPS
        app.runtime_ram = {}    # available RAM in runtimes, runtime -> MB
        app.runtime_cpu_total = {}    # total CPU in runtimes, runtime -> MIPS
        app.runtime_ram_total = {}    # total RAM in runtimes, runtime -> MB
        app.phys_link_latency = {}  # available latency in physical link
        app.phys_link_bandwidth = {}# available bandwidth in physical link
        app.monetary_cost_ram = {}
        app.control_uri = {}
        app.futures = {}
        app.monetary_cost_cpu = {}
        app.runtimes_nbr = set()
        app.dynamic_capabilities = collections.Counter()

        app.link_placement_nbr = len(link_ids) # number of links that must be found
        app.phys_link_placement_runtimes_nbr = set()
        app.placement_done = False # controls when the placement was done
        app.batch = None

        # requests all actors placements
        for actor_id in actor_ids:
           # if actor_id not in self._node.am.actors.keys():
           #     _log.debug("Only apply requirements to local actors")
           #     app.actor_placement[actor_id] = None
           #     continue
            self.storage.get_actor(actor_id, cb=CalvinCB(self.collect_actor_storage, app=app, actor_id=actor_id))

            _log.analyze(self._node.id, "+ ACTOR REQ", {'actor_id': actor_id}, tb=True)
            r = ReqMatch(self._node,
                         callback=CalvinCB(self.collect_actor_placement, app=app, actor_id=actor_id))
            r.match_actor_registry(actor_id)
            _log.analyze(self._node.id, "+ ACTOR REQ DONE", {'actor_id': actor_id}, tb=True)

        # requests all links placements
        for link_id in link_ids:
            self.storage.get_link(link_id, cb=CalvinCB(self.collect_link_storage, app=app, link_id=link_id))
            _log.analyze(self._node.id, "+ LINK REQ", {'link_id': link_id}, tb=True)
            r = ReqMatch(self._node,
                         callback=CalvinCB(self.collect_link_placement, app=app, link_id=link_id))
            link = self._node.link_manager.links[link_id]
            r.match(requirements=link.requirements_get())
            _log.analyze(self._node.id, "+ LINK REQ DONE", {'link_id': link_id}, tb=True)

        self.storage.get("", "batch", cb=CalvinCB(func=self.collect_batch, app=app))

        _log.analyze(self._node.id, "+ DONE", {'application_id': application_id}, tb=True)

    def _verify_collect_placement(self, app):
        if (len(app.actor_placement) == app.actor_placement_nbr and
            len(app.actor_storage) == app.actor_placement_nbr and
            set(app.port_storage.keys()) == app.port_nbr and
            len(app.link_storage) == app.link_placement_nbr and
            len(app.link_placement) == app.link_placement_nbr and
            set(app.phys_link_placement_runtimes.keys()) == app.phys_link_placement_runtimes_nbr and
            app.batch is not None and
            self._calculate_cost_for_placement_verify(app)):
            self.decide_placement(app)

    def collect_batch(self, key, value, app):
        if value is not None and value == "true":
            app.batch = True
        else:
            app.batch = False

        self._verify_collect_placement(app)

    def collect_actor_storage(self, key, value, app, actor_id):
        app.actor_storage[actor_id] = value
        for port in value['inports'] + value['outports']:
            port_id = port['id']
            app.port_nbr.add(port_id)
            self.storage.get_port(port['id'], cb=CalvinCB(self.collect_port_storage, app=app, port_id=port_id))

        self._verify_collect_placement(app)

    def collect_link_storage(self, key, value, app, link_id):
        app.link_storage[link_id] = value

        self._verify_collect_placement(app)

    def collect_port_storage(self, key, value, app, port_id):
        app.port_storage[port_id] = value

        self._verify_collect_placement(app)

    def collect_actor_placement(self, app, actor_id, possible_placements, status):
        """
        Collects possible runtimes that satisfies the requirements for a certain actor
        app: Application structure
        actor_id: Actor (described in .calvin file) identifier in UUID format
        possible_placements: all possible runtimes that respect the requirements
        status: return status from ReqMatch
        """
        # TODO look at status
        _log.debug("Collect possible placements: %s for actor: %s" %(str(possible_placements), actor_id))
        print("Collect possible placements: %s for actor: %s" %(str(possible_placements), actor_id))
        app.actor_placement[actor_id] = possible_placements

        app.runtimes_nbr.update(possible_placements)
        for candidate in possible_placements:
            if isinstance(candidate, dynops.InfiniteElement):
                _log.debug("Skipping InfiniteElement in placement")
                print actor_id
                print("Skipping InfiniteElement in placement")
                continue

            if candidate not in app.runtime_cpu or candidate not in app.runtime_ram:
                if candidate not in app.dynamic_capabilities:
                    app.dynamic_capabilities.update({candidate : 5 })
                    self.storage.get("nodeCpu-", candidate, cb=CalvinCB(func=self.collect_runtime_cpu, app=app, update_cb=CalvinCB(self._verify_collect_placement, app)))
                    self.storage.get("nodeRam-", candidate, cb=CalvinCB(func=self.collect_runtime_ram, app=app, update_cb=CalvinCB(self._verify_collect_placement, app)))
                    self.storage.get_node(candidate, cb=CalvinCB(func=self.collect_monetary_costs, app=app, update_cb=CalvinCB(self._verify_collect_placement, app)))
                    #total cpu and ram for each node
                    self.storage.get("nodeCpuTotal-", candidate, cb=CalvinCB(func=self.collect_runtime_cpu_total, app=app, update_cb=CalvinCB(self._verify_collect_placement, app)))
                    self.storage.get("nodeMemTotal-", candidate, cb=CalvinCB(func=self.collect_runtime_mem_total, app=app, update_cb=CalvinCB(self._verify_collect_placement, app)))

        self._verify_collect_placement(app)

    def collect_runtime_cpu(self, key, value, app, update_cb):
        if not value or value == response.NOT_FOUND:
            value = 0

        app.runtime_cpu[key] = value
        app.dynamic_capabilities.subtract({key : 1})

        if self._calculate_cost_for_placement_verify(app):
            update_cb()

    def collect_runtime_cpu_total(self, key, value, app, update_cb):
        if not value or value == response.NOT_FOUND:
            value = 0

        app.runtime_cpu_total[key] = value
        app.dynamic_capabilities.subtract({key : 1})

        if self._calculate_cost_for_placement_verify(app):
            update_cb()

    def collect_runtime_mem_total(self, key, value, app, update_cb):
        if not value or value == response.NOT_FOUND:
            value = 0

        app.runtime_ram_total[key] = value
        app.dynamic_capabilities.subtract({key : 1})

        if self._calculate_cost_for_placement_verify(app):
            update_cb()

    def collect_monetary_costs(self, key, value, app, update_cb):
        try:
            app.monetary_cost_ram[key] = value['attributes']['public']['cost_ram']
            app.monetary_cost_cpu[key] = value['attributes']['public']['cost_cpu']
        except:
            app.monetary_cost_ram[key] = 0
            app.monetary_cost_cpu[key] = 0

        app.control_uri[key] = value['control_uris'][0]

        app.dynamic_capabilities.subtract({key : 1})

        if self._calculate_cost_for_placement_verify(app):
            update_cb()

    def collect_runtime_ram(self, key, value, app, update_cb):
        if not value or value == response.NOT_FOUND:
            value = 0

        app.runtime_ram[key] = value
        app.dynamic_capabilities.subtract({key : 1})

        if self._calculate_cost_for_placement_verify(app):
            update_cb()

    def collect_link_placement(self, app, link_id, possible_placements, status):
        """
        Collects possible physical links that matches the requirement for a certain link specified by user
        app: Application structure
        link_id: Link (described in .calvin file) identifier in UUID format
        possible_placements: all possible physical links that respect the requirements
        status: return status from ReqMatch
        """
        _log.debug("Collect possible placements: %s for link: %s" %(str(possible_placements), link_id))
        app.link_placement[link_id] = copy.copy(possible_placements)

        physical_links_not_in_set = possible_placements - app.phys_link_placement_runtimes_nbr
        # expect to receive all answers before continuing the placement
        app.phys_link_placement_runtimes_nbr.update(possible_placements)
        for candidate in physical_links_not_in_set:
            if isinstance(candidate, dynops.InfiniteElement):
                _log.debug("Skipping InfiniteElement in link placement")
                app.phys_link_placement_runtimes_nbr.discard(candidate)
                continue

            if candidate in self.phys_link_placement_runtimes:
                # we already know which runtimes this link connects
                app.phys_link_placement_runtimes[candidate] = self.phys_link_placement_runtimes[candidate]
            else:
                # requesting the runtimes that this physical link connects
                self._node.link_monitor.get_info(candidate, cb=CalvinCB(func=self.collect_link_placement_runtime, app=app, link=link_id))

        self._verify_collect_placement(app)

    def collect_phys_link_bandwidth(self, key, value, app, update_cb):
        if not value or value == response.NOT_FOUND:
            value = 0

        app.phys_link_bandwidth[key] = value
        app.dynamic_capabilities.subtract({key : 1})

        if self._calculate_cost_for_placement_verify(app):
            update_cb()

    def collect_phys_link_latency(self, key, value, app, update_cb):
        if not value or value == response.NOT_FOUND:
            value = 0

        app.phys_link_latency[key] = value
        app.dynamic_capabilities.subtract({key : 1})

        if self._calculate_cost_for_placement_verify(app):
            update_cb()

    def collect_link_placement_runtime(self, key, value, app, link):
        """
        Collects the information about the physical links got in the collect_link_placement method
        This information is the 2 runtimes that this "physical link" interconnects.
        key: Physical link identifier (UUID)
        value: Structure containing the 2 runtimes that this link interconnects
        app: Application structure
        link_id: Link (described in .calvin file) identifier in UUID format
        """
        if not value:
            _log.error("Error collecting placement for physical link %s. Application placement is probably incomplete" % (key))
            app.link_placement[link].remove(key)
            app.phys_link_placement_runtimes_nbr.discard(key)
        else:
            _log.debug("Collect runtimes for physical link: %s, source: %s, dst: %s" % (key, value['runtime1'], value['runtime2']))
            app.phys_link_placement_runtimes[key] = value
            self.phys_link_placement_runtimes[key] = value

        self._verify_collect_placement(app)

    def filter_update_placement_set(self, actor, rt_actor, dst_actor, rt_dst, plac_set):
        for opt in plac_set:
            # actor already in some placement option
            if actor in opt and opt[actor] == rt_actor:
                if dst_actor not in opt:
                    opt[dst_actor] = rt_dst
                    return
                elif opt[dst_actor] == rt_dst:
                    #nothing to do, already there
                    return
            # actor already in some placement option
            if dst_actor in opt and opt[dst_actor] == rt_dst:
                if actor not in opt:
                    opt[actor] = rt_actor
                    return
                elif opt[actor] == rt_actor:
                    #nothing to do, already there
                    return
        # neither actor nor dst_actor already in some placement option, create a new one
        plac_set.append({actor: rt_actor, dst_actor: rt_dst})

    def filter_link_placement_no_link(self, app, actor, dst_actor, plac_set):
        """
        Auxiliary method to filter runtimes when no physical link is available
        """
        rts_actor = app.actor_placement[actor]
        rts_dst = app.actor_placement[dst_actor]
        for rt in rts_actor:
            # the runtime selected must be acceptable for both actors
            if rt in rts_dst:
                self.filter_update_placement_set(actor, rt, dst_actor, rt, plac_set)

    def filter_link_placement_with_link(self, app, actor, dst_actor, link, plac_set):
        """
        Auxiliary method to filter runtimes when 1 physical link is available
        """

        _log.debug("Filtering placement for actors: %s and %s, Link: %s" % (actor, dst_actor, link))
        rt1 = app.phys_link_placement_runtimes[link]['runtime1']
        rt2 = app.phys_link_placement_runtimes[link]['runtime2']

        _log.debug("Possible runtimes: %s and %s" % (rt1, rt2))
        useful_link = False
        if rt1 in app.actor_placement[actor] and rt2 in app.actor_placement[dst_actor]:
            self.filter_update_placement_set(actor, rt1, dst_actor, rt2, plac_set)
            useful_link = True
        if rt2 in app.actor_placement[actor] and rt1 in app.actor_placement[dst_actor]:
            self.filter_update_placement_set(actor, rt2, dst_actor, rt1, plac_set)
            useful_link = True
        return useful_link


    def filter_link_placement(self, app, status):
        """
        After deciding the actors placement, filter out the possible runtimes considering the links

        General idea: For each pair of actors, select the runtimes which links respect the initial requirements.

        3 possibilities:
        1) InfinityElement: No link requirements. So, we can put the actor anywhere.
        2) set([]): No physical link matches the requirements. We must put the pair of actors in the same runtime.
        3) set(link1, link2): Several links matches the requirements. We got the first pair of runtimes that respect both actor and link requirements. Note that other possible placements are discarded.

        app: Application structure
        status: result of deployment
        """

        # build a map of actors that have links requirements
        actor_link = {}
        for link_id, phys_link_placements in app.link_placement.iteritems():
            src_actor = app.link_storage[link_id]['src_actor']
            dst_actor = app.link_storage[link_id]['dst_actor']
            actor_link.setdefault(src_actor, []).append((dst_actor, phys_link_placements))
            actor_link.setdefault(dst_actor, []).append((src_actor, phys_link_placements))

        # we are only interested in filtering actors that are in the actor_link map
        keys_plac = set(app.actor_placement.keys())
        keys_act = set(actor_link.keys())
        actors_to_filter = keys_plac & keys_act

        place_set = []
        for actor in actors_to_filter:
            _log.debug("Source actor: " + str(actor))
            for dst_actor, link_set in actor_link[actor]:
                # don't build the placement set for actors without link requirements (infiniteElement)
                if any([isinstance(n, dynops.InfiniteElement) for n in link_set]):
                    continue

                _log.debug("Dst actor: " + str(dst_actor))
                self.filter_link_placement_no_link(app, actor, dst_actor, place_set)
                found = False
                for link in link_set:
                    _log.debug("Link: " + str(link))
                    if self.filter_link_placement_with_link(app, actor, dst_actor, link, place_set):
                        found = True
                if not found and link_set:
                    _log.debug("Placement impossible for %s -> %s" % (actor, dst_actor))
                    status = response.CalvinResponse(response.CREATED)

        if place_set:
            # get the placement that contains the largest number of actors
            # and prefers the placement where actors are spread between runtimes
            # otherwise we would always put everybody in the same runtime
            place_set = sorted(place_set, key=len, reverse=True)
            place_set = sorted(place_set, key=lambda k : len(set(k.values())), reverse=True)
            elected_placement = place_set[0]
            _log.debug("Elected placement" + str(elected_placement))
            for actor in elected_placement:
                app.actor_placement[actor] = elected_placement[actor]

        _log.debug("Final actor placement " + str(app.actor_placement))

    def get_runtimes_in_a_range(self, runtime, levels):
        import json
        neighbors = set()
        neighbors.add(runtime)
        with open("neighbors.json") as json_data_file:
            neigh_list = json.load(json_data_file)

        next_runtimes = [ runtime ]
        while (levels > 0):
            next_temp = []
            for r in next_runtimes:
                for n in neigh_list[r]:
                    if n not in neighbors:
                        neighbors.add(n)
                        next_temp.append(n)
            levels -= 1
            next_runtimes = next_temp
        _log.debug(str(neighbors))

    def incremental_filter_candidates_considering_neighbors(self, app, placement, actor_id, actor_placement):
        # filter possible runtimes considering already placed actors and links
        for i in app.actor_storage[actor_id]['inports']:
            for p in app.port_storage[i['id']]['peers']:
                neigh_id = ""
                try:
                    neigh_id = app.port_storage[p[1]]['actor_id']
                except:
                    _log.debug("Didn't find neighbor actor")
                    continue

                for link_id, phys_link_placements in app.link_placement.iteritems():
                    src_actor = app.link_storage[link_id]['src_actor']
                    dst_actor = app.link_storage[link_id]['dst_actor']
                    # skip if src_actor wasn't placed for some reason
                    if src_actor not in placement:
                        continue
                    if src_actor == neigh_id and dst_actor == actor_id:
                        _log.debug("Found link connecting an actor already placed (%s) and our actor (%s), link_id: %s" % (src_actor, dst_actor, link_id))
                        if any([isinstance(n, dynops.InfiniteElement) for n in phys_link_placements]):
                            _log.debug("Link can be any physical link")
                            continue

                        # add runtime that hosts src_actor as acceptable runtimes
                        accept_runtimes = {}
                        accept_runtimes[placement[src_actor].runtime] = placement[src_actor]

                        for phy_link_id in phys_link_placements:

                            rt1 = app.phys_link_placement_runtimes[phy_link_id]['runtime1']
                            rt2 = app.phys_link_placement_runtimes[phy_link_id]['runtime2']
                            if rt1 == placement[src_actor].runtime:
                                accept_runtimes[rt2] = ActorPlacement(rt2, link_id, phy_link_id)
                            elif rt2 == placement[src_actor].runtime:
                                accept_runtimes[rt1] = ActorPlacement(rt1, link_id, phy_link_id)

                        _log.debug("Acceptable runtimes...")
                        _log.debug(str(accept_runtimes))
                        temp = { i : accept_runtimes[i] for i in accept_runtimes if i in actor_placement }
                        actor_placement.clear()
                        actor_placement.update(temp)

    def parse_requirements(self, requirements):
        parsed = []
        for i in requirements:
            if "requirements" in i:
                parsed += self.parse_requirements(i["requirements"])
            else:
                parsed.append(i)
        return parsed

    def update_cache_cost_link(self, app, link_id):
        from calvin.runtime.north.resource_monitor.link import bandwidth_text2number, latency_text2number
        link = self._node.link_manager.links[link_id]
        cost_band = 0.0
        cost_lat = 0.0
        for i in self.parse_requirements(app.get_req(app.link_storage[link_id]['name'])):
            if i["op"] == "link_attr_match":
                if "latency" in i["kwargs"]["index"]:
                    cost_lat += float(latency_text2number(i["kwargs"]["index"]["latency"]))
                if "bandwidth" in i["kwargs"]["index"]:
                    cost_band += float(bandwidth_text2number(i["kwargs"]["index"]["bandwidth"]))
        app.cost_link_band[link_id] = cost_band
        app.cost_link_lat[link_id] = cost_lat

    def cost_for_link_v2(self, app, link_id):

        # no link, just returns
        if link_id == "":
            return 0.0

        # cost already calculated and  in the cache, get value and returns...
        if link_id not in app.cost_link_band or link_id not in app.cost_link_lat:
            self.update_cache_cost_link(app, link_id)

        _log.debug("Calculating cost v2 for link %s" % (link_id))
        cost = app.cost_link_band[link_id]*COST_LINK
        _log.debug("Total cost v2: %f" % cost)
        return cost

    def cost_for_link(self, app, link_id, phy_link_id):
        # no link, just returns
        if link_id == "" or phy_link_id == "":
            return 0.0

        # cost already calculated and  in the cache, get value and returns...
        if (link_id not in app.cost_link_band) or (link_id not in app.cost_link_lat):
            self.update_cache_cost_link(app, link_id)

        _log.debug("Calculating cost for link %s, phys_link %s" % (link_id, phy_link_id))
        cost = app.cost_link_band[link_id]/float(app.phys_link_bandwidth[phy_link_id])
        if app.cost_link_lat[link_id] > 0:
            cost += float(app.phys_link_latency[phy_link_id])/app.cost_link_lat[link_id]
        _log.debug("Total cost: %f" % cost)
        return cost

    def update_cache_cost_actor(self, app, actor_id):
        _log.debug("Calculating cost for actor %s" % (actor_id))
        cost_cpu = 0.0
        cost_ram = 0.0
        for i in self.parse_requirements(app.get_req(app.actors[actor_id])):
            if i["op"] == "node_attr_match":
                if "cpu" in i["kwargs"]["index"]:
                    cost_cpu += float(i["kwargs"]["index"]["cpu"])
                if "ram" in i["kwargs"]["index"]:
                    cost_ram += float(i["kwargs"]["index"]["ram"])
                if "cpuRaw" in i["kwargs"]["index"]:
                    cost_cpu += float(i["kwargs"]["index"]["cpuRaw"])
                if "ramRaw" in i["kwargs"]["index"]:
                    cost_ram += float(i["kwargs"]["index"]["ramRaw"])
        app.cost_runtime_cpu[actor_id] = cost_cpu
        app.cost_runtime_ram[actor_id] = cost_ram

    def cost_for_runtime_v2(self, app, actor_id, runtime):
        if (actor_id not in app.cost_runtime_cpu) or (actor_id not in app.cost_runtime_ram):
            self.update_cache_cost_actor(app, actor_id)

        _log.debug("Calculating cost v2 for actor %s, runtime %s" % (actor_id, runtime))
        cost = app.cost_runtime_cpu[actor_id]*app.monetary_cost_cpu[runtime]
        cost += app.cost_runtime_ram[actor_id]*app.monetary_cost_ram[runtime]
        if (app.move and runtime == app.actor_storage[actor_id]['node_id']):
            max_ram = max(app.monetary_cost_ram.values())
            max_cpu = max(app.monetary_cost_cpu.values())
            cost = cost*2.0*(max_cpu + max_ram)
        _log.debug("Total cost v2: %f" % cost)
        return cost

    def cost_for_runtime(self, app, actor_id, runtime):
        if (actor_id not in app.cost_runtime_cpu) or (actor_id not in app.cost_runtime_ram):
            self.update_cache_cost_actor(app, actor_id)

        _log.debug("Calculating cost for actor %s, runtime %s" % (actor_id, runtime))
        cost = app.cost_runtime_cpu[actor_id]/(float(app.runtime_cpu[runtime]+0.0001))
        cost += app.cost_runtime_ram[actor_id]/(float(app.runtime_ram[runtime]+0.0001))
        _log.debug("Total cost: %f" % cost)
        return cost

    def get_neighbors_actors(self, app, actors_ids, actor_id, place_set):
        neigh_actors = {}
        try:
            for i in app.actor_storage[actor_id]['outports']:
                for p in app.port_storage[i['id']]['peers']:
                    try:
                        neigh_id = app.port_storage[p[1]]['actor_id']
                    except:
                        _log.debug("Didn't find neighbor actor")
                        continue
                    if not neigh_id in neigh_actors:
                        neigh_actors[neigh_id] = -1
                    else:
                        neigh_actors[neigh_id] -= 1
        except:
            for i in set(actors_ids):
                already_placed = { x for placement in place_set for x in placement[0].keys() }
                if i not in already_placed:
                    neigh_actors[i] = -1

        return neigh_actors

    def random_actor_placement(self, app, actor_ids):
        orphan_actors = []
        for actor_id in actor_ids:
            _log.debug("Actor id: %s, name: %s" % (actor_id, app.actors[actor_id]))
            if len(app.actor_storage[actor_id]['inports']) == 0:
                heappush(orphan_actors, (-100000, actor_id))

        _log.debug("Starting placing the actors...")
        actor_placement = {}
        while len(orphan_actors) > 0:
            neigh_actors = {}
            for prio, actor_id in orphan_actors:
                _log.debug("Actor id: %s, name: %s, prio(orphan): %d" % (actor_id, app.actors[actor_id], prio))
                options = self.incremental_placement(app, actor_placement, actor_id)
                if len(options) > 0:
                    import random
                    actor_placement[actor_id] = options[random.choice(tuple(options))]
                for i in app.actor_storage[actor_id]['outports']:
                    for p in app.port_storage[i['id']]['peers']:
                        try:
                            neigh_id = app.port_storage[p[1]]['actor_id']
                        except:
                            _log.debug("Didn't find neighbor actor")
                            continue
                        if not neigh_id in neigh_actors:
                            neigh_actors[neigh_id] = -1
                        else:
                            neigh_actors[neigh_id] -= 1
            orphan_actors = [(val, actor) for actor, val in neigh_actors.iteritems()]
            heapify(orphan_actors)

        _log.debug("Ending placing actors:...")
        return actor_placement

    def random_placement(self, app, actor_ids):
        n_samples = _conf.get('global', 'deployment_n_samples')
        place_set = []
        for i in range(0,n_samples):
            place_set.append((self.random_actor_placement(app, actor_ids), 0.0))

        self.random_calculate_cost_for_placement(app, actor_ids, place_set, cb_cost_calculated=CalvinCB(self.random_placement_finish, app, actor_ids, n_samples))

    def random_placement_finish(self, app, actor_ids, n_samples, place_set):
        _log.debug("Ending placing actors:...")
        _log.debug(str(place_set))
        print "Random placement cost: %f, n_samples: %d" % (place_set[0][1], n_samples)
        print place_set[0][0]
        placement_best = { actor: plac.runtime for actor,plac in place_set[0][0].iteritems() }
        app.actor_placement.update(placement_best)
        print "FINAL"
        print app.actor_placement
        status = response.CalvinResponse(True)

        if app.batch == True:
            self.batch_update_available_resources(app)

        app._org_cb(status=status, placement=app.actor_placement)
        del app._org_cb
        _log.analyze(self._node.id, "+ DONE", {'app_id': app.id}, tb=True)
        _log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))

    def incremental_placement(self, app, placement, actor_id):

        actor_placement = { i : ActorPlacement(i, "", "") for i in app.actor_placement[actor_id] }
        # filter possible runtimes considering already placed actors and links
        self.incremental_filter_candidates_considering_neighbors(app, placement, actor_id, actor_placement)

        _log.debug("Summary placement for actor %s: " % actor_id)
        _log.debug(str(actor_placement))
        return actor_placement


    def _calculate_cost_for_placement_verify(self, app):
        app.dynamic_capabilities += collections.Counter() # remove empty entries
        return len(app.dynamic_capabilities) == 0

    def random_calculate_cost_for_placement(self, app, actor_ids, place_set, cb_cost_calculated):
        place_set_sorted = []
        for opt,old_cost in place_set:
            cost = 0.0
            runtimes_set = set()
            for actor, actorPlac in opt.iteritems():
                runtimes_set.add(actorPlac.runtime)
            multiplier = 4*len(actor_ids)
            cost += multiplier*(len(app.runtimes_nbr) - len(runtimes_set))
            cost += multiplier*len(app.runtimes_nbr)*(len(actor_ids) - len(opt))
            place_set_sorted.append((opt, cost))
        place_set_sorted = sorted(place_set_sorted, key=lambda k : k[1])
        cb_cost_calculated(place_set_sorted)

    def latency_calculate_cost_for_placement(self, app, actor_ids, place_set, cb_cost_calculated):

        app.dynamic_capabilities = collections.Counter()
        # request for unnkown values needed by cost functions
        for opt,old_cost in place_set:
            for actor, actorPlac in opt.iteritems():
                if actorPlac.runtime not in app.runtime_cpu or actorPlac.runtime not in app.runtime_ram:
                    if actorPlac.runtime not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.runtime : 2 })
                        self.storage.get("nodeCpu-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_cpu, app=app, update_cb=CalvinCB(self.latency_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        self.storage.get("nodeRam-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_ram, app=app, update_cb=CalvinCB(self.latency_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                if actorPlac.phys_link not in app.phys_link_latency:
                    if actorPlac.phys_link != "" and actorPlac.phys_link not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.phys_link : 1 })
                        self.storage.get("linkLatency-", actorPlac.phys_link, cb=CalvinCB(func=self.collect_phys_link_latency, app=app, update_cb=CalvinCB(self.latency_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))

        if self._calculate_cost_for_placement_verify(app):
            self.latency_calculate_cost_for_placement_finish(app, actor_ids, place_set, cb_cost_calculated)


    def latency_calculate_cost_for_placement_finish(self, app, actor_ids, place_set, cb_cost_calculated):
        place_set_sorted = []
        for opt,old_cost in place_set:
            if not self.is_resource_usage_in_placement_ok(app, opt):
                continue
            cost = 0.0
            runtimes_set = set()
            for actor, actorPlac in opt.iteritems():
                if actorPlac.phys_link != "":
                    cost += float(app.phys_link_latency[actorPlac.phys_link])
                runtimes_set.add(actorPlac.runtime)
            multiplier = 1000000*app.link_placement_nbr
            cost += multiplier*(len(app.runtimes_nbr) - len(runtimes_set))
            cost += multiplier*len(app.runtimes_nbr)*(len(actor_ids) - len(opt))
            place_set_sorted.append((opt, cost))
        place_set_sorted = sorted(place_set_sorted, key=lambda k : k[1])
        print place_set_sorted
        cb_cost_calculated(place_set_sorted)

    def green_calculate_cost_for_placement(self, app, actor_ids, place_set, cb_cost_calculated):
        app.dynamic_capabilities = collections.Counter()
        # request for unnkown values needed by cost functions
        for opt,old_cost in place_set:
            for actor, actorPlac in opt.iteritems():
                if actorPlac.runtime not in app.runtime_cpu or actorPlac.runtime not in app.runtime_ram:
                    if actorPlac.runtime not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.runtime : 2 })
                        self.storage.get("nodeCpu-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_cpu, app=app, update_cb=CalvinCB(self.green_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        self.storage.get("nodeRam-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_ram, app=app, update_cb=CalvinCB(self.green_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))

        if self._calculate_cost_for_placement_verify(app):
            self.green_calculate_cost_for_placement_finish(app, actor_ids, place_set, cb_cost_calculated)

    def is_resource_usage_in_placement_ok(self, app, opt):
        runtimes_set = {}
        for actor, actorPlac in opt.iteritems():
            self.update_cache_cost_actor(app, actor)
            runtimes_set.setdefault(actorPlac.runtime, {"cpu": 0, "ram": 0})
            runtimes_set[actorPlac.runtime]["cpu"] += app.cost_runtime_cpu[actor]
            runtimes_set[actorPlac.runtime]["ram"] += app.cost_runtime_ram[actor]
            max_tolerance = _conf.get("global", "deployment_tolerance")
            if (runtimes_set[actorPlac.runtime]["cpu"] > app.runtime_cpu.setdefault(actorPlac.runtime, 0)*max_tolerance or runtimes_set[actorPlac.runtime]["ram"] > app.runtime_ram.setdefault(actorPlac.runtime, 0)*max_tolerance):
                return False
        return True

    def green_calculate_cost_for_placement_finish(self, app, actor_ids, place_set, cb_cost_calculated):
        place_set_sorted = []
        for opt,old_cost in place_set:
            if not self.is_resource_usage_in_placement_ok(app, opt):
                continue
            runtimes_set = set()
            for actor, actorPlac in opt.iteritems():
                runtimes_set.add(actorPlac.runtime)

            cost = len(runtimes_set) - len(runtimes_set & self.actor_by_runtime.viewkeys())
            place_set_sorted.append((opt, cost))
        place_set_sorted = sorted(place_set_sorted, key=lambda k : k[1])
        print place_set_sorted
        cb_cost_calculated(place_set_sorted)

    def money_calculate_cost_for_placement(self, app, actor_ids, place_set, cb_cost_calculated):
        app.dynamic_capabilities = collections.Counter()
        # request for unnkown values needed by cost functions
        for opt,old_cost in place_set:
            for actor, actorPlac in opt.iteritems():
                if actorPlac.runtime not in app.runtime_cpu or actorPlac.runtime not in app.runtime_ram:
                    if actorPlac.runtime not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.runtime : 5 })
                        self.storage.get("nodeCpu-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_cpu, app=app, update_cb=CalvinCB(self.money_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        self.storage.get("nodeRam-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_ram, app=app, update_cb=CalvinCB(self.money_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        self.storage.get_node(actorPlac.runtime, cb=CalvinCB(func=self.collect_monetary_costs, app=app, update_cb=CalvinCB(self.money_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        #total cpu and ram for each node
                        self.storage.get("nodeCpuTotal-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_cpu_total, app=app, update_cb=CalvinCB(self.money_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        self.storage.get("nodeMemTotal-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_mem_total, app=app, update_cb=CalvinCB(self.money_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))


                if actorPlac.phys_link not in app.phys_link_bandwidth or actorPlac.phys_link not in app.phys_link_latency:
                    if actorPlac.phys_link != "" and actorPlac.phys_link not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.phys_link : 1 })
                        self.storage.get("linkBandwidth-", actorPlac.phys_link, cb=CalvinCB(func=self.collect_phys_link_bandwidth, app=app, update_cb=CalvinCB(self.money_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))

        if self._calculate_cost_for_placement_verify(app):
            self.money_calculate_cost_for_placement_finish(app, actor_ids, place_set, cb_cost_calculated)

    def money_calculate_cost_for_placement_finish(self, app, actor_ids, place_set, cb_cost_calculated):
        place_set_sorted = []
        for opt,old_cost in place_set:
            if not self.is_resource_usage_in_placement_ok(app, opt):
                _log.info("Placement app: %s, opt: %s, discarded, resource usage not okay" % (app.id, str(opt)))
                continue
            cost = 0.0
            runtimes_set = set()
            for actor, actorPlac in opt.iteritems():
                cost += self.cost_for_runtime_v2(app, actor, actorPlac.runtime) + self.cost_for_link_v2(app, actorPlac.link)
                runtimes_set.add(actorPlac.runtime)
            multiplier = 1000000*COST_LINK*(len(actor_ids)+app.link_placement_nbr) # 1000000 max bandwidth value
            cost += multiplier*(len(app.runtimes_nbr) - len(runtimes_set))
            cost += multiplier*len(app.runtimes_nbr)*(len(actor_ids) - len(opt))
            place_set_sorted.append((opt, cost))
        place_set_sorted = sorted(place_set_sorted, key=lambda k : k[1])
        cb_cost_calculated(place_set_sorted)

    def grasp_calculate_cost_for_placement(self, app, actor_ids, place_set, cb_cost_calculated):
        app.dynamic_capabilities = collections.Counter()
        # request for unnkown values needed by cost functions
        for opt,old_cost in place_set:
            for actor, actorPlac in opt.iteritems():
                if actorPlac.runtime not in app.runtime_cpu or actorPlac.runtime not in app.runtime_ram:
                    if actorPlac.runtime not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.runtime : 5 })
                        self.storage.get("nodeCpu-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_cpu, app=app, update_cb=CalvinCB(self.grasp_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        self.storage.get("nodeRam-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_ram, app=app, update_cb=CalvinCB(self.grasp_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        self.storage.get_node(actorPlac.runtime, cb=CalvinCB(func=self.collect_monetary_costs, app=app, update_cb=CalvinCB(self.grasp_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        #total cpu and ram for each node
                        self.storage.get("nodeCpuTotal-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_cpu_total, app=app, update_cb=CalvinCB(self.grasp_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))
                        self.storage.get("nodeMemTotal-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_mem_total, app=app, update_cb=CalvinCB(self.grasp_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))


                if actorPlac.phys_link not in app.phys_link_bandwidth or actorPlac.phys_link not in app.phys_link_latency:
                    if actorPlac.phys_link != "" and actorPlac.phys_link not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.phys_link : 1 })
                        self.storage.get("linkBandwidth-", actorPlac.phys_link, cb=CalvinCB(func=self.collect_phys_link_bandwidth, app=app, update_cb=CalvinCB(self.grasp_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated)))

        if self._calculate_cost_for_placement_verify(app):
            self.grasp_calculate_cost_for_placement_finish(app, actor_ids, place_set, cb_cost_calculated)

    def grasp_calculate_cost_for_placement_finish(self, app, actor_ids, place_set, cb_cost_calculated):
        place_set_sorted = []
        for opt,old_cost in place_set:
            if not self.is_resource_usage_in_placement_ok(app, opt):
                continue
            cost = 0.0
            load = -1.0
            runtimes_set = set()
            for actor, actorPlac in opt.iteritems():
                cost += self.cost_for_runtime_v2(app, actor, actorPlac.runtime) + self.cost_for_link_v2(app, actorPlac.link)
                runtimes_set.add(actorPlac.runtime)
            for runtime in runtimes_set:
                if (load == -1.0 or self.grasp_load_balance_cost(app, runtime) < load):
                    load = self.grasp_load_balance_cost(app, runtime)
            multiplier = 1000000*COST_LINK*(len(actor_ids)+app.link_placement_nbr) # 1000000 max bandwidth value
            cost += multiplier*len(app.runtimes_nbr)*(len(actor_ids) - len(opt))
            place_set_sorted.append((opt, cost, load))
        place_set_sorted = sorted(place_set_sorted, key=lambda k : k[2], reverse=True) #load
        place_set_sorted = sorted(place_set_sorted, key=lambda k : k[1]) # cost
        cb_cost_calculated(place_set_sorted)

    def best_calculate_cost_for_placement(self, app, actor_ids, place_set, cb_cost_calculated, worst):

        app.dynamic_capabilities = collections.Counter()
        # request for unnkown values needed by cost functions
        for opt,old_cost in place_set:
            for actor, actorPlac in opt.iteritems():
                if actorPlac.runtime not in app.runtime_cpu or actorPlac.runtime not in app.runtime_ram:
                    if actorPlac.runtime not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.runtime : 2 })
                        self.storage.get("nodeCpu-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_cpu, app=app, update_cb=CalvinCB(self.best_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated, worst)))
                        self.storage.get("nodeRam-", actorPlac.runtime, cb=CalvinCB(func=self.collect_runtime_ram, app=app, update_cb=CalvinCB(self.best_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated, worst)))
                if actorPlac.phys_link not in app.phys_link_bandwidth or actorPlac.phys_link not in app.phys_link_latency:
                    if actorPlac.phys_link != "" and actorPlac.phys_link not in app.dynamic_capabilities:
                        app.dynamic_capabilities.update({actorPlac.phys_link : 2 })
                        self.storage.get("linkBandwidth-", actorPlac.phys_link, cb=CalvinCB(func=self.collect_phys_link_bandwidth, app=app, update_cb=CalvinCB(self.best_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated, worst)))
                        self.storage.get("linkLatency-", actorPlac.phys_link, cb=CalvinCB(func=self.collect_phys_link_latency, app=app, update_cb=CalvinCB(self.best_calculate_cost_for_placement_finish, app, actor_ids, place_set, cb_cost_calculated, worst)))

        if self._calculate_cost_for_placement_verify(app):
            self.best_calculate_cost_for_placement_finish(app, actor_ids, place_set, cb_cost_calculated, worst)


    def best_calculate_cost_for_placement_finish(self, app, actor_ids, place_set, cb_cost_calculated, worst):
        place_set_sorted = []
        for opt,old_cost in place_set:
            if not self.is_resource_usage_in_placement_ok(app, opt):
                continue
            cost = 0.0
            runtimes_set = set()
            for actor, actorPlac in opt.iteritems():
                cost += self.cost_for_runtime(app, actor, actorPlac.runtime) + self.cost_for_link(app, actorPlac.link, actorPlac.phys_link)
                runtimes_set.add(actorPlac.runtime)
            multiplier = 4*(len(actor_ids)+app.link_placement_nbr)
            if worst:
                cost += multiplier*len(runtimes_set)
                cost += multiplier*len(app.runtimes_nbr)*len(opt)
            else:
                cost += multiplier*(len(app.runtimes_nbr) - len(runtimes_set))
                cost += multiplier*len(app.runtimes_nbr)*(len(actor_ids) - len(opt))
            place_set_sorted.append((opt, cost))
        place_set_sorted = sorted(place_set_sorted, key=lambda k : k[1], reverse=worst)
        cb_cost_calculated(place_set_sorted)


    def best_first_actor_placement(self, app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement, worst):

        place_set = place_set[:n_samples]
        # all actors places... call finished callback
        if len(orphan_actors) == 0:
            cb_finish_placement(place_set)
            return

        next_actor = heappop(orphan_actors)
        prio = next_actor[0]
        actor_id = next_actor[1]

        _log.debug("Actor id: %s, name: %s, prio(orphan): %d" % (actor_id, app.actors[actor_id], prio))
        place_set_for_actor = []
        for idx in range(0, len(place_set)):
            actor_placement = place_set[idx][0]
            plac_cost = place_set[idx][1]
            options = self.incremental_placement(app, actor_placement, actor_id)
            if (len(options) == 0):
                place_set_for_actor.append((actor_placement, plac_cost))
            for opt in options:
                new_actor_placement = copy.copy(actor_placement)
                new_actor_placement[actor_id] = options[opt]
                _log.debug(str(new_actor_placement))
                place_set_for_actor.append((new_actor_placement, 0.0))

        # update next actors to be placed
        neigh_actors = self.get_neighbors_actors(app, actor_ids, actor_id, place_set)
        for actor, val in neigh_actors.iteritems():
            heappush(orphan_actors, (val, actor))

        # get cost to next step of loop
        self.best_calculate_cost_for_placement(app, actor_ids, place_set_for_actor, cb_cost_calculated=CalvinCB(self.best_first_actor_placement, app, actor_ids, n_samples, orphan_actors, cb_finish_placement = cb_finish_placement, worst=worst), worst=worst)

    def best_first_placement_finish(self, app, actor_ids, n_samples, place_set):
        _log.debug("Ending placing actors:...")
        _log.debug(str(place_set))
        print "Best First placement cost: %f, n_samples: %d" % (place_set[0][1], n_samples)
        placement_best = { actor: plac.runtime for actor,plac in place_set[0][0].iteritems() }
        app.actor_placement.update(placement_best)
        print "FINAL"
        print app.actor_placement
        status = response.CalvinResponse(True)

        app._org_cb(status=status, placement=app.actor_placement)
        del app._org_cb
        _log.analyze(self._node.id, "+ DONE", {'app_id': app.id}, tb=True)
        _log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))

    def best_first_placement(self, app, actor_ids):
        n_samples = _conf.get('global', 'deployment_n_samples')
        orphan_actors = []
        for actor_id in actor_ids:
            _log.debug("Actor id: %s, name: %s" % (actor_id, app.actors[actor_id]))
            if len(app.actor_storage[actor_id]['inports']) == 0:
                heappush(orphan_actors, (-100000, actor_id))

        _log.debug("Starting placing the actors...")
        place_set = [({}, 0.0)]
        self.best_first_actor_placement(app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement=CalvinCB(self.best_first_placement_finish, app, actor_ids, n_samples), worst=False)

    def worst_placement(self, app, actor_ids):
        n_samples = _conf.get('global', 'deployment_n_samples')
        orphan_actors = []
        for actor_id in actor_ids:
            _log.debug("Actor id: %s, name: %s" % (actor_id, app.actors[actor_id]))
            if len(app.actor_storage[actor_id]['inports']) == 0:
                heappush(orphan_actors, (-100000, actor_id))

        _log.debug("Starting placing the actors...")
        place_set = [({}, 0.0)]
        self.best_first_actor_placement(app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement=CalvinCB(self.worst_placement_finish, app, actor_ids, n_samples), worst=True)

    def latency_actor_placement(self, app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement):

        place_set = place_set[:n_samples]
        # all actors places... call finished callback
        if len(orphan_actors) == 0:
            cb_finish_placement(place_set)
            return

        next_actor = heappop(orphan_actors)
        prio = next_actor[0]
        actor_id = next_actor[1]

        _log.debug("Actor id: %s, name: %s, prio(orphan): %d" % (actor_id, app.actors[actor_id], prio))
        place_set_for_actor = []
        for idx in range(0, len(place_set)):
            actor_placement = place_set[idx][0]
            plac_cost = place_set[idx][1]
            options = self.incremental_placement(app, actor_placement, actor_id)
            if (len(options) == 0):
                place_set_for_actor.append((actor_placement, plac_cost))
            for opt in options:
                new_actor_placement = copy.copy(actor_placement)
                new_actor_placement[actor_id] = options[opt]
                _log.debug(str(new_actor_placement))
                place_set_for_actor.append((new_actor_placement, 0.0))

        # update next actors to be placed
        neigh_actors = self.get_neighbors_actors(app, actor_ids, actor_id, place_set)
        for actor, val in neigh_actors.iteritems():
            heappush(orphan_actors, (val, actor))

        # get cost to next step of loop
        self.latency_calculate_cost_for_placement(app, actor_ids, place_set_for_actor, cb_cost_calculated=CalvinCB(self.latency_actor_placement, app, actor_ids, n_samples, orphan_actors, cb_finish_placement = cb_finish_placement))

    def money_actor_placement(self, app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement):

        place_set = place_set[:n_samples]
        # all actors places... call finished callback
        if len(orphan_actors) == 0:
            cb_finish_placement(place_set)
            return

        next_actor = heappop(orphan_actors)
        prio = next_actor[0]
        actor_id = next_actor[1]

        _log.debug("Actor id: %s, name: %s, prio(orphan): %d" % (actor_id, app.actors[actor_id], prio))
        place_set_for_actor = []
        for idx in range(0, len(place_set)):
            actor_placement = place_set[idx][0]
            plac_cost = place_set[idx][1]
            options = self.incremental_placement(app, actor_placement, actor_id)
            if (len(options) == 0):
                place_set_for_actor.append((actor_placement, plac_cost))
            for opt in options:
                new_actor_placement = copy.copy(actor_placement)
                new_actor_placement[actor_id] = options[opt]
                _log.debug(str(new_actor_placement))
                place_set_for_actor.append((new_actor_placement, 0.0))

        # update next actors to be placed
        neigh_actors = self.get_neighbors_actors(app, actor_ids, actor_id, place_set)
        for actor, val in neigh_actors.iteritems():
            heappush(orphan_actors, (val, actor))

        # get cost to next step of loop
        self.money_calculate_cost_for_placement(app, actor_ids, place_set_for_actor, cb_cost_calculated=CalvinCB(self.money_actor_placement, app, actor_ids, n_samples, orphan_actors, cb_finish_placement = cb_finish_placement))

    def grasp_actor_placement(self, app, actor_ids, alpha):
        orphan_actors = []
        for actor_id in actor_ids:
            _log.debug("Actor id: %s, name: %s" % (actor_id, app.actors[actor_id]))
            if len(app.actor_storage[actor_id]['inports']) == 0:
                heappush(orphan_actors, (-100000, actor_id))

        _log.debug("Starting placing the actors...")
        actor_placement = {}
        while len(orphan_actors) > 0:
            neigh_actors = {}
            for prio, actor_id in orphan_actors:
                _log.debug("Actor id: %s, name: %s, prio(orphan): %d" % (actor_id, app.actors[actor_id], prio))
                options = self.incremental_placement(app, actor_placement, actor_id)
                if (len(options) == 0):
                    continue
                options_cost = []
                for opt in options:
                    cost = self.cost_for_runtime_v2(app, actor_id, opt)
                    options_cost.append((opt, cost))
                best_r, best_r_cost = sorted(options_cost, key=lambda k : k[1])[0]
                RCL = [ r[0] for r in options_cost if r[1] <= best_r_cost*(1+alpha) ]
                import random
                elected = random.choice(tuple(RCL))
                actor_placement[actor_id] = options[elected]

                for i in app.actor_storage[actor_id]['outports']:
                    for p in app.port_storage[i['id']]['peers']:
                        try:
                            neigh_id = app.port_storage[p[1]]['actor_id']
                        except:
                            _log.debug("Didn't find neighbor actor")
                            continue
                        if not neigh_id in neigh_actors:
                            neigh_actors[neigh_id] = -1
                        else:
                            neigh_actors[neigh_id] -= 1
            orphan_actors = [(val, actor) for actor, val in neigh_actors.iteritems()]
            heapify(orphan_actors)

        _log.debug("Ending placing actors:...")
        return actor_placement

    def green_actor_placement(self, app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement):

        place_set = place_set[:n_samples]
        # all actors places... call finished callback
        if len(orphan_actors) == 0:
            cb_finish_placement(place_set)
            return

        next_actor = heappop(orphan_actors)
        prio = next_actor[0]
        actor_id = next_actor[1]

        _log.debug("Actor id: %s, name: %s, prio(orphan): %d" % (actor_id, app.actors[actor_id], prio))
        place_set_for_actor = []
        for idx in range(0, len(place_set)):
            actor_placement = place_set[idx][0]
            plac_cost = place_set[idx][1]
            options = self.incremental_placement(app, actor_placement, actor_id)
            if (len(options) == 0):
                place_set_for_actor.append((actor_placement, plac_cost))
            for opt in options:
                new_actor_placement = copy.copy(actor_placement)
                new_actor_placement[actor_id] = options[opt]
                _log.debug(str(new_actor_placement))
                place_set_for_actor.append((new_actor_placement, 0.0))

        # update next actors to be placed
        neigh_actors = self.get_neighbors_actors(app, actor_ids, actor_id, place_set)
        for actor, val in neigh_actors.iteritems():
            heappush(orphan_actors, (val, actor))

        # get cost to next step of loop
        self.green_calculate_cost_for_placement(app, actor_ids, place_set_for_actor, cb_cost_calculated=CalvinCB(self.green_actor_placement, app, actor_ids, n_samples, orphan_actors, cb_finish_placement = cb_finish_placement))

    def latency_placement_finish(self, app, actor_ids, n_samples, place_set):
        _log.debug("Ending placing actors:...")
        _log.debug(str(place_set))
        print "Latency placement cost: %f, n_samples: %d" % (place_set[0][1], n_samples)
        placement_lat = { actor: plac.runtime for actor,plac in place_set[0][0].iteritems() }
        app.actor_placement.update(placement_lat)
        print "FINAL"
        print app.actor_placement

        status = response.CalvinResponse(True)

        app._org_cb(status=status, placement=app.actor_placement)
        del app._org_cb
        _log.analyze(self._node.id, "+ DONE", {'app_id': app.id}, tb=True)
        _log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))

    def batch_update_available_resources(self, app):
        nodes = set()
        for actor_id, node_id in app.actor_placement.iteritems():
            nodes.add(node_id)
            app.runtime_cpu[node_id] -= app.cost_runtime_cpu[actor_id]
            app.runtime_ram[node_id] -= app.cost_runtime_ram[actor_id]

        for node_id in nodes:
            cpu_mips = app.runtime_cpu[node_id]
            ram_bytes = app.runtime_ram[node_id]
            self._node.cpu_monitor.set_avail_for_node(cpu_mips, node_id)
            self._node.mem_monitor.set_avail_for_node(ram_bytes, node_id)


    def money_placement_finish(self, app, actor_ids, n_samples, place_set):
        _log.debug("Ending placing actors:...")
        _log.debug(str(place_set))
        if (len(place_set) == 0):
            status = response.CalvinResponse(True)
            app._org_cb(status=status, placement = {})
            del app._org_cb
            _log.analyze(self._node.id, "+ DONE", {'app_id': app.id}, tb=True)
            _log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))
            return

        print "Money placement cost: %f, n_samples: %d" % (place_set[0][1], n_samples)
        placement_lat = { actor: plac.runtime for actor,plac in place_set[0][0].iteritems() }
        print placement_lat
        _log.info("Money placement cost: %f, n_samples: %d, placement: %s" % (place_set[0][1], n_samples, str(placement_lat)))

        if _conf.get('global', 'grasp') == "v0":
            app.actor_placement = self.grasp_optimization(app, actor_ids, placement_lat, False)
        elif _conf.get('global', 'grasp') == "v1":
            app.actor_placement = self.grasp_optimization(app, actor_ids, placement_lat, True)
        elif _conf.get('global', 'grasp') == "v2":
            n_solutions = []
            N = n_samples
            for i in range(0,min(N, len(place_set))):
                n_solutions.append({ actor: plac.runtime for actor,plac in place_set[i][0].iteritems() })
            app.actor_placement = self.grasp_optimization_v2(app, actor_ids, n_solutions, False)
        else:
            app.actor_placement = placement_lat

        print "FINAL"
        if app.batch == True:
            self.batch_update_available_resources(app)

        # farseeing placement update
        for actor_id, node_id in app.actor_placement.iteritems():
            self.farseeing_placement[actor_id] = node_id

        print app.actor_placement
        status = response.CalvinResponse(True)
        app._org_cb(status=status, placement = app.actor_placement)
        del app._org_cb
        _log.analyze(self._node.id, "+ DONE", {'app_id': app.id}, tb=True)
        _log.info("FINAL placement: %s" % (str(app.actor_placement)))
        _log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))

    def grasp_placement_finish(self, app, actor_ids, N, place_set):
        print("Ending placing actors:...")
        print(str(place_set))

        print "Grasp placement cost: %f, N: %d" % (place_set[0][1], N)
        placement_lat = { actor: plac.runtime for actor,plac in place_set[0][0].iteritems() }
        app.actor_placement.update(placement_lat)
        print "FINAL"
        if app.batch == True:
            self.batch_update_available_resources(app)

        print app.actor_placement
        status = response.CalvinResponse(True)
        app._org_cb(status=status, placement=app.actor_placement)
        del app._org_cb
        _log.analyze(self._node.id, "+ DONE", {'app_id': app.id}, tb=True)
        _log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))

    def worst_placement_finish(self, app, actor_ids, n_samples, place_set):
        _log.debug("Ending placing actors:...")
        _log.debug(str(place_set))
        print "Worst First placement cost: %f, n_samples: %d" % (place_set[0][1], n_samples)
        placement_best = { actor: plac.runtime for actor,plac in place_set[0][0].iteritems() }
        print placement_best
        app.actor_placement.update(placement_best)
        print "FINAL"
        print app.actor_placement

        if app.batch == True:
            self.batch_update_available_resources(app)

        status = response.CalvinResponse(True)
        app._org_cb(status=status, placement=app.actor_placement)
        del app._org_cb
        _log.analyze(self._node.id, "+ DONE", {'app_id': app.id}, tb=True)
        _log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))

    def green_placement_finish(self, app, actor_ids, n_samples, place_set):
        _log.debug("Ending placing actors:...")
        _log.debug(str(place_set))
        print "Green placement cost: %f, n_samples: %d" % (place_set[0][1], n_samples)
        placement_best = { actor: plac.runtime for actor,plac in place_set[0][0].iteritems() }
        print placement_best
        app.actor_placement.update(placement_best)
        print "FINAL"
        print app.actor_placement

        # leaving here to update self.actor_by_runtime variable
        actor_placement = { actor_id: (node_id if isinstance(node_id, list) else [node_id]) for actor_id, node_id in app.actor_placement.iteritems() }
        for actor_id, node_id in actor_placement.iteritems():
            _log.debug("Actor deployment %s \t-> %s" % (app.actors[actor_id], node_id))
            self.actor_by_runtime.setdefault(node_id[0], set())
            self.actor_by_runtime[node_id[0]].add(actor_id)

        status = response.CalvinResponse(True)
        app._org_cb(status=status, placement=app.actor_placement)
        del app._org_cb
        _log.analyze(self._node.id, "+ DONE", {'app_id': app.id}, tb=True)
        _log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))

    def latency_placement(self, app, actor_ids):
        n_samples = _conf.get('global', 'deployment_n_samples')
        orphan_actors = []
        for actor_id in actor_ids:
            _log.debug("Actor id: %s, name: %s" % (actor_id, app.actors[actor_id]))
            if len(app.actor_storage[actor_id]['inports']) == 0:
                heappush(orphan_actors, (-100000, actor_id))

        _log.debug("Starting placing the actors...")
        place_set = [({}, 0.0)]
        self.latency_actor_placement(app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement=CalvinCB(self.latency_placement_finish, app, actor_ids, n_samples))

    def money_placement(self, app, actor_ids):
        n_samples = _conf.get('global', 'deployment_n_samples')
        orphan_actors = []
        for actor_id in actor_ids:
            _log.debug("Actor id: %s, name: %s" % (actor_id, app.actors[actor_id]))
            if len(app.actor_storage[actor_id]['inports']) == 0:
                heappush(orphan_actors, (-100000, actor_id))

        _log.debug("Starting placing the actors...")
        place_set = [({}, 0.0)]
        self.money_actor_placement(app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement=CalvinCB(self.money_placement_finish, app, actor_ids, n_samples))

    def grasp_placement(self, app, actor_ids, alpha):
        N = _conf.get('global', 'deployment_n_samples')
        place_set = []
        for i in range(0, N):
            placement = self.grasp_actor_placement(app, actor_ids, alpha)
            placement_temp = { actor: plac.runtime for actor,plac in placement.iteritems() }
            if len(actor_ids) > len(placement_temp):
                place_set.append((placement, 0.0))
                continue
            if _conf.get('global', 'grasp') == "v0":
                placement = self.grasp_optimization(app, actor_ids, placement_temp, False)
            elif _conf.get('global', 'grasp') == "v1":
                placement = self.grasp_optimization(app, actor_ids, placement_temp, True)
            else:
                placement = placement_temp
            # put back links to consider in cost
            placement_link = {}
            for link_id, phys_link_placements in app.link_placement.iteritems():
                src_actor = self._node.link_manager.links[link_id].src_id
                dst_actor = self._node.link_manager.links[link_id].dst_id
                if placement[src_actor] != placement[dst_actor]:
                    placement_link[dst_actor] = ActorPlacement(placement[dst_actor], link_id, "")
            for actor, runtime in placement.iteritems():
                if actor not in placement_link:
                    placement_link[actor] = ActorPlacement(runtime, "", "")

            place_set.append((placement_link, 0.0))

        self.grasp_calculate_cost_for_placement(app, actor_ids, place_set, cb_cost_calculated=CalvinCB(self.grasp_placement_finish, app, actor_ids, N))

    def green_placement(self, app, actor_ids):
        n_samples = _conf.get('global', 'deployment_n_samples')
        orphan_actors = []
        for actor_id in actor_ids:
            _log.debug("Actor id: %s, name: %s" % (actor_id, app.actors[actor_id]))
            if len(app.actor_storage[actor_id]['inports']) == 0:
                heappush(orphan_actors, (-100000, actor_id))

        _log.debug("Starting placing the actors...")
        place_set = [({}, 0.0)]
        self.green_actor_placement(app, actor_ids, n_samples, orphan_actors, place_set, cb_finish_placement=CalvinCB(self.green_placement_finish, app, actor_ids, n_samples))


    def decide_placement_filter_raw_param_farseeing(self, app):
        farseeing_cpu_used = {}
        farseeing_ram_used = {}

        # update resources
        for app_id in self.farseeing_active_apps:
            # do not count this application
            if app_id == app.id:
                continue
            app_temp = self._node.app_manager.applications[app_id]
            for actor_id in app_temp.get_actors():
                if actor_id not in self.farseeing_placement:
                    continue
                runtime = self.farseeing_placement[actor_id]
                cpu = farseeing_cpu_used.setdefault(runtime, 0)
                ram = farseeing_ram_used.setdefault(runtime, 0)
                farseeing_cpu_used[runtime] = cpu + app_temp.cpu_raw[actor_id]
                farseeing_ram_used[runtime] = ram + app_temp.ram_raw[actor_id]

        _log.info("Farseeing filter: app id: %s, CPU consumed: %s, RAM: consumed: %s, active apps: %s, placement: %s" % (app.id, str(farseeing_cpu_used), str(farseeing_ram_used), str(self.farseeing_active_apps), str(self.farseeing_placement)))

        # verify available CPU and RAM in nodes
        for actor_id, nodes_ids in app.actor_placement.iteritems():
            self.update_cache_cost_actor(app, actor_id)
            nodes_to_remove = [ node_id for node_id in nodes_ids if (app.runtime_cpu_total[node_id] - farseeing_cpu_used.setdefault(node_id, 0) < app.cost_runtime_cpu.setdefault(actor_id, 0)) or (app.runtime_ram_total[node_id] - farseeing_ram_used.setdefault(node_id, 0) < app.cost_runtime_ram.setdefault(actor_id, 0)) ]

            _log.info("Placement actor: %s. Internal state: runtimes considered: %s, runtimes removed: %s, CPU used: %s, RAM used %s, CPU total: %s, RAM total: %s", actor_id, str(nodes_ids), str(nodes_to_remove), str(farseeing_cpu_used), str(farseeing_ram_used), str(app.runtime_cpu_total), str(app.runtime_ram_total))
            nodes_ids -= set(nodes_to_remove)

        # update app.runtime_cpu app.runtime_ram used in is_resource_usage_in_placement_ok
        for runtime,value in farseeing_cpu_used.iteritems():
            app.runtime_cpu[runtime] = app.runtime_cpu_total[runtime] - farseeing_cpu_used[runtime]
            app.runtime_ram[runtime] = app.runtime_ram_total[runtime] - farseeing_ram_used[runtime]


    def decide_placement_filter_raw_param(self, app):
        # verify available CPU and RAM in nodes
        for actor_id, nodes_ids in app.actor_placement.iteritems():
            self.update_cache_cost_actor(app, actor_id)
            nodes_to_remove = [ node_id for node_id in nodes_ids if (app.runtime_cpu[node_id] < app.cost_runtime_cpu.setdefault(actor_id, 0)) or (app.runtime_ram[node_id] < app.cost_runtime_ram.setdefault(actor_id, 0)) ]

            nodes_tolerance = []
            tolerance = 1.1
            max_tolerance = _conf.get("global", "deployment_tolerance")
            while (tolerance <= max_tolerance + 1e-09):
                nodes = [ node_id for node_id in nodes_ids if (app.runtime_cpu[node_id]*tolerance >= app.cost_runtime_cpu.setdefault(actor_id, 0)) and (app.runtime_ram[node_id]*tolerance >= app.cost_runtime_ram.setdefault(actor_id, 0)) ]
                nodes_tolerance.append(nodes)
                tolerance += 0.1

            _log.info("Placement actor: %s. Internal state: runtimes considered: %s, runtimes removed: %s, CPU: %s, RAM %s, CPU total: %s, RAM total: %s", actor_id, str(nodes_ids), str(nodes_to_remove), str(app.runtime_cpu), str(app.runtime_ram), str(app.runtime_cpu_total), str(app.runtime_ram_total))
            nodes_ids -= set(nodes_to_remove)

            tolerance = 1.1
            for nodes_range in nodes_tolerance:
                if (len(nodes_ids) > 0):
                    break
                _log.info("Placement actor: %s. Nodes list empty: %s, adding nodes in %f range: %s" % (actor_id, str(nodes_ids), tolerance, str(nodes_range)))
                nodes_ids |= set(nodes_range)
                tolerance += 0.1


    def decide_placement(self, app):
        # this method can be called more than once depending on collect_* callbacks execution order (inlined calls)
        # So, we added this verification here
        if app.placement_done:
            return
        app.placement_done = True

        _log.info("Deployment: app: %s: finished filter phase: elapsed time %d" % (app.id, time.time() - app.start_time))
        _log.analyze(self._node.id, "+ BEGIN", {}, tb=True)

        actor_ids = app.get_actors()

        # Get list of all possible nodes
        node_ids = set([])
        for possible_nodes in app.actor_placement.values():
            node_ids |= possible_nodes
        node_ids = list(node_ids)
        node_ids = [n for n in node_ids if not isinstance(n, dynops.InfiniteElement)]
        for actor_id, possible_nodes in app.actor_placement.iteritems():
            if any([isinstance(n, dynops.InfiniteElement) for n in possible_nodes]):
                app.actor_placement[actor_id] = node_ids

        # initialize learn
        self.ew_learning_init(app)
        # farseeing...
        if app.migration and self.reconfig.algo == "app_farseeing":
            self.decide_placement_filter_raw_param_farseeing(app)
            okay = True
            for actor_id in actor_ids:
                if app.actor_storage[actor_id]['type'] == "std.DynamicTrigger":
                    continue
                if (app.actor_storage[actor_id]['node_id'] not in app.actor_placement[actor_id]):
                    okay = False
                    _log.info("Farseeing: placement not okay for actor: %s, current runtime: %s available: %s" % (actor_id, app.actor_storage[actor_id]['node_id'], str(app.actor_placement[actor_id])))
                    break
            if okay:
                _log.info('Farseeing: current placement is okay for application')
                return
        else:
            self.decide_placement_filter_raw_param(app)

        # Weight the actors possible placement with their connectivity matrix
        if _conf.get('global', 'deployment_algorithm') == 'random':
            placement_best = self.random_placement(app, actor_ids)
        elif _conf.get('global', 'deployment_algorithm') == 'latency':
            placement_best = self.latency_placement(app, actor_ids)
        elif _conf.get('global', 'deployment_algorithm') == 'green':
            placement_best = self.green_placement(app, actor_ids)
        elif _conf.get('global', 'deployment_algorithm') == 'worst':
            placement_best = self.worst_placement(app, actor_ids)
        elif _conf.get('global', 'deployment_algorithm') == 'best':
            placement_best = self.best_first_placement(app, actor_ids)
        elif _conf.get('global', 'deployment_algorithm') == 'money':
            placement_best = self.money_placement(app, actor_ids)
        elif _conf.get('global', 'deployment_algorithm') == 'grasp':
            placement_best = self.grasp_placement(app, actor_ids, alpha=0.1)
        else:
            placement_best = self.money_placement(app, actor_ids)

        #_log.info("Deployment: app: %s: finished placement: total elapsed time %d" % (app.id, time.time() - app.start_time))

    def grasp_load_balance_cost(self, app, runtime):
        cost = (float(app.runtime_cpu[runtime])/float(app.runtime_cpu_total[runtime]))
        cost += (float(app.runtime_ram[runtime])/float(app.runtime_ram_total[runtime]))
        return cost

    def grasp_get_actor_neighbors(self, app, actor_id):
        actors = []
        for i in app.actor_storage[actor_id]['outports'] + app.actor_storage[actor_id]['inports']:
            for p in app.port_storage[i['id']]['peers']:
                neigh_id = ""
                try:
                    neigh_id = app.port_storage[p[1]]['actor_id']
                    actors.append(neigh_id)
                except:
                    _log.debug("Didn't find neighbor actor")
                    continue
        return actors


    def grasp_runtime_accept_actor(self, app, actor_id, runtime, placement):
        neighbors = self.grasp_get_actor_neighbors(app, actor_id)
        _log.debug("GRASP, actor id: %s, neighbors: %s" % (actor_id, neighbors))
        # verify resource usage
        total_cpu = app.cost_runtime_cpu[actor_id]
        total_ram = app.cost_runtime_ram[actor_id]
        for actor, node in app.actor_placement.iteritems():
            if actor != actor_id and node == runtime:
                total_cpu += app.cost_runtime_cpu[actor]
                total_ram += app.cost_runtime_ram[actor]

        max_tolerance = _conf.get("global", "deployment_tolerance")
        if (total_cpu > app.runtime_cpu.setdefault(runtime, 0)*max_tolerance or
                    total_ram > app.runtime_ram.setdefault(runtime, 0)*max_tolerance):
            return False

        # verify links
        for link_id, phys_link_placements in app.link_placement.iteritems():
            if any([isinstance(n, dynops.InfiniteElement) for n in phys_link_placements]):
                _log.debug("Link can be any physical link")
                continue

            must_verify = False
            src_actor = self._node.link_manager.links[link_id].src_id
            dst_actor = self._node.link_manager.links[link_id].dst_id

            if src_actor == actor_id and dst_actor in neighbors:
                must_verify = True
            elif src_actor in neighbors and dst_actor == actor_id:
                must_verify = True
                dst_actor = src_actor
                src_actor = actor_id

            if not must_verify:
                continue

            if runtime == placement[dst_actor]:
                continue

            found = False
            for phy_link_id in phys_link_placements:
                rt1 = app.phys_link_placement_runtimes[phy_link_id]['runtime1']
                rt2 = app.phys_link_placement_runtimes[phy_link_id]['runtime2']
                if (rt1 == runtime and rt2 == placement[dst_actor]) or (rt1 == placement[dst_actor] and rt2 == runtime):
                    found = True
                    break
            if not found:
                return False
        return True

    def grasp_actor_rcl(self, app, actor_id, placement, rcl):
        best_runtime = ""
        if actor_id in placement:
            best_runtime = placement[actor_id]
        for r in rcl:
            if not self.grasp_runtime_accept_actor(app, actor_id, r, placement):
                _log.debug("GRASP, actor id: %s runtime: %s, CANNOT host it" % (actor_id, r))
                continue
            if best_runtime == "":
                best_runtime = r

            if _conf.get('global', 'deployment_algorithm') == 'grasp':
                if self.cost_for_runtime_v2(app, actor_id, r) < self.cost_for_runtime_v2(app, actor_id, best_runtime):
                    best_runtime = r
                elif (self.grasp_load_balance_cost(app, r) > self.grasp_load_balance_cost(app, best_runtime)) and (self.cost_for_runtime_v2(app, actor_id, r) <= self.cost_for_runtime_v2(app, actor_id, best_runtime)):
                    best_runtime = r
            else: # keep old behavior for other modes
                if self.grasp_load_balance_cost(app, r) > self.grasp_load_balance_cost(app, best_runtime):
                    best_runtime = r
        return best_runtime


    def grasp_actor_optimization(self, app, actor_id, placement, update_resources, alpha):
        best_money = -1
        for i in app.actor_placement[actor_id]:
            cost = self.cost_for_runtime_v2(app, actor_id, i)
            if best_money == -1 or cost < best_money:
                best_money = cost
        rcl = [ i for i in app.actor_placement[actor_id] if self.cost_for_runtime_v2(app, actor_id, i) <= best_money + alpha*best_money ]
        rcl = sorted(rcl, key=lambda k : self.cost_for_runtime_v2(app, actor_id, k))
        _log.debug("RCL, actor: %s, list: %s" % (actor_id, str(rcl)))
        runtime = self.grasp_actor_rcl(app, actor_id, placement, rcl)
        if runtime != "" and placement[actor_id] != runtime:
            _log.debug("GRASP OPTIMIZATION: actor_id %s, old runtime: %s, new runtime: %s" % (actor_id, placement[actor_id], runtime))
            if update_resources:
                old_runtime = placement[actor_id]
                app.runtime_cpu[old_runtime] += app.cost_runtime_cpu[actor_id]
                app.runtime_ram[old_runtime] += app.cost_runtime_ram[actor_id]
                app.runtime_cpu[runtime] -= app.cost_runtime_cpu[actor_id]
                app.runtime_ram[runtime] -= app.cost_runtime_ram[actor_id]
            placement[actor_id] = runtime
            return True
        return False

    def grasp_actor_order(self, app, actor_ids):
        ordered_actors = []
        for actor_id in actor_ids:
            #if len(self._node.am.actors[actor_id].inports.values()) == 0:
            if len(app.actor_storage[actor_id]['inports']) == 0:
                ordered_actors.append(actor_id)

        next_actors = copy.copy(ordered_actors)
        while len(next_actors) > 0:
            actor_id = next_actors.pop(0)

            for i in app.actor_storage[actor_id]['outports']:
                for p in app.port_storage[i['id']]['peers']:
                    try:
                        neigh_id = app.port_storage[p[1]]['actor_id']
                    except:
                        _log.debug("Didn't find neighbor actor")
                        continue
                    ordered_actors.append(neigh_id)
                    next_actors.append(neigh_id)

        return ordered_actors

    def grasp_optimization(self, app, actor_ids, placement, update_resources=False, alpha=0.1):
        optimized = True
        backup_cpu = copy.copy(app.runtime_cpu)
        backup_ram = copy.copy(app.runtime_ram)

        if update_resources:
            for actor_id, runtime in placement.iteritems():
                app.runtime_cpu[runtime] -= app.cost_runtime_cpu[actor_id]
                app.runtime_ram[runtime] -= app.cost_runtime_ram[actor_id]

        K = 0
        ordered_actors = self.grasp_actor_order(app, actor_ids)
        while optimized and K < 10:
            optimized = False
            K += 1
            for actor_id in ordered_actors:
                optimized |= self.grasp_actor_optimization(app, actor_id, placement, update_resources, alpha)

        app.runtime_cpu = backup_cpu
        app.runtime_ram = backup_ram
        return placement

    def grasp_evaluate_cost_and_load(self, app, placement):
        cost = 0.0
        runtimes_cpu_usage = {}
        runtimes_ram_usage = {}
        for actor_id, node_id in placement.iteritems():
            runtimes_cpu_usage[node_id] = runtimes_cpu_usage.setdefault(node_id, 0) + app.cost_runtime_cpu[actor_id]
            runtimes_ram_usage[node_id] = runtimes_ram_usage.setdefault(node_id, 0) + app.cost_runtime_ram[actor_id]
            cost += self.cost_for_runtime_v2(app, actor_id, node_id) 

        max_load = 0.0
        for runtime, avail in app.runtime_cpu.iteritems():
            #v1 load balance considering this placement
            #load_cpu = (avail - runtimes_cpu_usage.setdefault(runtime, 0))/app.runtime_cpu_total[runtime]
            #load_ram = (app.runtime_ram[runtime] - runtimes_ram_usage.setdefault(runtime, 0))/app.runtime_ram_total[runtime]
            #v0 load between applications
            load_cpu = avail/app.runtime_cpu_total[runtime]
            load_ram = app.runtime_ram[runtime]/app.runtime_ram_total[runtime]
            if max_load == 0.0 or (load_cpu + load_ram > max_load):
                max_load = load_cpu + load_ram

        return cost, max_load


    def grasp_evaluate_solution(self, app, best, plac, alpha):
        best_cost, best_max_load = self.grasp_evaluate_cost_and_load(app, best)
        plac_cost, plac_max_load = self.grasp_evaluate_cost_and_load(app, plac)

        if plac_cost < best_cost:
            return True
        if (plac_cost < best_cost + best_cost*alpha) and plac_max_load < best_max_load:
            return True
        return False


    def grasp_optimization_v2(self, app, actor_ids, placement, update_resources=False, alpha=0.1):
        best = None
        optimized_opts = []
        for plac in placement:
            plac_opt = self.grasp_optimization(app, actor_ids, plac, update_resources, alpha)
            optimized_opts.append(plac_opt)
            if best == None or self.grasp_evaluate_solution(app, best, plac_opt, alpha):
                best = plac_opt

        if not app.migration:
            return best

        if self.reconfig.is_greedy():
            #migration exploit
            if (random.random() > _conf.get('global', 'deployment_epsilon_greedy')):
                _log.info("Application: %s, migration exploit: %s" % (app.id, str(best)))
            else:
                #migration explore
                best = random.choice(optimized_opts)
                _log.info("Application: %s, migration explore: %s, options: %s" % (app.id, str(best), str(optimized_opts)))

        # request update resource for each node
        if self.reconfig.is_lazy():
            for node_id in set(best.values()):
                if node_id == self._node.id:
                    cpu = self._node.docker.get_cpu_usage()
                    ram = self._node.docker.get_ram_usage()
                    if (cpu != -1):
                        self._node.cpu_monitor.set_avail(100 - cpu)
                    if (ram != -1):
                        self._node.mem_monitor.set_avail(100 - ram)
                else:
                    try:
                        #workaround for local tests
                        #session.trust_env = False
                        import re
                        ip_addr = re.match("(http://[a-zA-Z0-9\.]*):[0-9]*", app.control_uri[node_id]).group(1)
                        app.futures[node_id] = session.get(ip_addr + ':6000/node/resource')
                    except:
                        _log.warning("Error getting resource utilization: %s" % (app.control_uri[node_id]))
                        continue
            async.DelayedCall(1, self.grasp_v2_update_resources_check, app, best)

        return best


    def grasp_v2_update_resources_check(self, app, best):
        for node_id,future in app.futures.iteritems():
            if not future.done():
                continue
            data = None
            try:
                data = future.result().json()
            except:
                continue

            if (data['cpu'] == -1 or data['ram'] == -1):
                continue

            max_tolerance = _conf.get("global", "deployment_tolerance")
            cpu = app.runtime_cpu_total[node_id]*(100-data["cpu"])/100
            ram = app.runtime_ram_total[node_id]*(100-data["ram"])/100
            _log.info("Resources after deployment: node: %s, CPU before: %d, CPU after %d, RAM before: %d RAM after: %d" % (node_id, app.runtime_cpu[node_id], cpu, app.runtime_ram[node_id], ram))

            for actor_id, node_id_sol in best.iteritems():
                if node_id_sol != node_id:
                    continue
                if (app.cost_runtime_cpu[actor_id] > max_tolerance*cpu):
                    _log.warning("Insufficient CPU for actor: %s, node: %s, after resource update: requested: %d, available: %d" % (actor_id, node_id, app.cost_runtime_cpu[actor_id], cpu))
                if (app.cost_runtime_ram[actor_id] > max_tolerance*ram):
                    _log.warning("Insufficient RAM for actor: %s, node: %s, after resource update: requested: %d, available: %d" % (actor_id, node_id, app.cost_runtime_ram[actor_id], ram))

        app.futures = {x:y for x,y in app.futures.iteritems() if not future.done()}

        if len(app.futures) > 0:
            async.DelayedCall(1, self.grasp_v2_update_resources_check, app, best)

    def ew_learning_init(self, app):
        if app.migration or not self.reconfig.is_learn():
            return

        burn_id = None
        sink_id = None
        for actor_id, actor in app.actor_storage.iteritems():
            if actor['type'] == "std.SmartBurn":
                burn_id = actor_id
            if actor['type'] == "test.Sink":
                sink_id = actor_id

        self.update_cache_cost_actor(app, burn_id)
        # FIXME: maybe filter memory too
        self._node.am.actors[sink_id]._learn.set_burn(burn_id, app.cost_runtime_cpu.get(burn_id, 0), [r for r in app.actor_placement[burn_id] if (app.runtime_cpu_total[r] >= app.cost_runtime_cpu.get(burn_id, 0) and (app.runtime_cpu_total[r] > 50))], app.runtime_cpu_total)

class FarseeingApp():
    def __init__(self, app_id, actor_id, state_info, trigger_timestamps):
        self.app_id = app_id
        self.actor_id = actor_id
        self.state_info = state_info
        self.trigger_timestamps = trigger_timestamps
        self.initial_date = time.time()

    def __str__(self):
        s = 'app_id: %s actor_id: %s state_info: %s timestamps: %s initial_date: %d' % (self.app_id, self.actor_id, self.state_info, self.trigger_timestamps, self.initial_date)
        return s


class Farseeing():
    def __init__(self, node, oracle_time=10):
        self.node = node
        self.apps = {}
        self.events = []
        self.next_schedule = None
        self.next_schedule_date = None
        self.oracle = oracle_time

    def __str__(self):
        s = 'Apps: \n'
        for app_id, app in self.apps.iteritems():
            s += '\t Id: %s App: %s\n' % (app_id, str(app))

        return s


    def add_app(self, app_id, app):
        self.apps[app_id] = app

        for ev in app.trigger_timestamps:
            date = ev[0] + app.initial_date
            state = ev[1]
            # just set initial state for first event
            if ev[0] == 0:
                self.node.app_manager.app_deployer.farseeing_set_app_state(app_id, app.state_info[state][0] > 0)
                continue
            date -= self.oracle
            heapq.heappush(self.events, (date, state, app))

        _log.info("Farseeing, queue size: %d" % len(self.events))
        first = self.events[0][0]
        if (self.next_schedule == None or first < self.next_schedule_date):
            if self.next_schedule != None:
                self.next_schedule.cancel()
            self.next_schedule = async.DelayedCall(max(0, first - time.time()), self.app_change_state)
            self.next_schedule_date = first

    def app_change_state(self):
        current = time.time()
        ev = heapq.heappop(self.events)
        date = ev[0]
        state = ev[1]
        app = ev[2]
        _log.info("Farseeing, app: %s will change to state: %s, event date: %f current date %d" % (app.app_id, state, date, current))

        if app.state_info[state][0] > 0:
            self.node.app_manager.app_deployer.farseeing_set_app_state(app.app_id, True)
            self.node.app_manager.migrate_with_requirements(app.app_id, None, move=True, extend=True, cb=None)
        else:
            self.node.app_manager.app_deployer.farseeing_set_app_state(app.app_id, False)
        _log.info("Farseeing, queue size: %d" % len(self.events))

        try:
            next_event_date = self.events[0][0]
            next_sched = next_event_date - time.time()
            if next_sched < 0:
                _log.warning("Farseeing, next event missed by %f" % next_sched)
                next_sched = 0
            self.next_schedule = async.DelayedCall(next_sched, self.app_change_state)
            self.next_schedule_date = next_event_date
        except:
            _log.warning("Farseeing, no more events in the queue")
