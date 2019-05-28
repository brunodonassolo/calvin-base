# -*- coding: utf-8 -*-

# Copyright (c) 2015 Ericsson AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import collections
import os
import copy
from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import dynops
from calvin.utilities import calvinlogger
from calvin.runtime.north.plugins.requirements import req_operations
import calvin.requests.calvinresponse as response
from calvin.utilities import calvinuuid
from calvin.actorstore.store import ActorStore, GlobalStore
from calvin.runtime.south.async import async
from calvin.utilities.security import Security
from calvin.utilities.requirement_matching import ReqMatch
from heapq import heappush, heapify, heappop
from calvin.utilities import calvinconfig
from calvin.runtime.north.appdeployer import AppDeployer,Application

_log = calvinlogger.get_logger(__name__)

class AppManager(object):

    """ Manage deployed applications """

    def __init__(self, node):
        self._node = node
        self.storage = node.storage
        self.applications = {}
        self.app_deployer = AppDeployer(node, node.storage)
        self.actor_by_runtime = {}
        self.phys_link_placement_runtimes = {}  # This information is quite stable, save it here to avoid collecting it each app deployment. Clean placement slate, saves the runtimes that the physical link connects

    def new(self, name, deploy_info=None):
        application_id = calvinuuid.uuid("APP")
        self.applications[application_id] = Application(application_id, name, self._node.id, self._node.am, deploy_info=deploy_info)
        self._node.control.log_application_new(application_id, name)
        return application_id

    def add(self, application_id, actor_id):
        """ Add an actor """
        if application_id in self.applications:
            self.applications[application_id].add_actor(actor_id)
        else:
            _log.error("Non existing application id (%s) specified" % application_id)
            return

    def add_link(self, application_id, link_id):
        """ Proxy to add a link  to an application
            application_id: Identifier, must exist in the manager
            link_id: Link identifier
        """
        if application_id in self.applications:
            self.applications[application_id].add_link(link_id)
        else:
            _log.error("Trying to add link(%s) but a non existing application id (%s) specified" % (link_id, application_id))
            return

    def finalize(self, application_id, migrate=False, cb=None):
        _log.analyze(self._node.id, "+", {'application_id': application_id, 'migrate': migrate, 'cb': str(cb)})
        if application_id not in self.applications:
            _log.error("Non existing application id (%s) specified" % application_id)
            return
        self.storage.add_application(self.applications[application_id])
        if migrate:
            app = self.applications[application_id]
            self.app_deployer.execute_requirements(app, CalvinCB(self._finalize_got_placement, app=app, cb=cb), move=False, migration=False)
        elif cb:
            cb(status=response.CalvinResponse(True))


    def _finalize_got_placement(self, app, status, placement, cb):

        actor_ids = app.get_actors()
        if len(actor_ids) > len(placement.keys()):
            print "It was impossible to place all actors(total: %d, placed: %d), aborting..." % (len(actor_ids), len(placement.keys()))
            status = response.CalvinResponse(False, data='Impossible to place all actors')
            if cb:
                cb(status=status)
            self._destroy(app, None)
            return

        actor_placement = { actor_id: (node_id if isinstance(node_id, list) else [node_id]) for actor_id, node_id in placement.iteritems() }
        for actor_id, node_id in actor_placement.iteritems():
            _log.debug("Actor deployment %s \t-> %s" % (app.actors[actor_id], node_id))
            self._node.am.robust_migrate(actor_id, node_id[:], None)
        if cb:
            cb(status=status, placement=actor_placement)

    def destroy(self, application_id, cb):
        """ Destroy an application and its actors """
        _log.analyze(self._node.id, "+", {'application_id': application_id})
        if application_id in self.applications:
            self._destroy(self.applications[application_id], cb=cb)
        else:
            self.storage.get_application(application_id, CalvinCB(self._destroy_app_info_cb, cb=cb))

    def _destroy_app_info_cb(self, key, value, cb):
        application_id = key
        _log.analyze(self._node.id, "+", {'application_id': application_id, 'value': value})
        _log.debug("Destroy app info %s: %s" % (application_id, value))
        if response.isnotfailresponse(value):
            self._destroy(Application(application_id, value['name'], value['origin_node_id'],
                                      self._node.am, value['actors_name_map'], links=value['links_set']), cb=cb)
        elif cb:
            cb(status=response.CalvinResponse(response.NOT_FOUND))

    def _destroy(self, application, cb):
        _log.analyze(self._node.id, "+", {'actors': application.actors})
        application.destroy_cb = cb
        try:
            del application._destroy_node_ids
        except:
            pass
        application.clear_node_info()
        application.actor_replicas = []
        application.replication_ids = []
        application._replicas_actor_final = {}
        application._replicas_node_final = {}
        for link_id in application.links:
            self.storage.delete_link(link_id)
        for actor_id in application.actors.keys():
            if actor_id in self._node.am.list_actors():
                application.update_node_info(self._node.id, actor_id)
                replication_id = self._node.am.actors[actor_id]._replication_id.id
                _log.analyze(self._node.id, "+ LOCAL ACTOR", {'actor_id': actor_id, 'replication_id': replication_id})
                if replication_id is not None:
                    # Destroy the replication manager
                    self._node.rm.destroy_replication_leader(replication_id)
                    # Destroy the replicas
                    application.replication_ids.append(replication_id)
                    self._node.storage.get_replica(
                        replication_id,
                        cb=CalvinCB(func=self._replicas_cb, replication_id=replication_id,
                                    master_id=self._node.am.actors[actor_id]._replication_id.original_actor_id,
                                    application=application))
            else:
                _log.analyze(self._node.id, "+ REMOTE ACTOR", {'actor_id': actor_id})
                self.storage.get_actor(actor_id, CalvinCB(func=self._destroy_actor_cb, application=application))

        if application.complete_node_info() and not application.replication_ids:
            # All actors were local and no replicas
            _log.analyze(self._node.id, "+ DONE", {'actors': application.actors})
            self._destroy_final(application)

    def _destroy_actor_cb(self, key, value, application, retries=0, check_replica=True):
        """ Get actor callback """
        _log.analyze(self._node.id, "+", {'actor_id': key, 'value': value, 'retries': retries,
                                        'check_replica': check_replica})
        if response.isnotfailresponse(value) and 'node_id' in value:
            application.update_node_info(value['node_id'], key)
            try:
                # When this is the callback for finding a replica we need to remove it as well
                application.actor_replicas.remove(key)
            except:
                pass
            if 'replication_id' in value and value['replication_id'] is not None and not check_replica:
                application._replicas_node_final.setdefault(value['replication_id'], set()).add(value['node_id'])
            if 'replication_id' in value and value['replication_id'] is not None and check_replica:
                # Destroy the replication manager
                self._node.rm.destroy_replication_leader(value['replication_id'])
                # Destroy the replicas
                application.replication_ids.append(value['replication_id'])
                self.storage.get_replica(value['replication_id'],
                    cb=CalvinCB(func=self._replicas_cb, replication_id=value['replication_id'], master_id=key,
                                application=application))
        else:
            if retries<10:
                # FIXME add backoff time
                _log.analyze(self._node.id, "+ RETRY", {'actor_id': key, 'value': value, 'retries': retries})
                self.storage.get_actor(key, CalvinCB(func=self._destroy_actor_cb, application=application, retries=(retries+1), check_replica=check_replica))
            else:
                # FIXME report failure
                _log.analyze(self._node.id, "+ GIVE UP", {'actor_id': key, 'value': value, 'retries': retries})
                application.update_node_info(None, key)

        if application.complete_node_info() and not application.replication_ids and not application.actor_replicas:
            _log.debug("_destroy_actor_cb final")
            self._destroy_final(application)

    def _replicas_cb(self, value, replication_id, master_id, application):
        _log.analyze(self._node.id, "+", {'value': value, 'replication_id': replication_id, 'replication_ids': application.replication_ids})
        application.replication_ids.remove(replication_id)
        application.actor_replicas.extend(value)
        application._replicas_actor_final.setdefault(replication_id, []).extend(value)
        for actor_id in value:
            if actor_id == master_id:
                application.actor_replicas.remove(actor_id)
                continue
            application.actors[actor_id] = "noname"
            if actor_id in self._node.am.list_actors():
                _log.debug("_replicas_cb actor %s LOCAL" % actor_id)
                application.actor_replicas.remove(actor_id)
                application.update_node_info(self._node.id, actor_id)
            else:
                _log.debug("_replicas_cb actor %s REMOTE" % actor_id)
                self.storage.get_actor(actor_id,
                    CalvinCB(func=self._destroy_actor_cb, application=application, check_replica=False))
        if application.complete_node_info() and not application.replication_ids:
            _log.debug("_replicas_cb final")
            self._destroy_final(application)

    def _destroy_final(self, application):
        """ Final destruction of the application on this node and send request to peers to also destroy the app """
        _log.analyze(self._node.id, "+ BEGIN 1", {'node_info': application.node_info, 'origin_node_id': application.origin_node_id})
        if hasattr(application, '_destroy_node_ids'):
            # Already called
            return
        _log.analyze(self._node.id, "+ BEGIN 2", {'node_info': application.node_info, 'origin_node_id': application.origin_node_id})
        application._destroy_node_ids = {n: None for n in application.node_info.keys()}
        for node_id, actor_ids in application.node_info.iteritems():
            if not node_id:
                _log.analyze(self._node.id, "+ UNKNOWN NODE", {})
                application._destroy_node_ids[None] = response.CalvinResponse(False, data=actor_ids)
                continue
            if node_id == self._node.id:
                ok = True
                for actor_id in actor_ids:
                    if actor_id in self._node.am.list_actors():
                        _log.analyze(self._node.id, "+ LOCAL ACTOR", {'actor_id': actor_id})
                        try:
                            self._node.am.destroy(actor_id)
                        except:
                            ok = False
                application._destroy_node_ids[node_id] = response.CalvinResponse(ok)
                continue
            # Inform peers to destroy their part of the application
            self._node.proto.app_destroy(node_id, CalvinCB(self._destroy_final_cb, application, node_id),
                application.id, actor_ids)

        if application.id in self.applications:
            del self.applications[application.id]
        elif application.origin_node_id not in application.node_info and application.origin_node_id != self._node.id:
            # All actors migrated from the original node, inform it also
            _log.analyze(self._node.id, "+ SEP APP NODE", {})
            self._node.proto.app_destroy(application.origin_node_id, None, application.id, [])

        self.storage.delete_application(application.id)
        self._destroy_final_cb(application, '', response.CalvinResponse(True))

    def _destroy_final_cb(self, application, node_id, status):
        _log.analyze(self._node.id, "+", {'node_id': node_id, 'status': status})
        application._destroy_node_ids[node_id] = status
        if any([s is None for s in application._destroy_node_ids.values()]):
            return
        # Done
        for replication_id, replica_ids in application._replicas_actor_final.items():
            for replica_id in replica_ids:
                self._node.storage.remove_replica(replication_id, replica_id)
        for replication_id, replica_node_ids in application._replicas_node_final.items():
            for replica_node_id in replica_node_ids:
                self._node.storage.remove_replica_node_force(replication_id, replica_node_id)
        if application.destroy_cb:
            if all(application._destroy_node_ids.values()):
                application.destroy_cb(status=response.CalvinResponse(True))
            else:
                # Missing is the actors that could not be found.
                # FIXME retry? They could have moved
                missing = []
                for status in application._destroy_node_ids.values():
                    missing += [] if status.data is None else status.data
                application.destroy_cb(status=response.CalvinResponse(False, data=missing))
        self._node.control.log_application_destroy(application.id)

    def destroy_request(self, application_id, actor_ids):
        """ Request from peer of local application parts destruction and related actors """
        _log.debug("Destroy request, app: %s, actors: %s" % (application_id, actor_ids))
        _log.analyze(self._node.id, "+", {'application_id': application_id, 'actor_ids': actor_ids})
        reply = response.CalvinResponse(True)
        missing = []
        for actor_id in actor_ids:
            if actor_id in self._node.am.list_actors():
                self._node.am.destroy(actor_id)
            else:
                reply = response.CalvinResponse(False)
                missing.append(actor_id)
        reply.data = missing
        if application_id in self.applications:
            del self.applications[application_id]
        _log.debug("Destroy request reply %s" % reply)
        _log.analyze(self._node.id, "+ RESPONSE", {'reply': str(reply)})
        return reply

    def destroy_request_with_disconnect(self, application_id, actor_ids, terminate, callback=None):
        _log.analyze(self._node.id, "+", {'application_id': application_id, 'actor_ids': actor_ids})
        missing = []
        for actor_id in actor_ids[:]:
            if actor_id in self._node.am.list_actors():
                self._node.am.destroy_with_disconnect(actor_id, terminate,
                    callback=CalvinCB(self._destroy_request_with_disconnect_cb, application_id=application_id,
                                        actor_ids=actor_ids, actor_id=actor_id, callback=callback, missing=missing))
            else:
                self._destroy_request_with_disconnect_cb(
                    application_id=application_id, actor_ids=actor_ids, callback=callback,
                    missing=missing, actor_id=actor_id, status=response.CalvinResponse(False))

    def _destroy_request_with_disconnect_cb(self, application_id, actor_ids, missing, status, actor_id, callback=None):
        actor_ids.remove(actor_id)
        if not status:
            missing.append(actor_id)
        if actor_ids:
            return
        if application_id in self.applications:
            del self.applications[application_id]
        if callback:
            if missing:
                callback(status=response.CalvinResponse(False, data={'missing': missing}))
            else:
                callback(status=response.CalvinResponse(True))

    def list_applications(self):
        """ Returns list of applications """
        return list(self.applications.keys())

    # Remigration

    def migrate_with_requirements(self, app_id, deploy_info, move=False, extend=False, cb=None):
        """ Migrate actors of app app_id based on newly supplied deploy_info.
            Optional argument move controls if actors prefers to stay when possible.
        """
        self.storage.get_application(app_id, cb=CalvinCB(self._migrate_got_app,
            app_id=app_id, deploy_info=deploy_info,
            move=move, extend=extend, cb=cb))

    def _migrate_got_app(self, key, value, app_id, deploy_info, move, extend, cb):
        if response.isfailresponse(value):
            if cb:
                cb(status=response.CalvinResponse(response.NOT_FOUND))
            return
        deploy_req = { "requirements" : {} }
        if value['deploy_info'] is not None:
            deploy_req = value['deploy_info']
        if extend:
            _log.warning("APP migration, extending requirements NOT working")
            pass
        else:
            deploy_req = deploy_info
        app = Application(app_id, value['name'], value['origin_node_id'],
                self._node.am, actors=value['actors_name_map'], deploy_info=deploy_req, links=value['links_name_map'])
        app.group_components()
        self.app_deployer.execute_requirements(app, CalvinCB(self._migrate_got_placement, app=app, cb=cb), move=move, migration=True)

    def _migrate_got_placement(self, app, status, placement, cb):
        actor_placement = { actor_id: (node_id if isinstance(node_id, list) else [node_id]) for actor_id, node_id in placement.iteritems() }
        for actor_id in set(app.actors.keys()) - set(actor_placement.keys()):
            _log.warning("Application: %s, Impossible to migrate actor: %s, no possible placement.", app.id, actor_id)
        for actor_id, node_id in actor_placement.iteritems():
            self._node.am.robust_migrate(actor_id, node_id[:], None)
        if cb:
            cb(status=status, placement=actor_placement)

class Deployer(object):

    """
    Process an app_info dictionary (output from calvin parser) to
    produce a running calvin application.
    """

    def __init__(self, deployable, node, name=None, deploy_info=None, security=None, verify=True, cb=None):
        super(Deployer, self).__init__()
        self.deployable = deployable
        self.deploy_info = deploy_info
        self.sec = security
        self.actorstore = ActorStore(security=self.sec)
        self.actor_map = {}
        self.replication_map = {}
        self.actor_connections = {}
        self.node = node
        self.cb = cb
        self._verified_actors = {}
        self._deploy_counter = 0
        self._instantiate_counter = 0
        self._requires_counter = 0
        self._connection_count = None
        if name:
            self.name = name
            self.app_id = self.node.app_manager.new(self.name, deploy_info=deploy_info)
            self.ns = os.path.splitext(os.path.basename(self.name))[0]
        elif "name" in self.deployable:
            self.name = self.deployable["name"]
            self.app_id = self.node.app_manager.new(self.name, deploy_info=deploy_info)
            self.ns = os.path.splitext(os.path.basename(self.name))[0]
        else:
            self.app_id = self.node.app_manager.new(None, deploy_info=deploy_info)
            self.name = self.app_id
            self.ns = ""
        self.group_components()
        _log.analyze(self.node.id, "+ SECURITY", {'sec': str(self.sec)})

    # TODO Make deployer use the Application class group_components, component_name and get_req
    def group_components(self):
        self.components = {}
        l = (len(self.ns)+1) if self.ns else 0
        for name in self.deployable['actors']:
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
            # print "Deployer::get_req({}) -> [] NO INFO".format(actor_name)
            return []
        # Trim of script name
        _, name = actor_name.split(':', 1)
        parts = name.split(':')
        req = []
        while parts and not req:
            current = ':'.join(parts)
            req = self.deploy_info['requirements'].get(current, [])
            parts = parts[:-1]
        # print "Deployer::get_req({}) -> ".format(actor_name), req
        return req


    def lookup_and_verify(self, actor_name, info, cb=None):
        """
        Lookup and verify actor in actor store.
          - 'actor_name' is <namespace>:<identifier>, e.g. app:src, or app:component:src
          - 'info' is information about the actor
        """
        actor_type = info['actor_type']
        try:
            actor_def, signer = self.node.am.lookup_and_verify(actor_type, self.sec)
            info['signer'] = signer
            info['requires'] = actor_def.requires if hasattr(actor_def, "requires") else []
        except Exception:
            # Not found locally, must be made shadow actor
            info = self.deployable['actors'][actor_name]
            info['shadow_actor'] = True
            actor_def = None
        self._verified_actors[actor_name] = (info, actor_def)
        if cb:
            cb()

    def check_requirements_and_sec_policy(self, actor_name, info, actor_def=None, cb=None):
        """
        Check requirements and security policy for actor.
          - 'actor_name' is <namespace>:<identifier>, e.g. app:src, or app:component:src
          - 'info' is information about the actor
          - 'actor_def' is the actor definition returned from the actor store
        """
        try:
            if not 'shadow_actor' in info:
                self.node.am.check_requirements_and_sec_policy(info['requires'],
                                                               security=self.sec,
                                                               signer=info['signer'],
                                                               callback=CalvinCB(self.instantiate,
                                                                                 actor_name, info,
                                                                                 actor_def, cb=cb))
                return
            self.instantiate(actor_name, info, cb=cb)
        except Exception:
            # Still want to create shadow actor.
            info['shadow_actor'] = True
            self.instantiate(actor_name, info, cb=cb)

    def _requirement_type(self, req):
        try:
            return req_operations[req['op']].req_type
        except:
            return "unknown"

    def instantiate(self, actor_name, info, actor_def=None, access_decision=None, cb=None):
        """
        Instantiate an actor.
          - 'actor_name' is <namespace>:<identifier>, e.g. app:src, or app:component:src
          - 'info' is information about the actor
             info['args'] is a dictionary of key-value arguments for this instance
             info['signature'] is the GlobalStore actor-signature to lookup the actor
          - 'access_decision' is a boolean indicating if access is permitted
        """
        #TODO component ns needs to be stored in registry /component/<app-id>/ns[0]/ns[1]/.../actor_name: actor_id
        try:
            if 'port_properties' in self.deployable:
                port_properties = self.deployable['port_properties'].get(actor_name, None)
            else:
                port_properties = None
            info['args']['name'] = actor_name
            # TODO add requirements should be part of actor_manager new
            actor_id = self.node.am.new(actor_type=info['actor_type'], args=info['args'], signature=info['signature'],
                                        actor_def=actor_def, security=self.sec, access_decision=access_decision,
                                        shadow_actor='shadow_actor' in info, port_properties=port_properties, app_id=self.app_id)
            if not actor_id:
                raise Exception("Could not instantiate actor %s" % actor_name)
            deploy_req = self.get_req(actor_name)
            if deploy_req:
                # Seperate replication and placement requirements
                actor_reqs = [r for r in deploy_req if self._requirement_type(r) != "replication"]
                replication_reqs = [r for r in deploy_req if self._requirement_type(r) == "replication"]
                if replication_reqs:
                    # Replication requirements (should only be one)
                    replication_result = self.node.rm.supervise_actor(actor_id, replication_reqs[0], actor_args=info['args'])
                    if replication_result:
                        self.replication_map[actor_name] = replication_result.data['replication_id']
                    else:
                        _log.error("ERROR {} when applying scaling requirements {} on actor {}".format(
                                    replication_result, replication_reqs, actor_name))
                # Placement requirements
                self.node.am.actors[actor_id].requirements_add(actor_reqs, extend=False)
                # Update requirements in registry
                self.node.storage.add_actor(self.node.am.actors[actor_id], self.node.id)
            self.store_complete_requirements(actor_id)
            self.actor_map[actor_name] = actor_id
            self.node.app_manager.add(self.app_id, actor_id)
        except Exception as e:
            _log.exception("INSTANTIATE FAILED")
            # FIXME: what should happen here?
            raise e
        finally:
            if cb:
                cb()

    def store_complete_requirements(self, actor_id):
        actor = self.node.am.actors[actor_id]
        if actor.is_shadow():
            # Find requires
            def _desc_cb(signature, description):
                _log.debug("REQUIRES BACK %s" % description)
                requires = None
                for actor_desc in description:
                    # We get list of possible descriptions back matching the signature
                    # In reality it is only one
                    if 'requires' in actor_desc:
                        requires = actor_desc['requires']
                if requires is not None:
                    actor.requires = requires
                    self.node.storage.add_actor_requirements(actor)
                self._requires_counter += 1
                if self._requires_counter >= len(self.deployable['actors']):
                    self._wait_for_all_connections_and_requires()
            try:
                GlobalStore(node=self.node).global_signature_lookup(actor._signature, cb=_desc_cb)
            except:
                _log.exception("actor instanciate GlobalStore exception")
                self._requires_counter += 1
        else:
            self._requires_counter += 1
            self.node.storage.add_actor_requirements(actor)

    def connectid(self, connection, cb):
        src_actor, src_port, dst_actor, dst_port = connection
        # connect from dst to src
        # use node info if exists, otherwise assume local node

        dst_actor_id = self.actor_map[dst_actor]
        src_actor_id = self.actor_map[src_actor]
        src_node = self.node.id
        result = self.node.connect(
            actor_id=dst_actor_id,
            port_name=dst_port,
            port_dir='in',
            peer_node_id=src_node,
            peer_actor_id=src_actor_id,
            peer_port_name=src_port,
            peer_port_dir='out',
            cb=cb)
        return result

    def deploy(self):
        """Verify actors, instantiate and link them together.
        """
        if not self.deployable['valid']:
            raise Exception("Deploy information is not valid")

        for actor_name, info in self.deployable['actors'].iteritems():
            self.lookup_and_verify(actor_name, info, cb=CalvinCB(self._deploy_instantiate))

    def _deploy_instantiate(self):
        self._deploy_counter += 1
        if self._deploy_counter < len(self.deployable['actors']):
            return
        for actor_name, info in self._verified_actors.iteritems():
            self.check_requirements_and_sec_policy(actor_name, info[0], info[1], cb=CalvinCB(self._deploy_finalize))

    def _deploy_finalize(self):
        self._instantiate_counter += 1
        if self._instantiate_counter < len(self._verified_actors):
            return
        for component_name, actor_names in self.components.iteritems():
            actor_ids = [self.actor_map[n] for n in actor_names]
            for actor_id in actor_ids:
                self.node.am.actors[actor_id].component_add(actor_ids)

        for src, dst_list in self.deployable['connections'].iteritems():
            if len(dst_list) > 1:
                src_name, src_port = src.split('.')
                _log.debug("GET PROPERTIES for %s, %s.%s" % (src, src_name, src_port))
                current_properties = self.node.pm.get_port_properties(
                                        actor_id=self.actor_map[src_name], port_dir='out', port_name=src_port)
                kwargs = {'nbr_peers': len(dst_list)}
                if 'routing' in current_properties and current_properties['routing'] != 'default':
                    kwargs['routing'] = current_properties['routing']
                else:
                    kwargs['routing'] = 'fanout'
                _log.debug("CURRENT PROPERTIES\n%s\n%s" % (current_properties, kwargs))
                self.node.pm.set_port_properties(actor_id=self.actor_map[src_name], port_dir='out', port_name=src_port,
                                                 **kwargs)

        for link_name, link_data in self.deployable['links'].iteritems():
            for l in link_data:
                try:
                    src_name, src_port = l[0].split('.')
                    dst_name, dst_port = l[1].split('.')
                    src_id = self.actor_map[src_name]
                    dst_id = self.actor_map[dst_name]
                    link_name_deploy = link_name.split(':', 1)[1] # get only link name to search in requirements list
                    deploy_req = self.deploy_info['requirements'].get(link_name_deploy, [])
                    link_id = self.node.link_manager.new(link_name, src_id, dst_id, deploy_req)
                    self.node.app_manager.add_link(self.app_id, link_id, link_name)
                    _log.debug("App Mgr: Creating link: name %s id %s. Between actor: %s and %s. Deployment rules: %s" % (link_name, link_id, src_name, dst_name, str(deploy_req)))
                except:
                    _log.error("Error creating link(%s) connecting actor (%s) to actor (%s). Actors ID not found" % (link_name, l[0], l[1]))
                    pass


        self._connection_count = sum(map(len, self.deployable['connections'].values()))
        self._connection_status = response.CalvinResponse(True)
        for src, dst_list in self.deployable['connections'].iteritems():
            src_actor, src_port = src.split('.')
            for dst in dst_list:
                dst_actor, dst_port = dst.split('.')
                c = (src_actor, src_port, dst_actor, dst_port)
                self.connectid(c, cb=self._wait_for_all_connections)

    def _wait_for_all_connections(self, status, *args, **kwargs):
        _log.debug("_wait_for_all_connections %d" % self._connection_count)
        self._connection_count -= 1
        if not status:
            # TODO handle connection errors
            _log.error("Deployer failed a port connection status: %s port info: %s" % (str(status), str(kwargs)))
            self._connection_status = status
        if self._connection_count == 0:
            _log.debug("_wait_for_all_connections final")
            # Replication manager needs to fetch port info if supervise ShadowActor
            self.node.rm.deployed_actors_connected(self.actor_map.values())
            self._wait_for_all_connections_and_requires()

    def _wait_for_all_connections_and_requires(self):
        if self._connection_count == 0 and self._requires_counter >= len(self.deployable['actors']):
            self.node.app_manager.finalize(self.app_id, migrate=True if self.deploy_info else False,
                                   cb=CalvinCB(self.cb, deployer=self))
