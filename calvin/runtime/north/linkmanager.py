# -*- coding: utf-8 -*-

from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinuuid

_log = get_logger(__name__)


class Link(object):

    def __init__(self, name, actor_src_id, actor_dst_id, requirements):
        self.name = name
        self.src_id = actor_src_id
        self.dst_id = actor_dst_id
        self.requirements = requirements

    def requirements_get(self):
        return self.requirements

class LinkManager(object):

    """Manages the entity representing application links.
    A link represents the connection between 2 ports of actors.
    It DOES NOT represent the real connection between runtimes """

    def __init__(self, node):
        self.links = {}
        self.node = node

    def new(self, name, actor_src_id, actor_dst_id, requirements):
        """
        Instantiates a new link structure.
        actor_src_id: Source connection point of link
        actor_dst_id: Destination connection point of link
        requirements: list of requirements to link
        Returns the link identifier
        """

        id = calvinuuid.uuid("LINK")

        _log.debug("link: id %s actor_src: %s actor_dst: %s" % (id, actor_src_id, actor_dst_id))
        self.links[id] = Link(name, actor_src_id, actor_dst_id, requirements)

        # TODO [donassolo] must a link be in storage?
        # self.node.storage.add_actor(a, self.node.id)

        return id
