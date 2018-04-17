= Network Application Deployment Requirements =

The objective is allow users to set network related requirements for their applications. 
Initially, users will be able to set bandwidth and latency between actors. These requirements will guide the selection of possible runtimes to place the actors.

== Application Deployment ==

=== Application description ==

A Calvin application is described by its .calvin script which contains its actors and connections.

A new label was added to identify links in the application. A link is simply the connection between 2 actor ports. Example:

---
src : std.Trigger(tick=1, data="fire")
snk : io.Log(loglevel="INFO")

src.data > snk.data
---

The *src.data > snk.data* identifies a link between the *src.data* port and *snk.data* port. Now, it is possible to give a name to it, using this syntax:

---
src : std.Trigger(tick=1, data="fire")
snk : io.Log(loglevel="INFO")

link myLink : src.data > snk.data
---

The work *link* in the beginning of the line is reserved. The string *myLink* identifies this connection.

=== Deployment description ===

Now that is is possible to identify the links, let's see how to set the requirements for them.

Example:
---
{"requirements":{
                 "myLink":[{"op":"link_attr_match","kwargs":{"index":{"bandwidth":"100M"}},"type":"+"}]
                }
}
---

It is very similar to other requirements in Calvin. The difference is the new operation and the new indexes for the network requirements.

New operations:
- *link_attr_match*: very similar to *node_attr_match*, it will match the links that satisfies the requirements described.

New indexes:
- *bandwidth*: Matches links that have at least the requested bandwidth. Acceptable values for bandwidth: {'100K', '1M', '10M', '100M', '1G'}
- *latency*: Matches links that have latency equal or lower to the requested. Acceptable values for latency: { '1s', '100ms', '1ms', '100us', '1us'}

== Calvin attributes ==

In order to filter the correct runtimes to run the actors, it is necessary to have the network metrics updated in Calvin. Following the same principle used for CPU and RAM, it is an external tool that is responsible to keep this data updated through the control API.

Methods added to the control API:
- */node/resource/bandwidth/runtime1/runtime2*: set the bandwidth between 2 runtimes
- */node/resource/latency/runtime1/runtime2*: set the latency between 2 runtimes

Both methods accept the same parameters:
- runtime1/runtime2: link connects the 2 runtimes
- value: new value to be set for the metric. It must be one of the acceptable values cited above.

== Internal stuff ==

We describe here some of the internal changes needed to implement the support to network requirements.

=== Creating "physical links" ===

Calvin contains the concept of connections internally. A connection is created to communicate the ports of 2 actors. Although similar, we need another entity to represent our possible links between runtimes. This "physical links" exist independently of actors are using them or not.

Each pair of runtimes has a "physical link" connecting them. They are created at LinkMonitor class. The algorithm to create them is quite simple. When the runtimes starts, it looks at the storage for all runtimes registered and then create the link. A set of data is saved in storage when a link is created:
- phyLink-*linkId*: Indexed by the UUID of the link, saves the 2 runtimes that this links interconnects.
- /physLinks/*runtimeId*: saves the set of link IDs of a runtime. Used to erase them when the node disappears.
- rt-link-*rt1rt2* and rt-link-*rt2rt1*: saves the link that connects the runtime1 to the runtime2 and vice versa. Used to update the bandwidth and latency of a link.

=== Filtering placements ===

With all the structure in place, it is possible to use it to decide the placement considering also the network requirements.

To do so, we adapted the deployment algorithm described in *execute_requirements* and *collect_placements* methods in *calvin/runtime/north/appmanager*. The high level steps of the new algorithm are:

1. Collect phase:
 - Collect actors placements: Same as before, it gets all the possible runtimes that are able to run the actors. Returns a set of UUID of the runtimes
 - Collect links placements: Similar to the actors, gets all the possible links that satisfies the users requirements. Returns a set of UUID of the links.
 - Collect links information: To the next step of the deployment algorithm. For each link ID, it is necessary to know which runtimes they interconnect.

2. Decide placement phase:
 - Decide actor placement: Same algorithm as before, returns a set of possible runtimes for each actor.
 - Filter actor placement considering the links: Leaves only the runtimes that have runtimes respecting the link requirements. This is done at the *filter_link_placement* method.

==== The *filter_link_placement* method ====

The actor placement step gives us the set of possible runtimes to host the actors. If we ignore the link requirements, it is possible to use any of them as final runtime. However, this new set of network requirements create a dependence among actors. Now, the placement of one actor may affect the other, what wasn't the case before and leads to problems in the migration (more details in the problem section below).

To cope with this dependence, it was created the *filter_link_placement* method. It gets each pair of actors and verify the link possibilities for them. We have 3 cases:

1. No link requirements: actors can run in any runtimes, nothing to do in this case.
2. Empty links: no physical link satisfies the requirements, but we can still put both actors in the same runtime.
3. Set of links: list of links that satisfy the requirements. Select the first link whose runtimes can host the 2 actors, that is, the runtimes are in the list got in the "actor placement" step.

As we have this dependency among the actors' placement, only 1 runtime per actor is selected in steps 2 and 3.
In the old version it was okay to return a set of possible runtimes as actors were independent. But now, the actors placement cannot change freely.

== Problems ==

=== Migration ===

Another open point in the implementation is the migration mechanism. Nowadays, it is possible to change/add the requirements of a single actor. As the actors deployment were independent, it was okay to simply retrigger the deployment algorithm for this actor.

However, in the new implementation this independence is broken. A change in an actor placement may trigger changes in other actors. It seems necessary to retrigger the deployment of the application in which the actor is part of, not only the actor. However, how to do it with actors distributed in many nodes is still an open question.

