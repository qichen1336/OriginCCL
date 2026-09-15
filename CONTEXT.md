# OriginCCL

A collective-communication library: several processes cooperate on one logical operation by
moving their buffers along a ring, with a bootstrap phase that exchanges endpoint information
and a data phase that moves the payload.

## Language

**Rank**:
One process in a communicator; exactly one rank per process.
_Avoid_: Process, worker, node

**Channel**:
An independent data path between two neighbouring ranks in the ring. A collective is split
across the channels so they progress in parallel.
_Avoid_: Stream, lane

**Edge**:
One direction of one channel between two ranks. A channel has two edges between each pair of
neighbours, and each edge is established as its own connection.
_Avoid_: Link, connection

**Data transport**:
The connection that carries a collective's payload for one edge.
_Avoid_: Data plane, wire

**Rendezvous**:
The initialization-only exchange that establishes an edge, including any kernel handles the
two ranks have to share. It carries no payload and is retired once the edge is up.
_Avoid_: Handshake, setup channel

**Machine identity**:
The value two ranks compare to decide whether they run on the same host. It is the hostname,
not an address.
_Avoid_: Host, node, IP

**Local edge**:
An edge whose two ranks have the same machine identity, so it can be carried by a
shared-memory transport instead of a socket.
_Avoid_: Intra-node link, shm edge

**Remote edge**:
An edge whose two ranks have different machine identities, so it is carried by TCP.
_Avoid_: Inter-node link, network edge

**Readiness**:
The condition an executor waits for before it advances one edge: a transport reports, per
direction, which handle to wait on and whether it needs that handle to become readable or
writable.
_Avoid_: Event, interrupt

**Executor**:
The component that decides how to wait for readiness and drives the algorithm's state machine
forward. One strategy is compiled into the library at a time.
_Avoid_: Scheduler, event loop
