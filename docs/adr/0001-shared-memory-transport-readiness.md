# Transport reports readiness as a descriptor instead of an fd

Each edge is one connection, and an executor has to wait for its two directions to become
progressable. That used to be expressed as a single `fd` per transport, which only describes a
TCP socket: receive means the fd is readable, send means it is writable. A shared-memory ring
does not fit, because its send side becomes progressable when a *notification* fd is readable,
not writable, and its two directions live on different fds.

We decided that a transport reports, per direction, a small value descriptor: the fd to wait on
and whether that fd must become readable or writable. Executors translate the condition into
poll/epoll interest and never learn which transport produced it; the logic direction is implied
by which descriptor is being waited on, so an event never has to be classified. `GetFd()` was
replaced rather than kept, because a ring has no single fd that could answer for both
directions.

**Considered options**

- *Extend the existing `Transport` interface with a descriptor* (chosen): one seam, no new
  inheritance level, and the only knowledge that changes is the three executors that register
  fds.
- *Add a second abstract base class for waitable transports*: rejected, because it splits the
  transport contract in two and adds architecture surface for no extra capability.
- *Let a shared-memory transport expose a negative fd and have executors poll it*: rejected,
  because an executor that registers no fd either blocks forever (epoll, reactor) or stops
  driving one direction (multi-thread).

# Same-host edges use a shared-memory ring, notified by eventfd over an abstract Unix socket

A same-host edge does not need a socket, so it carries its payload through a single-producer
single-consumer ring mapped from a memfd, with 1 MiB per edge. The mapping and two eventfds —
one readable while the ring has unread bytes, one readable while it has room to write — are
handed to the peer as fds over a short-lived abstract Unix socket, which is closed once the
control handshake is done. The ring is created with `memfd_create` and transferred as an fd, so
there is no name to derive, no `/dev/shm` object to unlink and nothing to clean up after a
crash. Cross-host edges keep using TCP.

**Considered options**

- *`pthread_cond_t`/`sem_t` in shared memory*: the obvious way to synchronize the ring, but
  neither is an fd, so neither can be waited on by `poll`/`epoll`. Bridging them would need a
  thread per edge, and busy-polling them would make the event-driven executors spin.
- *A named `shm_open` object plus its name in the handshake*: reuses the existing TCP handshake
  and needs no fd passing, but leaves up to 1 MiB per edge in `/dev/shm` whenever a rank dies
  between creation and unlink, and needs a name that cannot collide with a concurrent job.
- *Keeping TCP as a notification channel*: no Unix socket at all, but keeps a socket open for
  the whole run on edges that are otherwise socket-free.
- *Sizing the ring at 16 MiB*: fewer backpressure rounds, but 128 MiB for two ranks and 256 MiB
  for a four-rank ring; 1 MiB streams any message size with the same code path.

**Consequences**

- Same-host edges are Linux-only: `memfd_create`, `SCM_RIGHTS` over an abstract `AF_UNIX` socket
  and `eventfd`.
- The send side is a *level* (readable while the ring has room), not a one-shot event, because
  that is the only thing that can drive the first send of a collective and every later one.
- A peer that finishes first closes while bytes it wrote may still be unread, so receive drains
  what arrived and only then reports the close, the way a socket reads buffered data before EOF.
- A rank killed with `SIGKILL` cannot set the shared closed flag, so a peer can wait until the
  job-level timeout; detecting that would need a liveness handle per edge.
