"""
This module contains all :class:`Resource` like resources.

These resources can be used by a limited number of processes at a time
(e.g., a gas station with a limited number of fuel pumps). Processes
*request* these resources to become a user (or to own them) and have to
*release* them once they are done (e.g., vehicles arrive at the gas
station, use a fuel-pump, if one is available, and leave when they are
done).

Requesting a resources is modeled as "putting a process' token into the
resources" and releasing a resources correspondingly as "getting
a process' token out of the resource". Thus, calling
``request()``/``release()`` is equivalent to calling
``put()``/``get()``. Note, that releasing a resource will always succeed
immediately, no matter if a process is actually using a resource or not.

Beside :class:`Resource`, there are a :class:`PriorityResource`, were
processes can define a request priority, and
a :class:`PreemptiveResource` whose resource users can be preempted by
other processes with a higher priority.

"""
from collections import namedtuple

from simpy.core import BoundClass
from simpy.resources import base


Preempted = namedtuple('Preempted', 'by, usage_since')
"""Used as interrupt cause for preempted processes."""


class Request(base.Put):
    """Request access on the *resource*. The event is triggered once
    access is granted.

    If the maximum capacity of users is not reached, the requesting
    process obtains the resource immediately. If the maximum capacity is
    reached, the requesting process waits until another process releases
    the resource.

    The request is automatically released when the request was created
    within a :keyword:`with` statement.

    """
    def __exit__(self, exc_type, value, traceback):
        super(Request, self).__exit__(exc_type, value, traceback)
        self.resource.release(self)


class Release(base.Get):
    """Releases the access privilege to *resource* granted by *request*.
    This event is triggered immediately.

    If there's another process waiting for the *resource*, resume it.

    If the request was made in a :keyword:`with` statement (e.g., ``with
    res.request() as req:``), this method is automatically called when
    the ``with`` block is left.

    """
    def __init__(self, resource, request):
        self.request = request
        super(Release, self).__init__(resource)


class PriorityRequest(Request):
    """Request the *resource* with a given *priority*. If the *resource*
    supports preemption and *preempted* is true other processes with
    access to the *resource* may be preempted (see
    :class:`PreemptiveResource` for details).

    This event type inherits :class:`Request` and adds some additional
    attributes needed by :class:`PriorityResource` and
    :class:`PreemptiveResource`

    """
    def __init__(self, resource, priority=0, preempt=True):
        self.priority = priority
        self.preempt = preempt
        self.time = resource._env.now
        self.key = (self.priority, self.time)
        super(PriorityRequest, self).__init__(resource)


class SortedQueue(list):
    """Queue that sorts events by their ``key`` attribute."""
    def __init__(self, maxlen=None):
        super(SortedQueue, self).__init__()
        #: Maximum length of the queue
        self.maxlen = maxlen

    def append(self, item):
        """Append *item* to the queue and keep the queue sorted."""
        if self.maxlen is not None and len(self) >= self.maxlen:
            raise ValueError('Cannot append event. Queue is full.')

        super(SortedQueue, self).append(item)
        super(SortedQueue, self).sort(key=lambda e: e.key)


class Resource(base.BaseResource):
    """A resource has a limited number of slots that can be requested
    by a process.

    If all slots are taken, requesters are put into a queue. If
    a process releases a slot, the next process is popped from the queue
    and gets one slot.

    The ``env`` parameter is the :class:`~simpy.core.Environment`
    instance the resource is bound to.

    The ``capacity`` defines the number of slots and must be a positive
    integer.

    """

    def __init__(self, env, capacity=1):
        super(Resource, self).__init__(env)
        self._capacity = capacity
        self.users = []
        self.queue = self.put_queue

    put = BoundClass(Request)
    get = BoundClass(Release)
    request = put
    release = get

    @property
    def capacity(self):
        """Maximum capacity of the resource."""
        return self._capacity

    @property
    def count(self):
        """Number of users currently using the resource."""
        return len(self.users)

    def _do_put(self, event):
        if len(self.users) < self.capacity:
            self.users.append(event)
            event.succeed()

    def _do_get(self, event):
        try:
            self.users.remove(event.request)
        except ValueError:
            pass
        event.succeed()


class PriorityResource(Resource):
    """This class works like :class:`Resource`, but requests are sorted
    by priority.

    The :attr:`~Resource.queue` is kept sorted by priority in ascending
    order (a lower value for *priority* results in a higher priority),
    so more important request will get the resource earlier.

    """
    PutQueue = SortedQueue
    GetQueue = SortedQueue

    put = BoundClass(PriorityRequest)
    request = put


class PreemptiveResource(PriorityResource):
    """This resource mostly works like :class:`Resource`, but users of
    the resource can be *preempted* by higher prioritized requests.

    Furthermore, the queue of requests is also sorted by *priority*.

    If a less important request is preempted, the process of that
    request will receive an :class:`~simpy.core.Interrupt` with
    a :class:`Preempted` instance as cause.

    """
    def _do_put(self, event):
        if len(self.users) >= self.capacity and event.preempt:
            # Check if we can preempt another process
            preempt = sorted(self.users, key=lambda e: e.key)[-1]

            if preempt.key > event.key:
                self.users.remove(preempt)
                preempt.proc.interrupt(Preempted(by=event.proc,
                                                 usage_since=preempt.time))

        return super(PreemptiveResource, self)._do_put(event)