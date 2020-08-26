import random

import simpy
from itertools import count
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    List,
    NamedTuple,
    Optional,
    Union,
)

from simpy.core import BoundClass, Environment
from simpy.resources import store
import desmod 

class DPStoreGet(store.StoreGet):
    """Request to get an *item* from the *store* matching the *filter*. The
    request is triggered once there is such an item available in the store.

    *filter* is a function receiving one item. It should return ``True`` for
    items matching the filter criterion. The default function returns ``True``
    for all items, which makes the request to behave exactly like
    :class:`StoreGet`.

    """

    def __init__(
        self,
        resource: 'DPStore',
        epsilon: float,
        block_nr: int
    ):
        self.epsilon = epsilon
        self.block_nr = block_nr
        """The filter function to filter items in the store."""
        super().__init__(resource)


class DPStore(store.Store):
    if TYPE_CHECKING:

        def get(
            self, epsilon: float, block_nr: int
        ) -> DPStoreGet:
            """Request to get an *item*, for which *filter* returns ``True``,
            out of the store."""
            return DPStoreGet(self, epsilon, block_nr )

    else:
        get = BoundClass(DPStoreGet)

    def _do_get(  # type: ignore[override] # noqa: F821
        self, event: DPStoreGet
    ) -> Optional[bool]:
        if len(self.items) >= event.block_nr:
            return_blocks = []
            for i in random.sample(range(len(self.items)), k=event.block_nr):
                block = self.items[i]
                if block["epsilon"] - event.epsilon <0:
                    block["epsilon"] = -1
                else:
                    block["epsilon"] = block["epsilon"] - event.epsilon
                return_blocks.append(block)

            event.succeed(return_blocks)
        return True

        # for item in self.items:
        #     if event.filter(item):
        #         self.items.remove(item)
        #         event.succeed(item)
        #         break
        # return True ## return false??? filter=false.


RANDOM_SEED = 42
SIM_TIME = 100
INIT_EPSILON:float = 5
data_block_id = count()
task_id = count()
BLOCK_ARRIVAL_PERIOD = 10
def block_generator(name, env, out_pipe):
    """A process which randomly generates messages."""
    while True:
        # wait for next transmission
        yield env.timeout(BLOCK_ARRIVAL_PERIOD)

        # messages are time stamped to later check if the consumer was
        # late getting them.  Note, using event.triggered to do this may
        # result in failure due to FIFO nature of simulation yields.
        # (i.e. if at the same env.now, message_generator puts a message
        # in the pipe first and then message_consumer gets from pipe,
        # the event.triggered will be True in the other order it will be
        # False
        block = {"timestamp": env.now,
                 "block_id": next(data_block_id),
                 "epsilon": INIT_EPSILON,
                 "comment": '%s says hello at %d' % (name, env.now)
                 }
        # (env.now,{next(data_block_id), epsilon), '%s says hello at %d' % (name, env.now))

        out_pipe.put(block)
        print("at time %d: generate block %s" % (env.now,block["block_id"]))



def consumer_generator( env, in_pipe):
    """A process which consumes messages."""
    task_duration_range = [2, 5]
    while True:
        # task inter-arrival time
        yield env.timeout(random.randint(4, 8))
        task_duration = random.randint(*task_duration_range)
        consumer_name = 'Task %d' % next(task_id)
        print("at time %d: generate %s" % (env.now,consumer_name))
        env.process(consumer(consumer_name, env, in_pipe, task_duration))






def consumer(name, env, in_pipe, task_complete_time):
    # Get event for message pipe
    print("at time %d: %s starts, getting data block..." % ( env.now, name))
    epsilon: float = 0.6

    # half of task getting block wont triggered
    block_demand_nr: int = random.randint(*[1,2*SIM_TIME/BLOCK_ARRIVAL_PERIOD])
    blocks: list = yield in_pipe.get(epsilon, block_demand_nr)
    # for b in blocks:
    print("at time %d: %s gets data block " % (env.now, name), [ "ID:%d, epsilon:%.2f" % (b["block_id"],b["epsilon"]) for b in blocks])

    # if msg[0] < env.now:
    #     # if message was already put into pipe, then
    #     # message_consumer was late getting to it. Depending on what
    #     # is being modeled this, may, or may not have some
    #     # significance
    #     print('LATE Getting Message: at time %d: %s received message: %s' %
    #           (env.now, name, msg[-1]))
    #
    # else:
    #     # message_consumer is synchronized with message_generator
    #     print('at time %d: %s received message: %s.' %
    #           (env.now, name, msg[-1]))

    yield env.timeout(task_complete_time)
    print("at time %d: %s finishes" % (env.now, name))

# Setup and start the simulation
random.seed(RANDOM_SEED)
env = simpy.Environment()

# For one-to-one or many-to-one type pipes, use Store
pipe = DPStore(env)
env.process(block_generator('Generator A', env, pipe))
env.process(consumer_generator( env, pipe))

print('\nOne-to-one pipe communication\n')
env.run(until=SIM_TIME)

