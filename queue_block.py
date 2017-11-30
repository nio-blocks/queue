import json
from collections import defaultdict
from datetime import timedelta
from threading import Lock

from nio.block.base import Block
from nio.block.mixins.group_by.group_by import GroupBy
from nio.block.mixins.persistence.persistence import Persistence
from nio.command import command
from nio.command.params.dict import DictParameter
from nio.command.params.string import StringParameter
from nio.modules.scheduler import Job
from nio.properties import IntProperty, BoolProperty, \
    Property, TimeDeltaProperty, VersionProperty
from nio.properties.util.evaluator import Evaluator


@command("update_props", DictParameter("props", default=''))
@command("view",
         StringParameter("query", default='{{ True }}'),
         StringParameter("group", default=''))
@command("remove",
         StringParameter("query", default=''),
         StringParameter("group", default=''))
@command("emit")
class Queue(Persistence, GroupBy, Block):
    """ Queue block.

    A NIO block for queueing up signals. As signals pile up,
    the Queue block releases a configurable number at a configurable
    interval. If incoming signals would overflow the queue, signals
    are popped off the front as needed.

    If a 'group_by' string is configured, incoming signals are divided
    and grouped by the value of that attribute. The configured capacity
    applies to *each* such queue, not the block as a whole.

    """
    version = VersionProperty('1.0.0')
    interval = TimeDeltaProperty(title='Notification Interval',
                                 default={'seconds': 1},
                                 allow_none=True)
    capacity = IntProperty(default=100, title='Capacity')
    chunk_size = IntProperty(default=1, title='Chunk Size')
    reload = BoolProperty(default=False, title='Auto-Reload?')
    uniqueness = Property(title='Queue Uniqueness Expression',
                          allow_none=True,
                          default="{{ None }}")
    update = BoolProperty(title='Update Non-Unique Signals', default=False)

    def persisted_values(self):
        return ["_queues"]

    def __init__(self):
        super().__init__()
        self._queues = defaultdict(list)
        self._queue_locks = defaultdict(Lock)
        self._meta_lock = Lock()
        self._emit_job = None

    def configure(self, context):
        super().configure(context)
        self._queues = defaultdict(list, self._queues)
        # Make sure perisisted queue capacity is less than current config
        for queue_name, queue_values in self._queues.items():
            self._queues[queue_name] = queue_values[:self.capacity()]
        # build _groups for groupby mixin
        self._groups = set(self._queues.keys())

    def start(self):
        super().start()
        self._start_emit_job()

    def stop(self):
        if self._emit_job is not None:
            self._emit_job.cancel()
        super().stop()

    def process_signals(self, signals):
        self.logger.debug("Processing {} signals".format(len(signals)))
        self.for_each_group(self._push_group, signals)

    def pop(self, grp):
        ''' Remove the top n signals from the specified queue.

        Args:
            grp (str): The queue from which to pop.
            count (int): The number of signals to pop off.
            reload (bool): If True, put popped signals back on queue.

        Returns:
            top_n (list): 'Count' signals from the front of the queue.

        '''
        count = self.chunk_size()
        reload = self.reload()
        # lock the queue we're popping from
        self.logger.debug("pop: {} {} {}".format(grp, count, reload))
        with self._get_lock(grp):
            # check out the front of the queue
            top_n = self._queues[grp][0:count]
            self.logger.debug(
                "Removing %d signals from %s_queue" % (len(top_n), grp))
            self._queues[grp][:] = self._queues[grp][len(top_n):]
            # If reloading, put signal back on queue.
            if reload:
                self.logger.debug("Reloading {}_queue".format(grp))
                self._queues[grp].extend(top_n)
        return top_n

    def push(self, signal, grp):
        ''' Add a signal to the back of the queue.

        Args:
            signal (Signal): The signal to add.
            grp (str): Group to add signal to.

        Returns:
            None

        '''
        queue = self._queues[grp]

        # check for uniqueness if property is set
        try:
            unique_val = self.uniqueness(signal)
            self.logger.debug(
                "Testing uniqueness for signal: {}".format(unique_val))
        except Exception as e:
            unique_val = None
            self.logger.warning(
                "Uniqueness expression failed. Using value of None.")

        if unique_val is not None:
            for idx, sig in enumerate(queue):
                try:
                    sig_val = self.uniqueness(sig)
                except Exception as e:
                    sig_val = None
                if sig_val == unique_val:
                    self.logger.debug(
                        "Signal {} already in {}_queue".format(sig_val, grp)
                    )
                    if self.update():
                        queue[idx] = signal
                    return

        # pop one off the top of that queue if it's at capacity
        if len(queue) == self.capacity():
            self.logger.debug(
                "Pushing signal and capactity of {}_signal is full: {}".format(
                    grp, self.capacity()
                )
            )
            queue.pop(0)

        self.logger.debug("Appending signal to {}_queue".format(grp))
        queue.append(signal)

    def _push_group(self, signals, group):
        # lock the queue before appending
        with self._get_lock(group):
            for signal in signals:
                self.push(signal, group)

    def _get_lock(self, grp):
        ''' Returns the lock for a particular queue.

        Note that we're maintaining a synchronized dictionary of locks
        alongside our dict of queues.

        '''
        with self._meta_lock:
            self._queue_locks[grp] = self._queue_locks.get(grp, Lock())
        return self._queue_locks[grp]

    def _start_emit_job(self):
        ''' Start job that emits signals from the queue '''
        if self.interval() and self.interval().total_seconds() > 0:
            # only schedule if the interval is a positive number
            self._emit_job = Job(
                self.emit,
                self.interval(),
                True
            )

    def emit(self):
        ''' Notify the configured number of signals from the front of the queue.

        '''
        signals_to_notify = self.for_each_group(self.pop)
        if signals_to_notify:
            self.logger.debug(
                "Notifying {} signals".format(len(signals_to_notify))
            )
            self.notify_signals(signals_to_notify)

    def _inspect_group(self, response, group):
        response_group = {'count': 0, 'signals': []}
        query = response.get('query', '{{ True }}')
        ignored_signals = []
        for signal in self._queues.get(group, []):
            try:
                eval = Evaluator(query).evaluate(signal)
            except:
                eval = False
            if eval:
                response_group['signals'].append(
                    json.loads(json.dumps(
                        signal.to_dict(),
                        indent=4, separators=(',', ': '),
                        default=str))
                )
                response_group['count'] += 1
                response['count'] += 1
            else:
                ignored_signals.append(signal)
        response['groups'][group] = response_group
        return response, ignored_signals

    def view(self, query, group):
        ''' Command to view the signals that are in the queue.

        If no group parameter is specified, all queues are returned.
        '''
        self.logger.debug("Command: view")
        response = {}
        response['query'] = query
        response['group'] = group
        response['count'] = 0
        response['groups'] = {}

        if group and group in self._queues:
            # if group exists, return only the specified group
            self._view_group(group, response)
        elif not group:
            # if no group is specifed in params return all groups
            self.for_each_group(self._view_group,
                                **{'response': response})

        return response

    def _view_group(self, group, response):
        with self._get_lock(group):
            response, _ = self._inspect_group(response, group)

    def remove(self, query, group):
        ''' Remove signals from *group* where *query* is True.

        Signals are not notified.

        '''
        self.logger.debug("Command: remove")
        response = {}
        response['query'] = query
        response['group'] = group
        response['count'] = 0
        response['groups'] = {}

        if group and group in self._queues:
            # if group exists, remove from only only the specified group
            self._remove_from_group(group, response, query)
        elif not group:
            # if no group is specifed in params return all groups
            self.for_each_group(self._remove_from_group,
                                **{'response': response, 'query': query})
        return response

    def _remove_from_group(self, group, response, query):
        with self._get_lock(group):
            response, signals = self._inspect_group(response, group)
            # signals that don't match the query stay in the queue, but if
            # there are no signals remaining, delete the entire queue.
            if len(signals) > 0:
                self._queues[group] = signals
            else:
                # _queues is a dict with keys that make up the set _groups.
                # These must be kept in sync when removing keys in order to
                # maintain the true state of the block. If these objects are
                # not synced, a "view" or "remove" command for all groups will
                # show that groups which have previously been expired are still
                # present, due to the for_each_group() call, which uses the
                # _groups set to iterate over the groups.
                self.logger.debug("Deleting empty queue {}.".format(group))
                self._queues.pop(group, None)
                self._groups.remove(group)

    def update_props(self, props):
        ''' Updates the *interval* property.

        The next scheduled emit job with be canceled and a new repeatable emit
        job is started.

        '''
        self.logger.debug("Command: update_props")
        response = {}

        if props is None or not isinstance(props, dict):
            response['message'] = \
                "'props' needs to be a dictionary: {}".format(props)
            return response

        # Update *interval*.
        interval = props.get('interval')
        if interval and isinstance(interval, dict) and \
                (interval.get('days') or
                 interval.get('seconds') or interval.get('microseconds')):
            days = interval.get('days', 0)
            seconds = interval.get('seconds', 0)
            microseconds = interval.get('microseconds', 0)
            interval = timedelta(days, seconds, microseconds)
            response['interval'] = interval
            response['prev_interval'] = self.interval
            # cancel emit job and restart with new interval
            if self._emit_job is not None:
                self._emit_job.cancel()
            self._start_emit_job()
            self.interval = interval
            self.logger.info(
                'Interval has been updated to {}'.format(interval))
        elif interval:
            response['message'] = \
                "'interval' needs to be a timedelta dict: {}".format(interval)

        return response
