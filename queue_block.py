import json
from collections import defaultdict
from datetime import timedelta
from nio.common.block.base import Block
from nio.common.command import command
from nio.common.discovery import Discoverable, DiscoverableType
from nio.metadata.properties import IntProperty, BoolProperty, PropertyHolder, \
    ExpressionProperty, ObjectProperty,StringProperty, TimeDeltaProperty
from nio.common.command.params.string import StringParameter
from nio.common.command.params.dict import DictParameter
from nio.modules.scheduler import Job
from nio.modules.threading import Lock
from nio.metadata.properties.expression import Evaluator
from .mixins.group_by.group_by_block import GroupBy


@command("update_props", DictParameter("props", default=''))
@command("view",
         StringParameter("query", default='{{ True }}'),
         StringParameter("group", default=''))
@command("remove",
         StringParameter("query", default=''),
         StringParameter("group", default=''))
@command("emit")
@Discoverable(DiscoverableType.block)
class Queue(GroupBy, Block):
    """ Queue block.

    A NIO block for queueing up signals. As signals pile up,
    the Queue block releases a configurable number at a configurable
    interval. If incoming signals would overflow the queue, signals
    are popped off the front as needed.

    If a 'group_by' string is configured, incoming signals are divided
    and grouped by the value of that attribute. The configured capacity
    applies to *each* such queue, not the block as a whole.

    """
    version = StringProperty(default='1.0')
    interval = TimeDeltaProperty(title='Notification Interval')
    backup_interval = TimeDeltaProperty(title='Backup Interval',
                                        visible=False,
                                        default={"minutes": 10})
    capacity = IntProperty(default=100, title='Capacity')
    group_by = ExpressionProperty(default='null', attr_default='null', title='Group By')
    chunk_size = IntProperty(default=1, title='Chunk Size')
    reload = BoolProperty(default=False, title='Auto-Reload?')
    uniqueness = ExpressionProperty(title='Queue Uniqueness Expression',
                                    attr_default=None)
    update = BoolProperty(title='Update Non-Unique Signals', default=False)

    def __init__(self):
        super().__init__()
        self._queues = defaultdict(list)
        self._queue_locks = {
            'null': Lock()
        }
        self._meta_lock = Lock()
        self._emit_job = None
        self._backup_job = None

    def configure(self, context):
        super().configure(context)
        self._load()

    def start(self):
        super().start()
        self._start_emit_job()
        self._backup_job = Job(
            self._backup,
            self.backup_interval,
            True
        )

    def stop(self):
        if self._emit_job is not None:
            self._emit_job.cancel()
        self._backup_job.cancel()
        self._backup()

    def process_signals(self, signals):
        self._logger.debug("Processing {} signals".format(len(signals)))
        self.for_each_group(self._push_group, signals)

    def pop(self, grp="null"):
        ''' Remove the top n signals from the specified queue.

        Args:
            grp (str): The queue from which to pop.
            count (int): The number of signals to pop off.
            reload (bool): If True, put popped signals back on queue.

        Returns:
            top_n (list): 'Count' signals from the front of the queue.

        '''
        count = self.chunk_size
        reload = self.reload
        # lock the queue we're popping from
        self._logger.debug("pop: {} {} {}".format(grp, count, reload))
        with self._get_lock(grp):
            # check out the front of the queue
            top_n = self._queues[grp][0:count]
            self._logger.debug(
                "Removing %d signals from %s_queue" % (len(top_n), grp))
            self._queues[grp][:] = self._queues[grp][len(top_n):]
            # If reloading, put signal back on queue.
            if reload:
                self._logger.debug("Reloading {}_queue".format(grp))
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
            self._logger.debug(
                "Testing uniqueness for signal: {}".format(unique_val))
        except Exception as e:
            unique_val = None
            self._logger.warning(
                "Uniqueness expression failed. Using value of None.")

        if unique_val is not None:
            for idx, sig in enumerate(queue):
                try:
                    sig_val = self.uniqueness(sig)
                except Exception as e:
                    sig_val = None
                if sig_val == unique_val:
                    self._logger.debug(
                        "Signal {} already in {}_queue".format(sig_val, grp)
                    )
                    if self.update:
                       queue[idx] = signal
                    return

        # pop one off the top of that queue if it's at capacity
        if len(queue) == self.capacity:
            self._logger.debug(
                "Pushing signal and capactity of {}_signal is full: {}".format(
                    grp, self.capacity
                )
            )
            queue.pop(0)

        self._logger.debug("Appending signal to {}_queue".format(grp))
        queue.append(signal)

    def _push_group(self, signals, group):
        # lock the queue before appending
        with self._get_lock(group):
            for signal in signals:
                self.push(signal, group)

    def _get_lock(self, grp="null"):
        ''' Returns the lock for a particular queue.

        Note that we're maintaining a synchronized dictionary of locks
        alongside our dict of queues.

        '''
        with self._meta_lock:
            self._queue_locks[grp] = self._queue_locks.get(grp, Lock())
        return self._queue_locks[grp]

    def _start_emit_job(self):
        if self.interval.total_seconds() >= 0:
            self._emit_job = Job(
                self.emit,
                self.interval,
                True
            )

    def emit(self):
        ''' Notify the configured number of signals from the front of the queue.

        '''
        signals_to_notify = self.for_each_group(self.pop)
        if signals_to_notify:
            self._logger.debug(
                "Notifying {} signals".format(len(signals_to_notify))
            )
            self.notify_signals(signals_to_notify)

    def _load(self):
        prev_queues = self.persistence.load('queues')
        # if persisted dictonary is not defaultdict, convert it
        if prev_queues:
            self._queues = defaultdict(list, prev_queues)
        # build _groups for groupby mixin
        self._groups = list(self._queues.keys())

    def _backup(self):
        ''' Persist the current state of the queues using the persistence module.

        '''
        # store the serialized signals and save to disk
        # grab the meta_lock so nobody else can interact with the queues during
        # serialization
        self._logger.debug("Persistence: backing up to file")
        self._meta_lock.acquire()
        self.persistence.store("queues", self._queues)
        self._meta_lock.release()
        self.persistence.save()

    def _inspect_group(self, response, group):
        response_group = {'count': 0, 'signals': []}
        query = response.get('query', '{{ True }}')
        ignored_signals = []
        for signal in self._queues.get(group, []):
            try:
                eval = Evaluator(query, None).evaluate(signal)
            except:
                eval = False
            if eval:
                response_group['signals'].append(json.loads(json.dumps(signal.to_dict(), indent=4, separators=(',', ': '), default=str)))
                response_group['count'] += 1
                response['count'] +=1
            else:
                ignored_signals.append(signal)
        response['groups'][group] = response_group
        return response, ignored_signals

    def view(self, query, group):
        ''' Command to view the signals that are in the queue.

        If no group parameter is specified, all queues are returned.
        '''
        self._logger.debug("Command: view")
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
                                kwargs={'response': response})

        return response

    def _view_group(self, group, response):
        with self._get_lock(group):
            response, _ = self._inspect_group(response, group)

    def remove(self, query, group):
        ''' Remove signals from *group* where *query* is True.

        Signals are not notified.

        '''
        self._logger.debug("Command: remove")
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
                                kwargs={'response': response,
                                        'query': query})

        return response

    def _remove_from_group(self, group, response, query):
        with self._get_lock(group):
            response, signals = self._inspect_group(response, group)
            # signals that don't match the query stay in the queue.
            self._queues[group] = signals

    def update_props(self, props):
        ''' Updates the *interval* property.

        The next scheduled emit job with be canceled and a new repeatable emit
        job is started.

        '''
        self._logger.debug("Command: update_props")
        response = {}

        if props is None or not isinstance(props, dict):
            response['message'] = \
                "'props' needs to be a dictionary: {}".format(props)
            return response

        # Update *interval*.
        interval = props.get('interval')
        if interval and isinstance(interval, dict) and \
                (interval.get('days') or interval.get('seconds') \
                 or interval.get('microseconds')):
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
            self._logger.info('Interval has been updated to {}'.format(interval))
        elif interval:
            response['message'] = "'interval' needs to be a timedelta dict: {}".format(interval)

        return response
