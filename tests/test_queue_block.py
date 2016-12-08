from collections import defaultdict
from threading import Event
from unittest.mock import MagicMock, patch

from nio.testing.block_test_case import NIOBlockTestCase
from nio.signal.base import Signal

from ..queue_block import Queue


class EventSignal(Signal):
    def __init__(self, event=None):
        super().__init__()
        self._event = event or Event()


class FlavorSignal(Signal):
    def __init__(self, flavor, meta='regular'):
        super().__init__()
        self.flavor = flavor
        self.meta = meta


class EventFlavorSignal(Signal):
    def __init__(self, flavor, event=None):
        super().__init__()
        self._event = event or Event()
        self.flavor = flavor


class TestQueue(NIOBlockTestCase):

    def test_emit(self):
        e = Event()
        signals = [EventSignal(e)]
        blk = Queue()
        config = {
            "interval": {
                "seconds": 1
            },
            "capacity": 4,
            "chunk_size": 1,
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        e.wait(2)
        self.assertEqual(len(blk._queues[None]), 0)
        self.assert_num_signals_notified(1, blk)
        blk.stop()

    def test_negative_interval(self):
        """ Don't emit signals on any interval when it is negative """
        e = Event()
        signals = [EventSignal(e)]
        blk = Queue()
        config = {
            "interval": {
                "seconds": -1
            },
            "capacity": 4,
            "chunk_size": 1,
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        e.wait(1)
        self.assertEqual(len(blk._queues[None]), 1)
        self.assert_num_signals_notified(0, blk)
        blk.stop()

    def test_zero_interval(self):
        """ Don't emit signals on any interval when it is zero """
        e = Event()
        signals = [EventSignal(e)]
        blk = Queue()
        config = {
            "interval": {
                "seconds": 0
            },
            "capacity": 4,
            "chunk_size": 1,
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        e.wait(1)
        self.assertEqual(len(blk._queues[None]), 1)
        self.assert_num_signals_notified(0, blk)
        blk.stop()

    def test_group_by(self):
        signals = [
            FlavorSignal(None),
            FlavorSignal('apple'),
            FlavorSignal('cherry')
        ]
        blk = Queue()
        config = {
            "interval": {
                "minutes": 1
            },
            "capacity": 100,
            "group_by": '{{$flavor}}'
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        self.assertEqual(len(blk._queues[None]), 1)
        self.assertEqual(len(blk._queues['cherry']), 1)
        self.assertEqual(len(blk._queues['apple']), 1)
        blk.stop()

    def test_full(self):
        signals = [
            FlavorSignal('cherry'),
            FlavorSignal('umami')
        ]
        blk = Queue()
        config = {
            "interval": {
                "minutes": 1
            },
            "capacity": 1,
            "log_level": "DEBUG"
        }

        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        self.assertEqual(len(blk._queues[None]), 1)
        self.assertEqual(blk._queues[None][0].flavor, 'umami')
        blk.stop()

    def test_reload(self):
        e1 = Event()
        e2 = Event()
        signals = [
            EventFlavorSignal(flavor='apple', event=e1),
            EventFlavorSignal(flavor='cherry', event=e2)
        ]
        blk = Queue()
        config = {
            "interval": {
                "seconds": 1
            },
            "capacity": 100,
            "group_by": '{{$flavor}}',
            "reload": True
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        self.assertEqual(len(blk._queues['cherry']), 1)
        self.assertEqual(len(blk._queues['apple']), 1)
        e1.wait(2)
        self.assertEqual(len(blk._queues['cherry']), 1)
        self.assertEqual(len(blk._queues['apple']), 1)
        self.assert_num_signals_notified(2, blk)
        blk.stop()

    def test_unique(self):
        signals = [
            FlavorSignal(flavor='apple'),
            FlavorSignal(flavor='cherry', meta='regular'),
            FlavorSignal(flavor='cherry', meta='sour')
        ]
        blk = Queue()
        config = {
            "interval": {
                "minutes": 1
            },
            "capacity": 4,
            "uniqueness": "{{$flavor}}"
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        self.assertEqual(len(blk._queues[None]), 2)
        self.assertEqual(blk._queues[None][1].meta, 'regular')
        blk.stop()

    def test_unique_with_default_config(self):
        signals = [
            FlavorSignal(flavor='apple'),
            FlavorSignal(flavor='cherry', meta='regular'),
            FlavorSignal(flavor='cherry', meta='sour')
        ]
        blk = Queue()
        self.configure_block(blk, {})
        blk.start()
        blk.process_signals(signals)
        self.assertEqual(len(blk._queues[None]), 3)
        self.assertEqual(blk._queues[None][1].meta, 'regular')
        blk.stop()

    def test_unique_with_update(self):
        signals = [
            FlavorSignal(flavor='apple'),
            FlavorSignal(flavor='cherry', meta='regular'),
            FlavorSignal(flavor='cherry', meta='sour')
        ]
        blk = Queue()
        config = {
            "interval": {
                "minutes": 1
            },
            "capacity": 4,
            "uniqueness": "{{$flavor}}",
            "update": True
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        self.assertEqual(len(blk._queues[None]), 2)
        self.assertEqual(blk._queues[None][1].meta, 'sour')
        blk.stop()

    def test_all(self):
        e1 = Event()
        e2 = Event()
        e3 = Event()
        e4 = Event()
        signals = [
            EventFlavorSignal(flavor='apple', event=e1),
            EventFlavorSignal(flavor='cherry', event=e2),
            EventFlavorSignal(flavor='cherry', event=e2),
            EventFlavorSignal(flavor='cherry', event=e4)
        ]
        blk = Queue()
        config = {
            "interval": {
                "seconds": 1
            },
            "capacity": 2,
            "group_by": '{{$flavor}}',
            "reload": True,
            "uniqueness": "{{$flavor}}"
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        self.assertEqual(len(blk._queues['cherry']), 1)
        self.assertEqual(len(blk._queues['apple']), 1)
        e1.wait(2)
        self.assertEqual(len(blk._queues['cherry']), 1)
        self.assertEqual(len(blk._queues['apple']), 1)
        blk.process_signals([FlavorSignal('cherry')])
        self.assertEqual(len(blk._queues['cherry']), 1)
        blk.stop()

    def test_view_command(self):
        signals = [
            FlavorSignal(None),
            FlavorSignal('apple'),
            FlavorSignal('cherry')
        ]
        blk = Queue()
        config = {
            "interval": {
                "minutes": 1
            },
            "capacity": 100,
            "group_by": '{{$flavor}}'
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        # view nothing from all groups
        resp = blk.view('', None)
        self.assertEqual(len(resp['groups'][None]['signals']), 0)
        self.assertEqual(resp['groups'][None]['count'], 0)
        self.assertEqual(resp['count'], 0)
        self.assertEqual(resp['query'], '')
        # viewing only None group is not possible because it becomes 'all'
        resp = blk.view('{{ True }}', None)
        self.assertEqual(len(resp['groups'][None]['signals']), 1)
        self.assertEqual(resp['groups'][None]['count'], 1)
        self.assertEqual(resp['count'], 3)
        self.assertEqual(resp['query'], '{{ True }}')
        # view all groups
        resp = blk.view('{{ True }}', '')
        self.assertEqual(resp['count'], 3)
        self.assertEqual(resp['query'], '{{ True }}')
        self.assertEqual(len(blk._queues[None]), 1)
        self.assertEqual(len(blk._queues['cherry']), 1)
        self.assertEqual(len(blk._queues['apple']), 1)
        blk.stop()

    def test_remove_command(self):
        signals = [
            FlavorSignal(None),
            FlavorSignal('apple'),
            FlavorSignal('cherry')
        ]
        blk = Queue()
        config = {
            "interval": {
                "minutes": 1
            },
            "capacity": 100,
            "group_by": '{{$flavor}}'
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)

        # don't remove anything from None
        resp = blk.remove('', None)
        self.assertEqual(len(resp['groups'][None]['signals']), 0)
        self.assertEqual(resp['groups'][None]['count'], 0)
        self.assertEqual(resp['count'], 0)
        self.assertEqual(resp['query'], '')
        self.assertEqual(len(blk._queues[None]), 1)
        self.assertTrue(None in blk._groups)

        # remove 'apple' group
        resp = blk.remove('{{ True }}', 'apple')
        self.assertEqual(len(resp['groups']['apple']['signals']), 1)
        self.assertEqual(resp['groups']['apple']['count'], 1)
        self.assertEqual(resp['count'], 1)
        self.assertEqual(resp['query'], '{{ True }}')
        self.assertFalse('apple' in blk._groups)
        self.assertFalse('apple' in blk._queues)

        # remove everything from all groups
        resp = blk.remove('{{ True }}', '')
        self.assertEqual(resp['count'], 2)
        self.assertEqual(resp['query'], '{{ True }}')
        self.assertEqual(len(blk._queues), 0)
        self.assertEqual(len(blk._groups), 0)
        blk.stop()

    def _check_persisted_values(self, blk, persisted_queues):
        blk._load.assert_called_once_with()
        # Make sure queues is a defaultdict
        self.assertEqual(defaultdict, type(blk._queues))
        # Check values of loaded queues
        for queue_name, queue_values in persisted_queues.items():
            self.assertEqual(queue_values[:blk.capacity()],
                             blk._queues[queue_name])
            self.assertTrue(queue_name in blk._groups)

    def test_load_persistence(self):
        blk = Queue()
        persisted_queues = defaultdict(list, {'a': [1], 'b': [2, 3]})
        def side_effect():
            blk._queues = persisted_queues
        blk._load = MagicMock(side_effect=side_effect)
        self.configure_block(blk, {})
        self._check_persisted_values(blk, persisted_queues)

    def test_load_persistence_when_capacity_config_shrinks(self):
        blk = Queue()
        persisted_queues = defaultdict(list, {'a': [1], 'b': [2, 3]})
        def side_effect():
            blk._queues = persisted_queues
        blk._load = MagicMock(side_effect=side_effect)
        # Use a smaller capacity than is loaded from persistence
        self.configure_block(blk, {"capacity": 1})
        self._check_persisted_values(blk, persisted_queues)
