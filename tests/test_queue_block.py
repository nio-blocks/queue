from unittest.mock import patch
from ..queue_block import Queue
from nio.util.support.block_test_case import NIOBlockTestCase
from nio.common.signal.base import Signal
from nio.modules.threading import Event


class EventSignal(Signal):
    def __init__(self, event=None):
        super().__init__()
        self._event = event or Event()


class FlavorSignal(Signal):
    def __init__(self, flavor):
        super().__init__()
        self.flavor = flavor


class EventFlavorSignal(Signal):
    def __init__(self, flavor, event=None):
        super().__init__()
        self._event = event or Event()
        self.flavor = flavor


class TestQueue(NIOBlockTestCase):

    def setUp(self):
        super().setUp()
        self.last_notified = []

    def signals_notified(self, signals):
        self.last_notified = signals
        signals[0]._event.set()

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_emit(self, *back_patch):
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
        self.assertEqual(len(blk._queues['null']), 0)
        self.assert_num_signals_notified(1, blk)
        blk.stop()

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_negative_interval(self, *back_patch):
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
        self.assertEqual(len(blk._queues['null']), 1)
        self.assert_num_signals_notified(0, blk)
        blk.stop()

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_group_by(self, *back_patch):
        signals = [
            EventSignal(),
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
        self.assertEqual(len(blk._queues['null']), 1)
        self.assertEqual(len(blk._queues['cherry']), 1)
        self.assertEqual(len(blk._queues['apple']), 1)
        blk.stop()

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_full(self, *back_patch):
        signals = [
            FlavorSignal('cherry'),
            FlavorSignal('umami')
        ]
        blk = Queue()
        config = {
            "interval": {
                "minutes": 1
            },
            "capacity": 1
        }
        self.configure_block(blk, config)
        blk.start()
        blk.process_signals(signals)
        self.assertEqual(len(blk._queues['null']), 1)
        self.assertEqual(blk._queues['null'][0].flavor, 'umami')
        blk.stop()

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_reload(self, *back_patch):
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

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_unique(self, *back_patch):
        signals = [
            FlavorSignal(flavor='apple'),
            FlavorSignal(flavor='cherry'),
            FlavorSignal(flavor='cherry')
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
        self.assertEqual(len(blk._queues['null']), 2)
        blk.stop()

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_all(self, *back_patch):
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

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_view_command(self, *back_patch):
        signals = [
            EventSignal(),
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
        # view 'null' group
        resp = blk.view('null')
        self.assertEqual(len(resp['groups']['null']['signals']), 1)
        self.assertEqual(resp['groups']['null']['count'], 1)
        self.assertEqual(resp['count'], 1)
        # view all groups
        resp = blk.view('')
        self.assertEqual(resp['count'], 3)
        self.assertEqual(len(blk._queues['null']), 1)
        self.assertEqual(len(blk._queues['cherry']), 1)
        self.assertEqual(len(blk._queues['apple']), 1)
        blk.stop()

    @patch.object(Queue, '_load')
    @patch.object(Queue, '_backup')
    def test_remove_command(self, *back_patch):
        signals = [
            EventSignal(),
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
        # don't remove anything from null
        resp = blk.remove('', 'null')
        self.assertEqual(len(resp['groups']['null']['signals']), 0)
        self.assertEqual(resp['groups']['null']['count'], 0)
        self.assertEqual(resp['count'], 0)
        self.assertEqual(len(blk._queues['null']), 1)
        # removing everyting from null
        resp = blk.remove('{{True}}', 'null')
        self.assertEqual(len(resp['groups']['null']['signals']), 1)
        self.assertEqual(resp['groups']['null']['count'], 1)
        self.assertEqual(resp['count'], 1)
        self.assertEqual(len(blk._queues['null']), 0)
        # remove everything from all groups
        resp = blk.remove('{{True}}', '')
        self.assertEqual(resp['count'], 2)
        self.assertEqual(len(blk._queues['null']), 0)
        self.assertEqual(len(blk._queues['cherry']), 0)
        self.assertEqual(len(blk._queues['apple']), 0)
        blk.stop()
