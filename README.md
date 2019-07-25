Queue
=====
The Queue block will gather incoming signals and emit a configurable 'chunk size' of signals at a configurable 'interval'. If incoming signals would overflow the queue 'capacity', the oldest signals are dropped from the queue and not emitted to the next block.

Properties
----------
- **backup_interval**: Period at which queues are backed up to disk using persistance. Queues are also backed up on stop.
- **capacity**: Size of each queue. When the queue exceeds `capacity`, the oldest signals are dropped from the queue and *not* notified.
- **chunk_size**: Number of signals to emit at each `interval` or `emit` command.
- **group_by**: Signal attribute that determines what queue to put the signal in. Defaults to group `null` if attribute does not exist or `group_by` is unspecified.
- **interval**: Period at which signals are emitted from queues. If `0`, signals will not be emitted only after processing imcoming signals, rather than at an interval. Note: It may be necessary to enable `Auto Reload` for this to work as intended.
- **load_from_persistence**: If `True`, after stopping the block, the previous state of the block (queue time remaining) will be loaded upon restart.
- **reload**: If `True`, emitted signals immediately get reloaded back into the end of the queue.
- **uniqueness**: If specified, each queue (i.e. `group_by`) will not allow multiple signals that evaluate to the same `uniqueness`. If a signal comes in that matches `group_by` and `uniqueness` with a signal already in the queue, then the new signal is dropped from the queue.
- **update**: If `True` and a uniqueness expression is set, incoming signals that would have been rejected by the uniqueness expression instead replace the signal that was competing with the incoming signal for uniqueness.

Inputs
------
- **default**: Any list of signals.

Outputs
-------
- **default**: List of signals emitted from queues each `interval` period and `emit` command.

Commands
--------
- **emit**: Emit signals off the end of the queues.
- **groups**: Returns a list of the block’s current signal groupings.
- **remove**: Remove signals from the queue *group* where *query* evaluates to `True`. *Query* uses the Expression Property syntax for evaluatuation. For example, to remove all signals, use `{{True}}` or to remove signals with *val* equal to 1 use `{{$val == 1}}`. If no *group* is specified, then all groups are inspected. Signals are not notified.
- **update_props**: Modify the value of a property on a started block. Parameter `props` is a dictionary where the `key` is a block property and the `value` is the new value. `interval` is the only property currently supported. When `interval` is updated, the recurring emit job will be cancelled and restarted, even if the new value is equal to the previous one.
- **view**: View signals in the queue *group*. If *group* is not specified, all signals in all queues are returned. Signals are not notified.

Dependencies
------------
None

