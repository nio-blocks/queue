Queue
=====
A NIO block for queueing up signals. As signals pile up, the Queue block notifies a configurable `chunk_size` of signals in the queue at a configurable time `interval`. If incoming signals would overflow the queue, signals are popped off the front end as needed. These overflow signals are simply dropped, not notified and sent to the next block.

If a `group_by` string is configured, incoming signals are put into groups based on the value of `group_by`, which should be an expected attribute of the incoming signals. The configured `capacity` applies to *each* such group, not the block as a whole. If no `group_by` is set, there is only one "group"/one queue.

If a `uniqueness` expression is set, then each group will only contain signals that are unique according to the expression. If a signal is to be appended to the group that matches a signal already in the group, then it will be dropped.

A negative `interval` means that signals will not be emitted at any interval. Instead, the `emit` command is the only way for the block to notify signals.

Uses persistance to maintain queues/groups between stopping and starting of the block. If the `capacity` is configured smaller than the the persisted queues, then signals are removed from the back of the queue during block configure to get the queues down to the current configured `capacity`.

Properties
----------
- **backup_interval**: Period at which queues are backed up to disk using persistance. Queues are also backed up on stop.
- **capacity**: Size of each queue. When queue exceeds `capacity`, a signal is popped off the front end and *not* notified.
- **chunk_size**: Number of signals to notify each `interval` period and `emit` command.
- **group_by**: Signal attribute that determines what queue to put the signal in. Defaults to group `null` if attribute does not exist or `group_by` is unspecified.
- **interval**: Period at which signals are notified from queues.
- **load_from_persistence**: It `True`, after stopping the block, the previous state of the block (queue time remaining) will be loaded upon restart.
- **reload**: If `True`, notified signals immediately get reloaded back onto the end of the queue they came off of.
- **uniqueness**: If specified, each queue (i.e. `group_by`) will not allow multiple signals that evaluate to the same `uniqueness`. If a signal comes in that matches `group_by` and `uniqueness` with a signal already in the queue, then it the new signal dropped.
- **update**: If `True` and a uniqueness expression is set, incoming signals that would have been rejected by the uniqueness expression instead replace the signal that was competing with the incoming signal for uniqueness.

Inputs
------
- **default**: Any list of signals.

Outputs
-------
- **default**: List of signals emitted from queues each `interval` period and `emit` command.

Commands
--------
- **emit**: Notify signals off the end of the queues.
- **groups**: Display the list of groupings that signals are being grouped into.
- **remove**: Remove signals from the queue *group* where *query* evaluates to `True`. *query* uses the Expression Property syntax for evaluatuation. For example, to remove all signals, use `{{True}}` or to remove signals with *val* equal to 1 use `{{$val == 1}}`. If no *group* is specified, then all groups are inspected. Signals are not notified.
- **update_props**: Modify the value of a property on a started block. Parameter `props` is a dictionary where the `key` is a block property and the `value` is the new value. `interval` is the only property currently supported. When `interval` is updated, the recurring emit job will be cancelled and restarted, even if the new value is equal to the previous one.
- **view**: View signals in the queue *group*. If *group* is not specified, all signals in all queues are returned. Signals are not notified.

Dependencies
------------
None
