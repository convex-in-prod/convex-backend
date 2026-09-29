# Search archive cleanup after runtime shutdown

The archive cache already delegates recursive deletion to a dedicated thread.
Using asynchronous filesystem calls there still depends on Tokio's blocking pool,
which can have stopped before the cache's final handles are dropped. Use synchronous
filesystem calls on that existing cleanup thread so retirement remains available
through runtime shutdown.

The recursive walk clears read-only permissions, skips symlinks, and tolerates paths
that disappear during cleanup. Unexpected filesystem errors still fail the cleanup
thread. No new worker, retry policy, storage format or configuration is introduced.

The focused test `clears_readonly_nested_archive_before_removal` checks nested
read-only cleanup. It does not simulate every runtime shutdown interleaving.
