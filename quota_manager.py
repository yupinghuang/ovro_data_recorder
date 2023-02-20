import argparse
import os
import glob
import shutil
import time
import threading
from collections import deque
from typing import Optional
import signal
import logging

from mnc.mcs import MonitorPoint, Client
from mnc.common import setup_signal_handling

ALPHA = 0.3 # Adjustment rate for the update interval

class StorageLogger(object):
    """
    Monitoring class for logging how storage is used by a pipeline and for enforcing
    a directory quota, if needed.
    """
    log: logging.Logger
    id: str
    directory: str
    quota: Optional[int]
    shutdown_event: threading.Event
    update_interval: int
    _client: Client
    _files: deque
    _file_sizes: deque

    def __init__(self, log, id, directory, quota=None, shutdown_event=None, update_interval=600):
        self.log = log
        self.id = id
        self.directory = directory
        if quota == 0:
            quota = None
        self.quota = quota
        if shutdown_event is None:
            shutdown_event = threading.Event()
        self.shutdown_event = shutdown_event
        self.update_interval = update_interval
        
        self._client = Client(id)
        
        self._reset()
        
    def _reset(self):
        self._files = deque()
        self._file_sizes = deque()
        
        ts = time.time()
        self._client.write_monitor_point('storage/active_disk_size',
                                        0, timestamp=ts, unit='B')
        self._client.write_monitor_point('storage/active_disk_free',
                                        0, timestamp=ts, unit='B')
        self._client.write_monitor_point('storage/active_directory',
                                        self.directory, timestamp=ts)
        self._client.write_monitor_point('storage/active_directory_size',
                                        0, timestamp=ts, unit='B')
        self._client.write_monitor_point('storage/active_directory_count',
                                        0, timestamp=ts)
        
    def _update(self):
        try:
            current_files = glob.glob(os.path.join(self.directory, '*'))
            current_files.sort()    # The files should have sensible names that
                                    # reflect their creation times
            
            new_files = deque()
            new_file_sizes = deque()
            for filename in current_files:
                try:
                    i = self._files.index(filename)
                    new_files.append(filename)
                    new_file_sizes.append(self._file_sizes[i])
                except ValueError:
                    size = getsize(filename)
                    new_files.append(filename)
                    new_file_sizes.append(size)
            self._files = new_files
            self._file_sizes = new_file_sizes
        except Exception as e:
            self.log.warning("Quota manager could not refresh the file list: %s", str(e))
            
    def _manage_quota(self):
        t0 = time.time()
        total_size = sum(self._file_sizes)
        
        removed = 0
        removed_size = 0
        while total_size > self.quota and len(self._files) > 1:
            to_remove = self._files.pop()
            size = self._file_sizes.pop()
            try:
                shutil.rmtree(to_remove)
                removed += 1
                removed_size += size
                total_size -= size
            except Exception as e:
                self.log.warning("Quota manager could not remove '%s': %s", to_remove, str(e))
                
        if removed > 0:
            self.log.debug("=== Quota Report ===")
            self.log.debug(" items removed: %i", len(removed))
            self.log.debug(" space freed: %i B", removed_size)
            self.log.debug(" elapsed time: %.3f s", time.time()-t0)
            self.log.debug("===   ===")
            
    def shutdown(self):
        self.shutdown_event.set()
    
    def main(self, once=False):
        """
        Main logging loop.  May be run only once with the "once" keyword set to
        True.
        """
        
        while not self.shutdown_event.is_set():
            # Update the state
            t0 = time.time()
            self._update()
            
            # Quota management, if needed
            if self.quota is not None:
                self._manage_quota()
                
            # Find the disk size and free space for the disk hosting the
            # directory - this should be quota-aware
            ts = time.time()
            st = os.statvfs(self.directory)
            disk_free = st.f_bavail * st.f_frsize
            disk_total = st.f_blocks * st.f_frsize
            self._client.write_monitor_point('storage/active_disk_size',
                                            disk_total, timestamp=ts, unit='B')
            self._client.write_monitor_point('storage/active_disk_free',
                                            disk_free, timestamp=ts, unit='B')
            
            # Find the total size of all files
            ts = time.time()
            total_size = sum(self._file_sizes)
            file_count = len(self._files)
            self._client.write_monitor_point('storage/active_directory',
                                            self.directory, timestamp=ts)
            self._client.write_monitor_point('storage/active_directory_size',
                                            total_size, timestamp=ts, unit='B')
            self._client.write_monitor_point('storage/active_directory_count',
                                            file_count, timestamp=ts)
            
            runtime = time.time() - t0
            t_sleep = self.update_interval if self.update_interval > 0 \
                else ALPHA * self.update_interval + (1- ALPHA) * runtime
            
            # Report
            self.log.debug("=== Storage Report ===")
            self.log.debug(" directory: %s", self.directory)
            self.log.debug(" disk size: %i B", disk_total)
            self.log.debug(" disk free: %i B", disk_free)
            self.log.debug(" file count: %i", file_count)
            self.log.debug(" total size: %i B", total_size)
            self.log.debug(" elapsed time: %.3f s", time.time()-t0)
            self.log.debug(" sleeping for: %.3f s", t_sleep)
            self.log.debug("===   ===")
            
            # Sleep
            if once:
                break
                
            self.shutdown_event.wait(t_sleep)
            
        self.log.info("StorageLogger - Done")

def main():
    parser = argparse.ArgumentParser(description='Quota manager')
    parser.add_argument('--id', type=str, required=True,
                        help='ID of the pipeline')
    parser.add_argument('--directory', type=str, required=True,
                        help='Directory to monitor')
    parser.add_argument('--quota', type=int, default=None,
                        help='Quota in bytes')
    parser.add_argument('--update-interval', type=int, default=600,
                        help='Update interval in seconds')
    parser.add_argument('--log-level', type=str, default='INFO',
                        help='Log level')
    parser.add_argument('--once', action='store_true',
                        help='Run the quota manager only once')
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(level=args.log_level.upper())
    log = logging.getLogger('storage_logger')
    
    op = StorageLogger(log, args.id, args.directory, quota=args.quota,
                           update_interval=args.update_interval)
    threads = [threading.Thread(target=op.main, name=type(op).__name__, kwargs={'once': args.once})]
    shutdown_event = setup_signal_handling(threads)
    # Launch!
    log.info("Launching %i thread(s)", len(threads))
    for thread in threads:
        thread.start()
        
    while not shutdown_event.is_set():
        signal.pause()

    log.info("Shutdown, waiting for threads to join")
    for thread in threads:
        thread.join()
    log.info("All done")
    return 0
    