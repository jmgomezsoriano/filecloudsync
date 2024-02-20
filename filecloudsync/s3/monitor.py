from os.path import split, relpath
from threading import Thread, Event, Lock
from typing import Set, Callable

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from filecloudsync import s3
from filecloudsync.s3.core import Location, Operation, apply_bucket_diffs, apply_local_diffs, _sync_upload, upload_file


class S3Monitor(Thread, FileSystemEventHandler):
    """ A monitor to synchronize a bucket with a folder. """

    def __init__(self, bucket: str, folder: str, delay: int = 60, files: Set[str] = None, **kwargs) -> None:
        """ Create a monitor of a bucket or some files of that bucketm and synchronize them with a given folder.

        .. code-block:: python
            x = 1 # Testing embedded code
            print(x)

        :param bucket: The bucket name.
        :param folder: The folder path.
        :param delay: The delay between buckets check.
            This does not apply in local folder that detects changes immediately.
        :param files: A list of keys to watch in Unix file path format.
            If none is given, then check all the bucket/folder files.
        :param kwargs: The s3 connection credentials.
        """
        super().__init__()
        self._client = s3.connect(**kwargs)
        self.bucket = bucket
        self.folder = folder
        self.delay = delay
        self.files = files
        self._stop_event = False
        self._observer = Observer()
        self._interrupt_event = Event()
        self._listeners = set()
        self._processing = False  # List of files blockued until the operation finishes
        self._lock = Lock()

    def on_modified(self, event: FileSystemEvent) -> None:
        """ It's executed when a watch folder or file is modified
        :param event: The file event.
        """
        self._process_file_event(event, Operation.MODIFIED, Location.LOCAL)

    def on_created(self, event: FileSystemEvent) -> None:
        """ It's executed when a watch folder or file is created
        :param event: The file event
        """
        self._process_file_event(event, Operation.ADDED, Location.LOCAL)

    def on_deleted(self, event: FileSystemEvent) -> None:
        """ It's executed when a watch folder or file is deleted
        :param event: The file event
        """
        self._process_file_event(event, Operation.DELETED, Location.LOCAL)

    def _trigger(self, file: str, operation: Operation, where: Location) -> None:
        """ Trigger this event to the listeners.
        :param file: The file with the event
        :param operation: The operation realized in that file
        :param where: Which file is, the local one or the bucket one
        """
        for listener in self._listeners:
            listener(file, operation, where)

    def run(self) -> None:
        """ Execute the monitor """
        s3.sync(self._client, self.bucket, self.folder, self.files)
        self._observer.schedule(self, self.folder, recursive=True)
        self._observer.start()
        try:
            while not self._stop_event:
                self._check_bucket_changes()
                self._interrupt_event.wait(timeout=self.delay)
        finally:
            self._observer.stop()

    def add(self, listener: Callable[[str, Operation, Location], None]) -> None:
        """ Add a listener
        :param listener: The listener to add
        """
        self._listeners.add(listener)

    def remove(self, listener: Callable[[str, Operation, Location], None]) -> None:
        """ Remove a listener

        :param listener: The listener to remove
        """
        self._listeners.remove(listener)

    def stop(self):
        """ Stops the monitor """
        self._stop_event = True

    def join(self, timeout: int = None):
        """ Wait until the thread finishes or the timeout is reached """
        self._interrupt_event.set()
        self._observer.join(timeout)
        super().join(timeout)

    def __enter__(self):
        """ Starts the monitor """
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ Stop the monitor """
        self.stop()
        self.join()

    def _process_file_event(self, event: FileSystemEvent, operation: Operation, where: Location) -> None:
        key = relpath(event.src_path, self.folder).replace('\\', '/')
        if not self._processing and not event.is_directory and (not self.files or key in self.files):
            with self._lock:
                self._observer.unschedule_all()
                try:
                    client, endpoint = self._client, self._client.meta.endpoint_url
                    self._processing = True
                    if operation == Operation.MODIFIED:
                        print(f'Modifying {event.src_path} on {where.value}')
                        upload_file(event.src_path, client, self.bucket, key)
                    elif operation == Operation.DELETED:
                        print(f'Deleting {event.src_path} on {where.value}')
                    else:
                        print(f'Creating {event.src_path} on {where.value}')
                        upload_file(event.src_path, client, self.bucket, key)
                    changes, local_files = s3.check_local_changes(client, self.bucket, self.folder, self.files)
                    bucket_files = s3.load_bucket_sync_status(endpoint, self.bucket, self.folder, self.files)
                    apply_local_diffs(client, self.bucket, self.folder, bucket_files, local_files, changes)
                    self._trigger(event.src_path, operation, where)
                finally:
                    self._observer.schedule(self, self.folder, recursive=True)
                    self._processing = False

    def _check_bucket_changes(self):
        if not self._processing:
            with self._lock:
                changes, bucket_files = s3.check_bucket_changes(self._client, self.bucket, self.folder, self.files)
                local_files = s3.load_local_sync_status(self._client.meta.endpoint_url, self.bucket, self.folder,
                                                        self.files)  # get_folder_files(self.folder, self.files)
                apply_bucket_diffs(self._client, self.bucket, self.folder, bucket_files, local_files, changes)
                if changes:
                    self._observer.unschedule_all()
                    for file, operation in changes.items():
                        self._trigger(file, operation, Location.BUCKET)
                    self._observer.schedule(self, self.folder, recursive=True)
