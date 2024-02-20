from threading import Thread, Event, Lock
from typing import Set, Callable

from filecloudsync import s3
from filecloudsync.s3.core import Location, Operation


class S3Monitor(Thread):
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
        self._interrupt_event = Event()
        self._hooks = set()

    def _trigger(self, file: str, operation: Operation, location: Location) -> None:
        """ Trigger this event to the hooks.
        :param file: The file with the event
        :param operation: The operation realized in that file
        :param location: Where the file is, on the local folder or on the bucket
        """
        for hook in self._hooks:
            hook(file, operation, location)

    def run(self) -> None:
        """ Execute the monitor """
        s3.sync(self._client, self.bucket, self.folder, self.files)
        while not self._stop_event:
            for key, operation, location in s3.sync(self._client, self.bucket, self.folder, self.files):
                self._trigger(key, operation, location)
            self._interrupt_event.wait(timeout=self.delay)

    def add(self, handle: Callable[[str, Operation, Location], None]) -> None:
        """ Add a event handle
        :param handle: The handle function to add
        """
        self._hooks.add(handle)

    def remove(self, handle: Callable[[str, Operation, Location], None]) -> None:
        """ Remove a event handle

        :param handle: The handle function to remove
        """
        self._hooks.remove(handle)

    def stop(self):
        """ Stops the monitor """
        self._stop_event = True

    def join(self, timeout: int = None):
        """ Wait until the thread finishes or the timeout is reached """
        self._interrupt_event.set()
        super().join(timeout)

    def __enter__(self):
        """ Starts the monitor """
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ Stop the monitor """
        self.stop()
        self.join()
