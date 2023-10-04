"""Thin VFS wrapper for accessing sqlite databases on GCS."""

import logging
import uuid
import apsw
import fsspec

logger = logging.getLogger(__name__)

class FSSpecVFS(apsw.VFS):
    def __init__(self, fs: fsspec.AbstractFileSystem):
        # TODO(rousik): name should be more dynamic than this.
        self.vfs_name = f"gcs-{str(uuid.uuid4())}"
        self.fs = fs
        super().__init__(name=self.vfs_name, base="")

    def xAccess(self, pathname: str, flags: int) -> bool:
        """Always returns True, no permissions checks."""
        return True

    def xOpen(self, name, flags: int):
        return FSSpecVFSFile(self.fs, name.filename())


class FSSpecVFSFile:
    def __init__(self, fs: fsspec.AbstractFileSystem, filepath):
        self.fs = fs
        self.filepath = filepath
        self.fd = fs.open(filepath, 'rb')
        self.bytes_read = 0
        self.read_calls = 0
        # TODO(rousik): consider flags... yo

    def xRead(self, amount, offset):
        # TODO(rousik): would it make sense to read-through to a local cache and
        # keep information on which parts of the file were already read?
        # Do we care about that?
        self.read_calls += 1
        self.bytes_read += amount
        self.fd.seek(offset)
        return self.fd.read(amount)

    def xFileControl(self, *args):
        return False

    def xCheckReservedLock(self):
        return False
    
    def xLock(self, level):
        pass

    def xUnlock(self, level):
        pass

    def xClose(self):
        self.fd.close()
        self.fd = None
        # Print out debug stats about how many reads and how much was transferred.
        logger.info(
            f"Remote file {self.filepath} incurred {self.read_calls} read"
            f" calls and {self.bytes_read} bytes were transferred."
        )

    def xFileSize(self):
        return self.fs.size(self.filepath)

    def xSync(self, flags):
        return True

    def xTruncate(self, newsize):
        raise NotImplementedError("Modifying files is not supported.")

    def xWrite(self, data, offset):
        raise NotImplementedError("Modifying files is not supported.")
    
    def xSectorSize(self):
        return 4096
    
    def xDeviceCharacteristics(self):
        """Returns characteristics of the device.

        Databases stored on GCS are considered immutable.
        """
        return apsw.SQLITE_IOCAP_IMMUTABLE