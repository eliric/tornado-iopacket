import collections
import errno
import numbers
import os
import socket
import sys
import re


from tornado.concurrent import TracebackFuture
from tornado import ioloop
from tornado.log import gen_log, app_log
from tornado.netutil import ssl_wrap_socket, ssl_match_hostname, SSLCertificateError, _client_ssl_defaults, _server_ssl_defaults
from tornado import stack_context
from tornado.util import errno_from_exception

try:
    from tornado.platform.posix import _set_nonblocking
except ImportError:
    _set_nonblocking = None


_ERRNO_WOULDBLOCK = (errno.EWOULDBLOCK, errno.EAGAIN)

if hasattr(errno, "WSAEWOULDBLOCK"):
    _ERRNO_WOULDBLOCK += (errno.WSAEWOULDBLOCK,)



class FdClosedError(IOError):
    def __init__(self, real_error=None):
        super(FdClosedError, self).__init__('fd is closed')
        self.real_error = real_error

class BufferFullError(Exception):
    """Exception raised by `IOStream` methods when the buffer is full.
    """

class IOPacket(object):

    def __init__(self, socket, io_loop=None, max_read_buffer_size=None, max_packet_size=None, max_write_buffer_size=None):
        self.socket = socket
        self.socket.setblocking(False)
        self.io_loop = io_loop or ioloop.IOLoop.current()

        self.max_read_buffer_size = max_read_buffer_size or 104857600
        self.max_packet_size = max_packet_size
        self.max_write_buffer_size = max_write_buffer_size
        self._read_buffer_size = 0
        self._write_buffer_size = 0
        self.error = None

        self._unread_packets = collections.deque()
        self._unsent_packets = collections.deque()

        self._read_callback = None
        self._read_future = None
        self._write_callback = None
        self._write_future = None
        self._close_callback = None
        self._state = None
        self._pending_callbacks = 0
        self._closed = False

    def has_unread_packets(self):
        return len(self._unread_packets)

    def has_unsent_packets(self):
        return len(self._unsent_packets)

    def add_unread_packet(self, packet, address):
        if len(packet) + self._read_buffer_size > self.max_read_buffer_size:
            raise BufferFullError("Reached maximum read buffer size")
        else:
            self._unread_packets.append((packet, address))
            self._read_buffer_size += len(packet)

    def add_unsent_packet(self, packet, address):
        if self.max_write_buffer_size is not None and len(packet) + self._write_buffer_size > self.max_write_buffer_size:
            raise BufferFullError("Reached maximum write buffer size")
        else:
            self._unsent_packets.append((packet, address))
            self._write_buffer_size += len(packet)


    def reading(self):
        """Returns true if we are currently reading."""
        return self._read_callback is not None or self._read_future is not None

    def writing(self):
        """Returns true if we are currently writing."""
        return self.has_unsent_packets()

    def closed(self):
        """Returns true if the stream has been closed."""
        return self._closed

    def fileno(self):
        return self.socket

    def close_fd(self):
        self.socket.close()
        self.socket = None

    def get_fd_error(self):
        errno = self.socket.getsockopt(socket.SOL_SOCKET,
                                       socket.SO_ERROR)
        return socket.error(errno, os.strerror(errno))

    def read_from_fd(self):

        try:
            chunk, address = self.socket.recvfrom(self.max_packet_size)
        except socket.error as e:
            if e.args[0] in _ERRNO_WOULDBLOCK:
                return None, None
            else:
                raise

        return chunk, address

    def write_to_fd(self, data, address):
        return self.socket.sendto(data, address)

    def read(self, callback=None):
        future = self._set_read_callback(callback)
        try:
            self._try_inline_read()
        except:
            if future is not None:
                # Ensure that the future doesn't log an error because its
                # failure was never examined.
                future.add_done_callback(lambda f: f.exception())
            raise
        return future

    def _try_inline_read(self):
        if self.has_unread_packets():
            self._read_buffered_packet()
            return

        #no buffered packet
        self._check_closed()
        has_packet = False
        try:
            has_packet = self._unread_packets_loop()
        except Exception:
            self._maybe_run_close_callback()
            raise
        if has_packet:
            self._read_buffered_packet()
            return
        # We couldn't satisfy the read inline, so either close
        # or listen for new data.
        if self.closed():
            self._maybe_run_close_callback()
        else:
            self._add_io_state(ioloop.IOLoop.READ)

    def _unread_packets_loop(self):
        # This method is called from _handle_read and _try_inline_read.
        try:
            has_packet = False
            self._pending_callbacks += 1
            while not self.closed():
                if self._read_packet() is None:
                    break
                else:
                    has_packet = True
            return has_packet
        finally:
            self._pending_callbacks -= 1

    def _read_buffered_packet(self):
        """Attempts to complete the currently-pending read from the buffer.
        """
        self._run_read_callback()

    def write(self, data, address, callback=None):

        assert isinstance(data, bytes)
        self._check_closed()
        # We use bool(_write_buffer) as a proxy for write_buffer_size>0,
        # so never put empty strings in the buffer.
        if data:
            self.add_unsent_packet(data, address)
        if callback is not None:
            self._write_callback = stack_context.wrap(callback)
            future = None
        else:
            future = self._write_future = TracebackFuture()
            future.add_done_callback(lambda f: f.exception())

        self._handle_write()
        if self._unsent_packets:
            self._add_io_state(self.io_loop.WRITE)
        self._maybe_add_error_listener()
        return future

    def set_close_callback(self, callback):
        self._close_callback = stack_context.wrap(callback)
        self._maybe_add_error_listener()

    def close(self, exc_info=False):
        """Close this stream.

        If ``exc_info`` is true, set the ``error`` attribute to the current
        exception from `sys.exc_info` (or if ``exc_info`` is a tuple,
        use that instead of `sys.exc_info`).
        """
        if not self.closed():
            if exc_info:
                if not isinstance(exc_info, tuple):
                    exc_info = sys.exc_info()
                if any(exc_info):
                    self.error = exc_info[1]
            if self._state is not None:
                self.io_loop.remove_handler(self.fileno())
                self._state = None
            self.close_fd()
            self._closed = True
        self._maybe_run_close_callback()

    def _maybe_run_close_callback(self):
        # If there are pending callbacks, don't run the close callback
        # until they're done (see _maybe_add_error_handler)
        if self.closed() and self._pending_callbacks == 0:
            futures = []

            if self._read_future is not None:
                futures.append(self._read_future)
                self._read_future = None
            if self._write_future is not None:
                futures.append(self._write_future)
                self._write_future = None

            for future in futures:
                future.set_exception(FdClosedError(real_error=self.error))

            if self._close_callback is not None:
                cb = self._close_callback
                self._close_callback = None
                self._run_callback(cb)

            self._read_callback = self._write_callback = None
            self._unread_packets = self._unsent_packets = None

    def _handle_events(self, fd, events):
        if self.closed():
            gen_log.warning("Got events for closed stream %s", fd)
            return
        try:
            if self.closed():
                return
            if events & self.io_loop.READ:
                self._handle_read()
            if self.closed():
                return
            if events & self.io_loop.WRITE:
                self._handle_write()
            if self.closed():
                return
            if events & self.io_loop.ERROR:
                self.error = self.get_fd_error()
                # We may have queued up a user callback in _handle_read or
                # _handle_write, so don't close the IOStream until those
                # callbacks have had a chance to run.
                self.io_loop.add_callback(self.close)
                return
            state = self.io_loop.ERROR
            if self.reading():
                state |= self.io_loop.READ
            if self.writing():
                state |= self.io_loop.WRITE
            if state == self.io_loop.ERROR and self._read_buffer_size == 0:
                # If the connection is idle, listen for reads too so
                # we can tell if the connection is closed.  If there is
                # data in the read buffer we won't run the close callback
                # yet anyway, so we don't need to listen in this case.
                state |= self.io_loop.READ
            if state != self._state:
                assert self._state is not None, \
                    "shouldn't happen: _handle_events without self._state"
                self._state = state
                self.io_loop.update_handler(self.fileno(), self._state)
        except Exception:
            gen_log.error("Uncaught exception, closing connection.",
                          exc_info=True)
            self.close(exc_info=True)
            raise

    def _run_callback(self, callback, *args):
        def wrapper():
            self._pending_callbacks -= 1
            try:
                return callback(*args)
            except Exception:
                app_log.error("Uncaught exception, closing connection.",
                              exc_info=True)
                self.close(exc_info=True)
                raise
            finally:
                self._maybe_add_error_listener()
        with stack_context.NullContext():
            self._pending_callbacks += 1
            self.io_loop.add_callback(wrapper)

    def _handle_read(self):
        try:
            has_packet = self._unread_packets_loop()
        except Exception as e:
            gen_log.warning("error on read: %s" % e)
            self.close(exc_info=True)
            return
        if has_packet:
            self._read_buffered_packet()
            return
        else:
            self._maybe_run_close_callback()

    def _set_read_callback(self, callback):
        assert self._read_callback is None, "Already reading"
        assert self._read_future is None, "Already reading"
        if callback is not None:
            self._read_callback = stack_context.wrap(callback)
        else:
            self._read_future = TracebackFuture()
        return self._read_future

    def _run_read_callback(self):
        callback = self._read_callback
        self._read_callback = None
        if self._read_future is not None:
            assert callback is None
            future = self._read_future
            self._read_future = None
            future.set_result(self._consume_unread_packet())
        if callback is not None:
            assert (self._read_future is None)
            self._run_callback(callback, self._consume_unread_packet())
        else:
            # If we scheduled a callback, we will add the error listener
            # afterwards.  If we didn't, we have to do it now.
            self._maybe_add_error_listener()

    def _consume_unread_packet(self):
        packet = self._unread_packets.popleft()
        self._read_buffer_size -= len(packet[0])
        return packet

    def _consume_unsent_packet(self):
        packet = self._unsent_packets.popleft()
        self._write_buffer_size -= len(packet[0])
        return packet

    def _read_packet(self):
        """Reads from the socket and appends the result to the read buffer.

        Returns the number of bytes read.  Returns 0 if there is nothing
        to read (i.e. the read returns EWOULDBLOCK or equivalent).  On
        error closes the socket and raises an exception.
        """
        while True:
            try:
                chunk, address = self.read_from_fd()
            except (socket.error, IOError, OSError) as e:
                if errno_from_exception(e) == errno.EINTR:
                    continue
                self.close(exc_info=True)
                raise
            break
        if chunk is None:
            return None
        self.add_unread_packet(chunk, address)
        return chunk, address

    def _handle_write(self):
        while self._unsent_packets:
            try:
                packet = self._unsent_packets[0]
                wrote_bytes = self.write_to_fd(packet[0], packet[1])
                self._consume_unsent_packet()

            except (socket.error, IOError, OSError) as e:
                if e.args[0] in _ERRNO_WOULDBLOCK:
                    break
                else:
                    self.close(exc_info=True)
                    return
        if not self._unsent_packets:
            if self._write_callback:
                callback = self._write_callback
                self._write_callback = None
                self._run_callback(callback)
            if self._write_future:
                future = self._write_future
                self._write_future = None
                future.set_result(None)

    def _maybe_add_error_listener(self):
        if self._pending_callbacks != 0:
            return
        if self._state is None or self._state == ioloop.IOLoop.ERROR:
            if self.closed():
                self._maybe_run_close_callback()
            elif (self._read_buffer_size == 0 and
                  self._close_callback is not None):
                self._add_io_state(ioloop.IOLoop.READ)

    def _check_closed(self):
        if self.closed():
            raise FdClosedError(real_error=self.error)

    def _add_io_state(self, state):
        if self.closed():
            # connection has been closed, so there can be no future events
            return
        if self._state is None:
            self._state = ioloop.IOLoop.ERROR | state
            with stack_context.NullContext():
                self.io_loop.add_handler(
                    self.fileno(), self._handle_events, self._state)
        elif not self._state & state:
            self._state = self._state | state
            self.io_loop.update_handler(self.fileno(), self._state)


