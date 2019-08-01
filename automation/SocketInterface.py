from __future__ import absolute_import, print_function

import json
import struct
import traceback

import dill
import eventlet
import socketio
import six
from six.moves import input
from six.moves.queue import Queue

if six.PY2:

    class ConnectionAbortedError(Exception):
        pass


# TODO - Implement a cleaner shutdown for server socket
# see: https://stackoverflow.com/a/1148237


class serversocket:
    """
    A server socket to receive and process string messages
    from client sockets to a central queue
    """

    def __init__(self, name=None, verbose=False):
        self.verbose = verbose
        self.name = name
        self.queue = Queue()
        if self.verbose:
            print("Server bound to: " + str(self.sock.getsockname()))
        self._sio = socketio.Server()

    def start_accepting(self):
        """ Start the listener thread """
        self._sio_app = socketio.WSGIApp(self._sio)

        # TODO: host/port for socketio server needs to be extracted
        # using python-decouple
        eventlet.wsgi.server(eventlet.listen(("", 5000)), self._sio_app)

    @sio.event
    def handle_message(self, sid, msg):
        msglen, serialization = struct.unpack(">Lc", msg)
        if self.verbose:
            print(
                "Received message, length %d, serialization %r"
                % (msglen, serialization)
            )
        if serialization != b"n":
            try:
                if serialization == b"d":  # dill serialization
                    msg = dill.loads(msg)
                elif serialization == b"j":  # json serialization
                    msg = json.loads(msg.decode("utf-8"))
                elif serialization == b"u":  # utf-8 serialization
                    msg = msg.decode("utf-8")
                else:
                    print("Unrecognized serialization type: %r" % serialization)
                    return
                self.queue.put(msg)
            except (UnicodeDecodeError, ValueError) as e:
                print(
                    "Error de-serializing message: %s \n %s"
                    % (msg, traceback.format_exc(e))
                )

    def close(self):
        self._sio.disconnect()


class clientsocket:
    """A client socket for sending messages"""

    def __init__(self, serialization="json", verbose=False):
        """ `serialization` specifies the type of serialization to use for
        non-string messages. Supported formats:
            * 'json' uses the json module. Cross-language support. (default)
            * 'dill' uses the dill pickle module. Python only.
        """
        if serialization != "json" and serialization != "dill":
            raise ValueError("Unsupported serialization type: %s" % serialization)
        self.serialization = serialization
        self.verbose = verbose

        self._sio = socketio.Client()

    def connect(self, host, port):
        if self.verbose:
            print("Connecting to: %s:%i" % (host, port))
        self._sio.connect("http://{}:{}".format(host, port))

    def send(self, msg):
        """
        Sends an arbitrary python object to the connected socket. Serializes
        using dill if not string, and prepends msg len (4-bytes) and
        serialization type (1-byte).
        """
        if isinstance(msg, six.binary_type):
            serialization = b"n"
        elif isinstance(msg, six.text_type):
            serialization = b"u"
            msg = msg.encode("utf-8")
        elif self.serialization == "dill":
            msg = dill.dumps(msg, dill.HIGHEST_PROTOCOL)
            serialization = b"d"
        elif self.serialization == "json":
            msg = json.dumps(msg).encode("utf-8")
            serialization = b"j"
        else:
            raise ValueError("Unsupported serialization type set: %s" % serialization)

        if self.verbose:
            print("Sending message with serialization %s" % serialization)

        # prepend with message length
        msg = struct.pack(">Lc", len(msg), serialization) + msg
        self._sio.send(msg)

    def close(self):
        self._sio.disconnect()


def main():
    import sys

    # Just for testing
    if sys.argv[1] == "s":
        sock = serversocket(verbose=True)
        sock.start_accepting()
        input("Press enter to exit...")
        sock.close()
    elif sys.argv[1] == "c":
        host = input("Enter the host name:\n")
        port = input("Enter the port:\n")
        serialization = input("Enter the serialization type (default: 'json'):\n")
        if serialization == "":
            serialization = "json"
        sock = clientsocket(serialization=serialization)
        sock.connect(host, int(port))
        msg = None

        # some predefined messages
        tuple_msg = ("hello", "world")
        list_msg = ["hello", "world"]
        dict_msg = {"hello": "world"}

        def function_msg(x):
            return x

        # read user input
        while msg != "quit":
            msg = input("Enter a message to send:\n")
            if msg == "tuple":
                sock.send(tuple_msg)
            elif msg == "list":
                sock.send(list_msg)
            elif msg == "dict":
                sock.send(dict_msg)
            elif msg == "function":
                sock.send(function_msg)
            else:
                sock.send(msg)
        sock.close()


if __name__ == "__main__":
    main()
