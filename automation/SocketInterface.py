from __future__ import absolute_import, print_function

import asyncio
import json
import struct
import traceback

import dill
import websockets
import six
from six.moves import input
from six.moves.queue import Queue

if six.PY2:

    class ConnectionAbortedError(Exception):
        pass


class serversocket:
    """
    A server socket to receive and process string messages
    from client sockets to a central queue
    """

    def __init__(self, name=None, verbose=False):
        self.verbose = verbose
        self.name = name
        self.queue = Queue()

    def start_accepting(self):
        """ Start the listener thread """
        start_server = websockets.serve(self._websocket_handler, "0.0.0.0", 5000)

        asyncio.get_event_loop().run_until_complete(start_server)
        asyncio.get_event_loop().run_forever()

        if self.verbose:
            # TODO: this is not the right thing to do. need to extract
            # the host and port
            print("Server bound to: " + str(self.start_server))

    async def _websocket_handler(self, websocket, path):
        """
        This is the sole handler for websocket messages.
        """
        msg = await websocket.recv()
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
        asyncio.get_event_loop().stop()


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
        self._websock = None

    def connect(self, host, port):
        if self.verbose:
            print("Connecting to: %s:%i" % (host, port))
        self._uri = "ws://{}:{}".format(host, port)

    async def _send_msg(self, uri, msg, serialization):
        # prepend with message length
        packed_msg = struct.pack(">Lc", len(msg), serialization) + msg
        async with websockets.connect(self._uri) as websock:
            await websock.send(packed_msg)

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

        # Build a new function to pass into the event loop so we can
        # send asynchronously

        asyncio.get_event_loop().run_until_complete(
            self._send_msg(self._uri, msg, serialization)
        )

    def close(self):
        # Closing the socket is no longer necessary as the websockets
        # API when the coroutine returns after sending
        pass


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
