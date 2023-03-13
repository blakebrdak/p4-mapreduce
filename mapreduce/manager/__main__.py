"""MapReduce framework Manager node."""
import os
import tempfile
import logging
import json
import time
import socket
import threading
import click
import mapreduce.utils


# Configure logging
LOGGER = logging.getLogger(__name__)

class Worker:
    """Store a Registered worker node."""
    def __init__(self, host, port, state):
      self.host = host
      self.port = port
      self.state = state

class Manager:
    """Represent a MapReduce framework Manager node."""

    def __init__(self, host, port):
        """Construct a Manager instance and start listening for messages."""
        self.host = host
        self.port = port
        # List storing all of the worker objects
        workers = []

        LOGGER.info(
            "Starting manager host=%s port=%s pwd=%s",
            host, port, os.getcwd(),
        )

        # This is a fake message to demonstrate pretty printing with logging
        message_dict = {
            "message_type": "register",
            "worker_host": "localhost",
            "worker_port": 6001,
        }
        LOGGER.debug("TCP recv\n%s", json.dumps(message_dict, indent=2))

        # Set up threads
        signals = {"shutdown": False}
        threads = []
        thread = threading.Thread(target=self.heartbeat, args=(signals,))
        threads.append(thread)
        # Add in the fault tolerance thread when we need that to be used
        thread.start()

        # Open the TCP Thread
        self.message_handler(signals, workers)
        
        signals['shutdown'] = True
        thread.join()
        print("main() shutting down")

    def heartbeat(self, signals):
        """Thread to handle UDP heartbeat messages."""
        while not signals['shutdown']:
            print("Listening for heartbeat ...")
            time.sleep(2)

    def fault_tolerance(self):
        """Thread to handle fault tolerance."""
        # TODO
    
    def message_handler(self, signals, workers):
        """Handle the main TCP port and different messages."""
        # Create an INET, STREAMing socket, this is TCP
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    
            # Bind the socket to the server
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
    
            # Socket accept() will block for a maximum of 1 second.  If you
            # omit this, it blocks indefinitely, waiting for a connection.
            sock.settimeout(1)
    
            while not signals["shutdown"]:
        
                # Wait for a connection for 1s.  The socket library avoids
                # consuming CPU while waiting for a connection.
                try:
                    clientsocket, address = sock.accept()
                except socket.timeout:
                    continue
                print("Connection from", address[0])
    
                # Socket recv() will block for a maximum of 1 second.  If you omit
                # this, it blocks indefinitely, waiting for packets.
                clientsocket.settimeout(1)
    
                # Receive data, one chunk at a time.  If recv() times out before
                # we can read a chunk, then go back to the top of the loop and try
                # again.  When the client closes the connection, recv() returns
                # empty data, which breaks out of the loop.  We make a simplifying
                # assumption that the client will always cleanly close the
                # connection.
                with clientsocket:
                    message_chunks = []
                    while True:
                        try:
                            data = clientsocket.recv(4096)
                        except socket.timeout:
                            continue
                        if not data:
                            break
                        message_chunks.append(data)
                # Decode list-of-byte-strings to UTF8 and parse JSON data
                message_bytes = b''.join(message_chunks)
                message_str = message_bytes.decode("utf-8")
                try:
                    message_dict = json.loads(message_str)
                except json.JSONDecodeError:
                    continue
                print(message_dict)

                # AT THIS POINT, WE DECODE MESSAGES AND TAKE ACTIONS BASED ON MSG TYPE

                # SHUTDOWN 
                if message_dict["message_type"] == "shutdown":
                    # Send Shutdown message to all workers
                    for worker in workers:
                        message = json.dumps({"message_type": "shutdown"})
                        self.send_msg(message, worker.host, worker.port)
                    break
                
                # WORKER REGISTRATION
                if message_dict["message_type"] == "register":
                    # save new worker to worker list
                    worker_host = message_dict['worker_host']
                    worker_port = message_dict['worker_port']
                    worker = Worker(worker_host, worker_port, 'ready')
                    workers.append(worker)

                    # sent ack message
                    message = json.dumps({"message_type": "register_ack",
                                          "worker_host": worker_host,
                                          "worker_port": worker_port,})
                    self.send_msg(message, worker_host, worker_port)
                
        print("TCP message handler shutting down")

    def send_msg(self, message, worker_host, worker_port):
        """Used to send a simple, one connection message"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            # connect to the server
            sock.connect((worker_host, worker_port))
            # send a message
            sock.sendall(message.encode('utf-8'))

        


@click.command()
@click.option("--host", "host", default="localhost")
@click.option("--port", "port", default=6000)
@click.option("--logfile", "logfile", default=None)
@click.option("--loglevel", "loglevel", default="info")
@click.option("--shared_dir", "shared_dir", default=None)
def main(host, port, logfile, loglevel, shared_dir):
    """Run Manager."""
    tempfile.tempdir = shared_dir
    if logfile:
        handler = logging.FileHandler(logfile)
    else:
        handler = logging.StreamHandler()
    formatter = logging.Formatter(
        f"Manager:{port} [%(levelname)s] %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(loglevel.upper())
    Manager(host, port)


if __name__ == "__main__":
    main()
