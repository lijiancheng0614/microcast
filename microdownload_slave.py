import os
import time
import select
import socket
import argparse
from threading import Thread
from collections import deque
from urllib.request import urlretrieve

CONNECT_TIMEOUT = 10
RECV_BUFSIZE = 1024
TIMEOUT = 0.1

class Slave(object):
    def __init__(self, server_host, server_port, store_dir):
        self.client_socket = None
        self.address = None
        server_address = (server_host, server_port)
        start_time = time.time()
        while time.time() - start_time <= CONNECT_TIMEOUT:
            try:
                self.client_socket = socket.create_connection(server_address)
                self.address = self.client_socket.getsockname()
                print('{}:{} start.'.format(self.address[0], self.address[1]))
                print('Start connecting {}:{}.'.format(server_host, server_port))
                break
            except Exception as e:
                pass
        if not self.address:
            print('[ERROR] Cannot connect to {}:{}.'.format(server_host, server_port))
            exit()
        if not os.path.exists(store_dir):
            os.makedirs(store_dir)
        self.store_dir = store_dir
        self.stop_flag = False
        self.download_list = deque()
        self.download_thread = Thread(target = self.download)
    def download(self):
        while not self.stop_flag:
            if self.download_list:
                segment = self.download_list.popleft()
                print('Downloading {}.'.format(segment))
                store_path = os.path.join(self.store_dir, segment.split('/')[-1])
                try:
                    urlretrieve(segment, store_path)
                    self.client_socket.send('DONE;{}'.format(segment).encode())
                    print('Done {}.'.format(segment))
                except Exception as e:
                    print(e)
                    self.client_socket.send('FAIL;{}'.format(segment).encode())
                    print('Fail {}.'.format(segment))
                # microcast, get_peers()
    def handle_response(self, sock, data):
        if data.startswith('FIN'):
            self.stop_flag = True
        elif data.startswith('DOW'):
            segment = data.split(';')[1]
            # TODO: need to ACK?
            # sock.send('ACK;{}'.format(segment).encode())
            self.download_list.append(segment)
    def run(self):
        try:
            self.download_thread.start()
            while not self.stop_flag:
                read_sockets, write_sockets, error_sockets = select.select([self.client_socket], [], [])
                for sock in read_sockets:
                    if sock == self.client_socket:
                        data = sock.recv(RECV_BUFSIZE).decode()
                        self.handle_response(sock, data)
            self.stop_flag = True
            while self.download_thread.is_alive():
                self.download_thread.join(TIMEOUT)
        except Exception as e:
            print(e)
            self.stop_flag = True
            while self.download_thread.is_alive():
                self.download_thread.join(TIMEOUT)
        self.client_socket.close()

def set_arguments():
    parser = argparse.ArgumentParser(description='MircoDownload_slave')
    parser.add_argument('--host', required=True,
                        help='host')
    parser.add_argument('--port', type=int, required=True,
                        help='port')
    parser.add_argument('--store_dir', required=True,
                        help='store directory')
    return parser

if __name__ == '__main__':
    parser = set_arguments()
    cmd_args = parser.parse_args()

    slave = Slave(cmd_args.host, cmd_args.port, cmd_args.store_dir.strip())
    slave.run()
