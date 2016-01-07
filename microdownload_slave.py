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
BROADCAST_BUFSIZE = 2048
TIMEOUT = 0.1
BYTES_FILE = 'F'.encode()
BYTES_FILE_EOF = 'E'.encode()
BYTES_SEP = '/'.encode()
BYTES_REQ = 'R'.encode()
BYTE_SPACE = ' '.encode()

class Slave(object):
    def __init__(self, server_host, server_port, store_dir, broadcast_port=12345):
        self.client_socket = None
        server_address = (server_host, server_port)
        start_time = time.time()
        while time.time() - start_time <= CONNECT_TIMEOUT:
            try:
                self.client_socket = socket.create_connection(server_address)
                address = self.client_socket.getsockname()
                print('{}:{} start.'.format(address[0], address[1]))
                print('Start connecting {}:{}.'.format(server_host, server_port))
                break
            except Exception as e:
                pass
        if not self.client_socket:
            print('[ERROR] Cannot connect to {}:{}.'.format(server_host, server_port))
            exit()
        if not os.path.exists(store_dir):
            os.makedirs(store_dir)
        self.store_dir = store_dir
        self.stop_flag = False
        self.download_list = deque()
        self.download_thread = Thread(target = self.download)
        # === microbroadcast
        self.segment_list = list()
        self.broadcast_port = broadcast_port
        self.broadcast_list = deque()
        self.broadcast_thread = Thread(target = self.broadcast)
        self.overhear_thread = Thread(target = self.overhear)
    def download(self):
        while not self.stop_flag:
            if self.download_list:
                segment = self.download_list.popleft()
                print('Downloading {}.'.format(segment))
                file_name = segment.split('/')[-1]
                store_path = os.path.join(self.store_dir, file_name)
                try:
                    urlretrieve(segment, store_path)
                    self.segment_list.append(file_name)
                    self.broadcast_list.append(store_path)
                    data = 'DONE;{}'.format(segment).encode()
                    data += BYTE_SPACE * (RECV_BUFSIZE - len(data))
                    self.client_socket.send(data)
                    print('Done {}.'.format(segment))
                except Exception as e:
                    print('[WARN] {}.'.format(e))
                    data = 'FAIL;{}'.format(segment).encode()
                    data += BYTE_SPACE * (RECV_BUFSIZE - len(data))
                    self.client_socket.send(data)
                    print('Fail {}.'.format(segment))
    def handle_response(self, sock, data):
        if data.startswith('FIN'):
            self.stop_flag = True
        elif data.startswith('DOW'):
            segment = data.split(';')
            if len(segment) > 2:
                print('[ERROR] response {}.'.format(data))
            else:
                segment = segment[1]
                # TODO: need to ACK?
                # sock.send('ACK;{}'.format(segment).encode())
                self.download_list.append(segment)
    def run(self):
        try:
            self.download_thread.start()
            self.overhear_thread.start()
            self.broadcast_thread.start()
            while not self.stop_flag:
                read_sockets, write_sockets, error_sockets = select.select([self.client_socket], [], [])
                for sock in read_sockets:
                    if sock == self.client_socket:
                        data = sock.recv(RECV_BUFSIZE).decode().strip()
                        self.handle_response(sock, data)
        except Exception as e:
            print('[ERROR] run: {}.'.format(e))
        self.stop_flag = True
        while self.download_thread.is_alive():
            self.download_thread.join(TIMEOUT)
        while self.overhear_thread.is_alive():
            self.overhear_thread.join(TIMEOUT)
        while self.broadcast_thread.is_alive():
            self.broadcast_thread.join(TIMEOUT)
        self.client_socket.close()
    # === microbroadcast
    def broadcast(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_address = ('<broadcast>', self.broadcast_port)
        while not self.stop_flag or self.broadcast_list:
            if not self.broadcast_list:
                continue
            file_path = self.broadcast_list.popleft()
            file_name = file_path.split(os.sep)[-1]
            FILEBYTES_SIZE = BROADCAST_BUFSIZE - 2 - len(file_name.encode())
            fd = open(file_path, 'rb')
            filebytes = fd.read(FILEBYTES_SIZE)
            while filebytes:
                data = BYTES_FILE + file_name.encode() + BYTES_SEP + filebytes
                sock.sendto(data, broadcast_address)
                filebytes = fd.read(FILEBYTES_SIZE)
            fd.close()
            # send EOF
            file_size = os.stat(file_path).st_size
            data = BYTES_FILE_EOF + file_name.encode() + BYTES_SEP + str(file_size).encode()
            sock.sendto(data, broadcast_address)
            print('Broadcast {}.'.format(file_name))
        sock.close()
    def overhear(self):
        sock = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.bind(('', self.broadcast_port))
        while not self.stop_flag:
            read_sockets, write_sockets, error_sockets = select.select([sock], [], [], 0)
            for s in read_sockets:
                if s == sock:
                    data, address = sock.recvfrom(BROADCAST_BUFSIZE)
                    data = data.strip()
                    if data.startswith(BYTES_FILE):
                        sep = data.find(BYTES_SEP)
                        file_name = data[1:sep].decode()
                        if file_name in self.segment_list:
                            continue
                        print('Overhear file {}.'.format(file_name))
                        file_path = os.path.join(self.store_dir, file_name)
                        fd = open(file_path, 'ab')
                        fd.write(data[sep + 1:])
                        fd.close()
                    elif data.startswith(BYTES_FILE_EOF):
                        sep = data.find(BYTES_SEP)
                        file_name = data[1:sep].decode()
                        if file_name in self.segment_list:
                            continue
                        file_path = os.path.join(self.store_dir, file_name)
                        file_size = os.stat(file_path).st_size
                        expected_file_size = 0
                        try:
                            expected_file_size = int(data[sep + 1:].decode())
                            if file_size == expected_file_size:
                                self.segment_list.append(file_name)
                                print('Received file {}.'.format(file_name))
                        except Exception as e:
                            print('[ERROR] Receive {} error.'.format(file_name))
                            print('[ERROR] Expected file size: {}. Received file size: {}.'.format(
                                expected_file_size, file_size))
                    elif data.startswith(BYTES_REQ):
                        # TODO: someone request a missing segment
                        pass
                    break
        sock.close()

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
