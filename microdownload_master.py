import m3u8
import select
import socket
import argparse
from threading import Thread
from collections import deque
from urllib.parse import urljoin

RECV_BUFSIZE = 1024
TIMEOUT = 0.1
BYTE_SPACE = ' '.encode()

class Master(object):
    def __init__(self, host, port, segments, K=5, TRIED_TIMES=5):
        self.address = (host, port)
        self.segments = segments[::-1]
        self.N = len(segments)
        self.K = K
        self.TRIED_TIMES = TRIED_TIMES
        self.stop_flag = False
        self.backlog_dict = dict()
        self.failcount_dict = dict()
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_thread = Thread(target = self.get_slaves)
        self.response_queue = deque()
        self.handle_slave_response_thread = Thread(target = self.handle_slave_response)
    def fail_segment(self, segment):
        # scenario 1: ask slave to download, not response
        # scenario 2: ask slave to download, download fail
        # if fail TRIED_TIMES times, ignore this segment
        self.failcount_dict[segment] = self.failcount_dict.get(segment, 0) + 1
        if self.failcount_dict[segment] < self.TRIED_TIMES:
            self.segments.append(segment)
    def handle_slave_response(self):
        while not self.stop_flag:
            if not self.response_queue:
                continue
            (sock, data) = self.response_queue.popleft()
            try:
                (status, segment) = data.split(';')
                if status == 'DONE' or status == 'FAIL':
                    if segment in self.backlog_dict[sock]:
                        self.backlog_dict[sock].remove(segment)
                        if status == 'DONE':
                            print('Done downloading {}.'.format(segment))
                            self.N -= 1
                        else:
                            print('Fail downloading {}.'.format(segment))
                            self.fail_segment(segment)
            except Exception as e:
                print('[ERROR] response {}.'.format(data))
    def get_slaves(self):
        self.server_socket.bind(self.address)
        self.server_socket.listen(5)
        while not self.stop_flag:
            slave_list = list(self.backlog_dict.keys())
            read_sockets, write_sockets, error_sockets = select.select(slave_list + [self.server_socket], [], [], 0)
            for sock in read_sockets:
                if sock == self.server_socket:
                    client_socket, client_address = sock.accept()
                    self.backlog_dict[client_socket] = list()
                    print('Client {}:{} connected.'.format(client_address[0], client_address[1]))
                else:
                    try:
                        data = sock.recv(RECV_BUFSIZE).decode().strip()
                        self.response_queue.append((sock, data))
                    except Exception as e:
                        print('[ERROR] get_slaves {}'.format(e))
                        sock.close()
                        del self.backlog_dict[sock]
        for sock in self.backlog_dict.keys():
            data = 'FIN'.encode()
            data += BYTE_SPACE * (RECV_BUFSIZE - len(data))
            sock.send(data)
            sock.close()
        self.server_socket.close()
    def get_smallest(self, d):
        return min(d.items(), key=lambda x:len(x[1]))
    def run(self):
        print('{}:{} start.'.format(self.address[0], self.address[1]))
        try:
            self.server_thread.start()
            self.handle_slave_response_thread.start()
            while not self.stop_flag:
                if self.N <= 0:
                    break
                if not self.segments:
                    continue
                if not self.backlog_dict:
                    continue
                (mi_slave, mi_backlog) = self.get_smallest(self.backlog_dict)
                if len(mi_backlog) < self.K:
                    segment = self.segments.pop()
                    read_sockets, write_sockets, error_sockets = select.select([], [mi_slave], [], 0)
                    for sock in write_sockets:
                        if sock == mi_slave:
                            if segment in self.backlog_dict[sock]:
                                continue
                            data = 'DOW;{}'.format(segment).encode()
                            data += BYTE_SPACE * (RECV_BUFSIZE - len(data))
                            sock.send(data)
                            address = sock.getpeername()
                            print('Ask {}:{} to download {}.'.format(address[0], address[1], segment))
                            self.backlog_dict[sock].append(segment)
                            break
            self.stop_flag = True
            while self.server_thread.is_alive():
                self.server_thread.join(TIMEOUT)
            while self.handle_slave_response_thread.is_alive():
                self.handle_slave_response_thread.join(TIMEOUT)
        except Exception as e:
            print('[ERROR] run {}.'.format(e))
            self.stop_flag = True
            while self.server_thread.is_alive():
                self.server_thread.join(TIMEOUT)
            while self.handle_slave_response_thread.is_alive():
                self.handle_slave_response_thread.join(TIMEOUT)

def load_m3u8(url):
    m3u8_obj = m3u8.load(url)
    file_list = [url]
    if m3u8_obj.is_variant:
        for playlist in m3u8_obj.playlists:
            sub_m3u8_obj = None
            if playlist.uri.startswith('http://'):
                file_list = load_m3u8(playlist.uri)
            else:
                sub_url = urljoin(url, playlist.uri)
                file_list = load_m3u8(sub_url)
    else:
        segments =  m3u8_obj.segments
        for segment in segments:
            duration = segment.duration
            if segment.uri.startswith('http://'):
                file_list.append(segment.uri)
            else:
                seg_url = urljoin(url, segment.uri)
                file_list.append(seg_url)
    return file_list

def set_arguments():
    parser = argparse.ArgumentParser(description='MircoDownload_master')
    parser.add_argument('--host', required=True,
                        help='host')
    parser.add_argument('--port', type=int, required=True,
                        help='port')
    parser.add_argument('--url', required=True,
                        help='m3u8 source url')
    return parser

if __name__ == '__main__':
    parser = set_arguments()
    cmd_args = parser.parse_args()

    # http://devimages.apple.com/iphone/samples/bipbop/gear1/prog_index.m3u8
    # http://www.usdi.net.tw/video/hls/Taylor_W1_S1.ts.m3u8
    # http://localhost:8000/m3u8/10.m3u8
    file_list = load_m3u8(cmd_args.url)
    print('load_m3u8 done.')
    master = Master(cmd_args.host, cmd_args.port, file_list)
    master.run()
