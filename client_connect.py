#!/usr/bin/env python
# encoding: utf-8

import zmq
import time
from multiprocessing import Process

from logger import logger
logger = logger('client_connect', stream_level='INFO')


class clientConnection(Process):
    """ """
    def __init__(self, connect_control, connect_info, client_id,
                 host='localhost', status_port=3301, pull_port=3302):
        """ """
        Process.__init__(self)
        self.connect_control = connect_control
        self.connect_info = connect_info
        self.client_id = client_id
        self.host = host
        self.status_port = status_port
        self.pull_port = pull_port

    def run(self):
        """ """
        context = zmq.Context()
        sock_status = context.socket(zmq.REQ)
        sock_status.connect('tcp://{}:{}'.format(self.host, self.status_port))

        context_sub = zmq.Context()
        sock_pull = context_sub.socket(zmq.PULL)
        sock_pull.connect('tcp://{}:{}'.format(self.host, self.pull_port))
        while True:
            logger.info('Sending message')
            sock_status.send(self.client_id.encode('utf-8'))
            msg = sock_status.recv()
            msg = msg.decode('utf-8')
            if msg == 'too early':
                print('DASAdadsA')
                time.sleep(2)
                pass
            else:
                logger.info('received {}'.format(msg))
                logger.info('wait for the data')
                data = sock_pull.recv_json()
                logger.info('Received data: loop_number {} \
                            - len lvl1 {} \
                            - len lvl2 {} \
                            - len lvl3 {}'.format(data['loop_number'],
                                                len(data['lvl1']),
                                                len(data['lvl2']),
                                                len(data['lvl3'])))
                self.process_data(data)
                self.connect_control.clear()
                self.connect_control.wait()

    def process_data(self, data):
        """ """
        # data = data.decode('utf-8')
        self.connect_info.put(data)


def main():
    client = clientConnection()
    client.conn_server()


if __name__ == '__main__':
    main()
