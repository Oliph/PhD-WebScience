#!/usr/bin/env python
# encoding: utf-8


import zmq
import json
from logger import logger
from utils import cut_list

from multiprocessing import Process

logger = logger('server_connect', stream_level='INFO')


class serverClass(Process):
    """ """
    def __init__(self, *args):
        """ """
        Process.__init__(self)
        self.client_id = args[0]
        self.new_data_pipe = args[1]
        self.connect_control = args[2]
        self.host = args[3]
        self.status_port = args[4]
        self.pub_port = args[5]
        self.loop_number = 1

    def process_data(self):
        """ """
        logger.info('Set the connect_control on True')
        self.connect_control.set()
        logger.info('Wait for new data')
        data = self.new_data_pipe.get()
        logger.info('Received new data')
        try:
            self.loop_number = data['loop_number']
            del data['loop_number']
        except KeyError:
            pass
        return self.get_json(data)

    # def new_data(self, data):
    #     """ """
    #     logger.info('get into new data')
    #     data = self.new_data.get()
    #     return self.get_json(data.decode('utf-8'))

    def get_json(self, data):
        """ """
        # data = json.loads(data)
        return self.split_element(data)

    def split_element(self, data):
        """ Slice every separated bit into a list of number """
        logger.info('Split the elements')
        dict_return = dict()
        dict_result = {k: [elt for elt in cut_list(data[k], len(self.client_id))]
                       for k in data}
        for key, value in dict_result.items():
            for i, j in enumerate(value):
                dict_return.setdefault(i, {})[key] = j
        return [dict_return[elt] for elt in dict_return]

    def run(self):
        """ """
        logger.info('Start the server to listen')

        context_status = zmq.Context()
        status_sock = context_status.socket(zmq.REP)
        status_sock.bind("tcp://{}:{}".format(self.host, self.status_port))
        print(self.host, self.status_port, self.pub_port)
        context_pub = zmq.Context()
        sock_push = context_pub.socket(zmq.PUSH)
        sock_push.bind('tcp://{}:{}'.format(self.host, self.pub_port))

        client_ok = list()
        while True:
            status_message = status_sock.recv()
            status_sock.send('Wait'.encode())
            if status_message not in client_ok:
                client_ok.append(status_message)
            if len(client_ok) == len(self.client_id):
                logger.info('Nbr of client waiting ok')
                dict_result = self.process_data()
                for result in dict_result:
                    result['loop_number'] = self.loop_number
                    logger.info('Send the new data')
                    sock_push.send_json(result)
                client_ok = list()


def main():
    pass


if __name__ == '__main__':
    main()
