# -*- coding: utf-8 -*-
from __future__ import print_function
import os
import unittest
import mock

from yaml import load
from couchdb import Server

from openprocurement.auction.bridge_utils.planning_bridge import AuctionsPlanningBridge


class BaseWebTest(unittest.TestCase):
    """Base Web Test to test openprocurement.api.

    It setups the database before each test and delete it after.
    """

    def setUp(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))

        with open(dir_path + '/planning_bridge.yaml') as config_file_obj:
            config = load(config_file_obj.read())

        with mock.patch('openprocurement.auction.databridge.check_workers'):
            bridge = AuctionsPlanningBridge(config)

        self.couchdb_server = Server(config['main'].get('couch_url'))
        self.db = bridge.stream_db
        self.mapper = bridge.manager_mapper

    def tearDown(self):
        try:
            del self.couchdb_server[self.db.name]
        except:
            pass