from gevent import monkey

monkey.patch_all()

try:
    import urllib3.contrib.pyopenssl

    urllib3.contrib.pyopenssl.inject_into_urllib3()
except ImportError:
    pass

import logging
import logging.config
import os
import argparse
from urlparse import urljoin
from yaml import load
from munch import Munch
from couchdb import Database, Session, Server
from openprocurement_client.sync import ResourceFeeder
from openprocurement.auction.bridge_utils.design import sync_design
from openprocurement.auction.bridge_utils.managers import MANAGERS_MAPPING
from openprocurement.auction.bridge_utils.constants import WORKING_DAYS, CALENDAR_ID, STREAMS_ID
from openprocurement.auction.bridge_utils.core import SlotProcessing, AuctionPeriodPatcher


LOGGER = logging.getLogger(__name__)
API_EXTRA = {'opt_fields': 'status,auctionPeriod,lots,procurementMethodType',
             'mode': '_all_'}

DEFAULT_RETRIEVERS_PARAMS = {
    'down_requests_sleep': 1,
    'up_requests_sleep': 1,
    'up_wait_sleep': 30,
    'up_wait_sleep_min': 5,
    'queue_size': 501
}


class AuctionsPlanningBridge(object):
    """Auctions Planning Bridge"""

    def __init__(self, config, debug=False):
        super(AuctionsPlanningBridge, self).__init__()
        self.config = config
        self.debug = debug

        self.feeder = ResourceFeeder(
            host=self.config_get('resource_api_server'),
            resource=self.config_get('resource_name'),
            version=self.config_get('resource_api_version'), key='',
            extra_params=API_EXTRA,
            retrievers_params=DEFAULT_RETRIEVERS_PARAMS
        )
        # Stream DB configurations

        couch_server = Server(self.config_get('couch_url'), session=Session(retry_delays=range(60)))
        db_name = os.environ.get('DB_NAME', self.config['main']['stream_db'])

        if db_name not in couch_server:
            couch_server.create(db_name)

        db_for_streams = urljoin(
            self.config_get('couch_url'),
            db_name
        )

        self.stream_db = Database(db_for_streams, session=Session(retry_delays=range(10)))
        self._set_holidays()
        self._set_streams_limits()
        sync_design(self.stream_db)

        # Managers Mapping
        self.manager_mapper = {'types': {}, 'pmts': {}}
        for name, plugin in self.config_get('plugins').items():
            auction_manager = MANAGERS_MAPPING[name]()
            self.manager_mapper['types'][name] = auction_manager
            if plugin.get('procurement_method_types', []):
                self.manager_mapper['pmts'].update({pmt: auction_manager for pmt in plugin.get('procurement_method_types')})

        self.patcher = AuctionPeriodPatcher(self.config, self.stream_db, self.manager_mapper)
        self.slot_processing = SlotProcessing(self.stream_db, self.manager_mapper)

    def _set_holidays(self):
        calendar = {'_id': CALENDAR_ID}
        calendar.update(WORKING_DAYS)
        if CALENDAR_ID in self.stream_db:
            del self.stream_db[CALENDAR_ID]
        self.stream_db.save(calendar)

    def _set_streams_limits(self):
        streams = self.config.get('main').get('streams', {})

        stream_amount = {'_id': STREAMS_ID}
        stream_amount.update(streams)
        if STREAMS_ID in self.stream_db:
            del self.stream_db[STREAMS_ID]
        self.stream_db.save(stream_amount)

    def config_get(self, name):
        return self.config.get('main').get(name)

    def run(self):

        for item in self.feeder.get_resource_items():
            # no magic goes here
            feed = Munch(item)

            self.patcher.add_auction_period(feed)
            self.slot_processing.check_to_free_slot(feed)


def main():
    parser = argparse.ArgumentParser(description='---- Auctions Planning Bridge ----')
    parser.add_argument('config', type=str, help='Path to configuration file')

    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
        logging.config.dictConfig(config)
        bridge = AuctionsPlanningBridge(config)
        bridge.run()


if __name__ == "__main__":
    main()
