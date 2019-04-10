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
from zope.interface import implementer
from yaml import load
from couchdb import Database, Session, Server
from dateutil.tz import tzlocal
from openprocurement_client.sync import ResourceFeeder
from openprocurement.auction.interfaces import \
    IAuctionDatabridge, IAuctionsManager

from openprocurement.auction.core import components
from openprocurement.auction.utils import FeedItem, check_workers

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


@implementer(IAuctionDatabridge)
class AuctionsDataBridge(object):
    """Auctions Data Bridge"""

    def __init__(self, config, re_planning=False, debug=False):
        super(AuctionsDataBridge, self).__init__()
        self.config = config
        self.resource_ids_list = []
        self.tz = tzlocal()
        self.debug = debug
        self.mapper = components.qA(self, IAuctionsManager)
        check_workers(self.mapper.plugins)
        self.re_planning = re_planning
        DEFAULT_RETRIEVERS_PARAMS.update(
            self.config.get('main').get('retrievers_params', {}))
        self.couch_url = urljoin(
            self.config_get('couch_url'),
            self.config_get('auctions_db')
        )
        self.db = Database(self.couch_url,
                           session=Session(retry_delays=range(10)))

        self.feeder = ResourceFeeder(
            host=self.config_get('resource_api_server'),
            resource=self.config_get('resource_name'),
            version=self.config_get('resource_api_version'), key='',
            extra_params=API_EXTRA,
            retrievers_params=DEFAULT_RETRIEVERS_PARAMS
        )

    def config_get(self, name):
        return self.config.get('main').get(name)

    def run(self):
        if self.re_planning:
            self.run_re_planning()
            return

        for item in self.feeder.get_resource_items():
            # magic goes here
            feed = FeedItem(item)
            planning = self.mapper(feed)
            if not planning:
                continue

            for cmd, item_id, lot_id in planning:
                if lot_id:
                    LOGGER.info('Lot {} of tender {} selected for {}'.format(
                        lot_id, item_id, cmd))
                else:
                    LOGGER.info('Tender {} selected for {}'.format(item_id,
                                                                   cmd))
                planning(cmd, item_id, lot_id=lot_id)

    def run_re_planning(self):
        pass
        # self.re_planning = True
        # self.offset = ''
        # LOGGER.info('Start Auctions Bridge for re-planning...',
        #             extra={
        #                 'MESSAGE_ID':
        #                     DATA_BRIDGE_RE_PLANNING_START_BRIDGE
        #             })
        # for tender_item in self.get_teders_list(re_planning=True):
        #     LOGGER.debug('Tender {} selected for re-planning'.format(
        #         tender_item))
        #     for planning_data in self.get_teders_list():
        #         if len(planning_data) == 1:
        #             LOGGER.info('Tender {0} selected for planning'.format(
        #                 *planning_data))
        #             self.start_auction_worker_cmd('planning',
        #                                           planning_data[0])
        #         elif len(planning_data) == 2:
        #             LOGGER.info(
        #                 'Lot {1} of tender {0} selected for planning'.format(
        #                     *planning_data))
        #             self.start_auction_worker_cmd('planning',
        #                                           planning_data[0],
        #                                           lot_id=planning_data[1])
        #         self.tenders_ids_list.append(tender_item['id'])
        #     sleep(1)
        # LOGGER.info("Re-planning auctions finished",
        #             extra={'MESSAGE_ID': DATA_BRIDGE_RE_PLANNING_FINISHED})


def main():
    parser = argparse.ArgumentParser(description='---- Auctions Bridge ----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    parser.add_argument(
        '--re-planning', action='store_true', default=False,
        help='Not ignore auctions which already scheduled')
    parser.add_argument('-t', dest='check', action='store_const',
                        const=True, default=False,
                        help='Workers check only')
    params = parser.parse_args()
    if os.path.isfile(params.config):
        with open(params.config) as config_file_obj:
            config = load(config_file_obj.read())
        logging.config.dictConfig(config)
        bridge = AuctionsDataBridge(config, re_planning=params.re_planning)
        if params.check:
            exit()
        bridge.run()


if __name__ == "__main__":
    main()
