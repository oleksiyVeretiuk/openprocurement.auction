import logging

from urlparse import urljoin
from json import dumps

from openprocurement.auction.bridge_utils.utils import (
    check_auction,
    context_unpack,
    SESSION,
    check_inner_auction
)

LOGGER = logging.getLogger('Openprocurement Auction')


class AuctionPeriodPatcher(object):

    def __init__(self, config, stream_db, manager_mapper):
        self.config = config.get('main', {})
        self.stream_db = stream_db
        self.manager_mapper = manager_mapper

    def add_auction_period(self, auction):
        url = urljoin(
            self.config.get('resource_api_server'),
            '/'.join(
                [
                    'api',
                    self.config.get('resource_api_version'),
                    self.config.get('resource_name'),
                    auction.id
                ],
            )
        )
        LOGGER.info(url)
        api_token = self.config.get('resource_api_token')
        db = self.stream_db
        changes = check_auction(auction, db, self.manager_mapper)
        if changes:
            data = dumps({'data': changes})
            r = SESSION.patch(url,
                              data=data,
                              headers={'Content-Type': 'application/json'},
                              auth=(api_token, ''))
            if r.status_code != 200:
                LOGGER.error(
                    "Error {} on updating auction '{}' with '{}': {}".format(r.status_code, url, data, r.text),
                    extra=context_unpack(r, {'MESSAGE_ID': 'error_patch_auction'},
                                         {'ERROR_STATUS': r.status_code}))
            else:
                LOGGER.info("Successfully updated auction '{}' with '{}'".format(r.status_code, url, data))


class SlotProcessing(object):

    def __init__(self, stream_db, manager_mapper):
        self.stream_db = stream_db
        self.manager_mapper = manager_mapper

    def check_to_free_slot(self, auction):
        check_inner_auction(self.stream_db, auction, self.manager_mapper)
