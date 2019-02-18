import logging
import iso8601
from json import dumps
from urlparse import urljoin

from datetime import datetime, timedelta, time
from time import mktime, time
from gevent.subprocess import check_call

from openprocurement.auction.systemd_msgs_ids import (
    DATA_BRIDGE_PLANNING_TENDER_SKIP,
    DATA_BRIDGE_PLANNING_TENDER_ALREADY_PLANNED,
    DATA_BRIDGE_PLANNING_LOT_SKIP,
    DATA_BRIDGE_PLANNING_LOT_ALREADY_PLANNED,
    DATA_BRIDGE_RE_PLANNING_TENDER_ALREADY_PLANNED,
    DATA_BRIDGE_RE_PLANNING_LOT_ALREADY_PLANNED,
)
from openprocurement.auction.design import (
    endDate_view,
    startDate_view,
    PreAnnounce_view
)
from openprocurement.auction.utils import (
    do_until_success, prepare_auction_worker_cmd
)
from openprocurement.auction.bridge_utils.utils import (
    check_auction,
    context_unpack,
    SESSION,
    check_inner_auction
)


SIMPLE_AUCTION_TYPE = 0
SINGLE_LOT_AUCTION_TYPE = 1
MULTILOT_AUCTION_ID = "{0[id]}_{1[id]}"  # {TENDER_ID}_{LOT_ID}
LOGGER = logging.getLogger('Openprocurement Auction')


class ClassicAuctionPlanning(object):

    def __init__(self, bridge, item):
        self.bridge = bridge
        self.item = item

    def next(self):
        return self

    def add_auction_period(self):
        url = urljoin(
            self.bridge.config_get('resource_api_server'),
            '/'.join(
                [
                    'api',
                    self.bridge.config_get('resource_api_version'),
                    self.bridge.config_get('resource_name'),
                    self.item.id
                ],
            )
        )
        LOGGER.info(url)
        api_token = self.bridge.config_get('resource_api_token')
        db = self.bridge.stream_db
        changes = check_auction(self.item, db, self.bridge.manager_mapper)
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

    def check_to_free_slot(self):
        check_inner_auction(self.bridge.stream_db, self.item, self.bridge.manager_mapper)

    def __iter__(self):
        if self.item['status'] == "active.auction":
            if 'lots' not in self.item and 'auctionPeriod' in self.item \
                    and 'startDate' in self.item['auctionPeriod'] \
                    and 'endDate' not in self.item['auctionPeriod']:

                start_date = iso8601.parse_date(
                    self.item['auctionPeriod']['startDate'])
                start_date = start_date.astimezone(self.bridge.tz)
                auctions_start_in_date = startDate_view(
                    self.bridge.db,
                    key=(mktime(start_date.timetuple()) +
                         start_date.microsecond / 1E6) * 1000
                )
                if datetime.now(self.bridge.tz) > start_date:
                    LOGGER.info(
                        "Tender {} start date in past. "
                        "Skip it for planning".format(self.item['id']),
                        extra={'MESSAGE_ID': DATA_BRIDGE_PLANNING_TENDER_SKIP})
                    raise StopIteration
                if self.bridge.re_planning  \
                        and self.item['id'] in self.tenders_ids_list:
                    LOGGER.info(
                        "Tender {} already planned while replanning".format(
                            self.item['id']),
                        extra={
                            'MESSAGE_ID':
                                DATA_BRIDGE_RE_PLANNING_TENDER_ALREADY_PLANNED
                        }
                    )
                    raise StopIteration
                elif not self.bridge.re_planning and \
                        [row.id for row in auctions_start_in_date.rows
                         if row.id == self.item['id']]:
                    LOGGER.info(
                        "Tender {} already planned on same date".format(
                            self.item['id']),
                        extra={
                            'MESSAGE_ID':
                                DATA_BRIDGE_PLANNING_TENDER_ALREADY_PLANNED})
                    raise StopIteration
                yield ("planning", str(self.item['id']), "")
            elif 'lots' in self.item:
                for lot in self.item['lots']:
                    if lot["status"] == "active" and 'auctionPeriod' in lot \
                            and 'startDate' in lot['auctionPeriod'] \
                            and 'endDate' not in lot['auctionPeriod']:
                        start_date = iso8601.parse_date(
                            lot['auctionPeriod']['startDate'])
                        start_date = start_date.astimezone(self.bridge.tz)
                        auctions_start_in_date = startDate_view(
                            self.bridge.db,
                            key=(mktime(start_date.timetuple()) +
                                 start_date.microsecond / 1E6) * 1000
                        )
                        if datetime.now(self.bridge.tz) > start_date:
                            LOGGER.info(
                                "Start date for lot {} in tender {} "
                                "is in past. Skip it for planning".format(
                                    lot['id'], self.item['id']),
                                extra={
                                    'MESSAGE_ID':
                                        DATA_BRIDGE_PLANNING_LOT_SKIP
                                }
                            )
                            raise StopIteration
                        auction_id = MULTILOT_AUCTION_ID.format(self.item, lot)
                        if self.bridge.re_planning \
                                and auction_id in self.tenders_ids_list:
                            extra = {
                                'MESSAGE_ID':
                                    DATA_BRIDGE_RE_PLANNING_LOT_ALREADY_PLANNED
                            }
                            LOGGER.info(
                                "Tender {} already planned while "
                                "replanning".format(auction_id), extra=extra)
                            raise StopIteration
                        elif not self.bridge.re_planning and \
                                [row.id for row in auctions_start_in_date.rows
                                 if row.id == auction_id]:
                            extra = {
                                'MESSAGE_ID':
                                    DATA_BRIDGE_PLANNING_LOT_ALREADY_PLANNED
                            }
                            LOGGER.info(
                                "Tender {} already planned on same "
                                "date".format(auction_id), extra=extra)
                            raise StopIteration
                        yield ("planning", str(self.item["id"]),
                               str(lot["id"]))
        if self.item['status'] == "active.qualification" \
                and 'lots' in self.item:
            for lot in self.item['lots']:
                if lot["status"] == "active":
                    is_pre_announce = PreAnnounce_view(self.bridge.db)
                    auction_id = MULTILOT_AUCTION_ID.format(self.item, lot)
                    if [row.id for row in is_pre_announce.rows
                            if row.id == auction_id]:
                        yield ('announce', self.item['id'], lot['id'])
            raise StopIteration
        if self.item['status'] == "cancelled":
            future_auctions = endDate_view(
                self.bridge.db, startkey=time() * 1000
            )
            if 'lots' in self.item:
                for lot in self.item['lots']:
                    auction_id = MULTILOT_AUCTION_ID.format(self.item, lot)
                    if auction_id in [i.id for i in future_auctions]:
                        LOGGER.info(
                            'Tender {0} selected for cancellation'.format(
                                self.item['id']))
                        yield ('cancel', self.item['id'], lot['id'])
                raise StopIteration
            else:
                if self.item["id"] in [i.id for i in future_auctions]:
                    LOGGER.info('Tender {0} selected for cancellation'.format(
                        self.item['id']))
                    yield ('cancel', self.item['id'], "")
                raise StopIteration
        raise StopIteration

    def __repr__(self):
        return "<Auction planning: {}>".format(
            self.item.get('procurementMethodType'))

    __str__ = __repr__

    def __call__(self, cmd, tender_id, with_api_version=None, lot_id=None):
        params = prepare_auction_worker_cmd(
            self.bridge,
            tender_id,
            cmd,
            self.item,
            lot_id=lot_id,
            with_api_version=with_api_version
        )
        result = do_until_success(
            check_call,
            args=(params,),
        )

        LOGGER.info("Auction command {} result: {}".format(params[1], result))


class NonClassicAuctionPlanning(ClassicAuctionPlanning):
    ready_to_plan_statuses = None

    def __iter__(self):
        if self.item['status'] in self.ready_to_plan_statuses:
            if 'auctionPeriod' in self.item \
                    and 'startDate' in self.item['auctionPeriod'] \
                    and 'endDate' not in self.item['auctionPeriod']:

                start_date = iso8601.parse_date(
                    self.item['auctionPeriod']['startDate'])
                start_date = start_date.astimezone(self.bridge.tz)
                auctions_start_in_date = startDate_view(
                    self.bridge.db,
                    key=(mktime(start_date.timetuple()) +
                         start_date.microsecond / 1E6) * 1000
                )
                if datetime.now(self.bridge.tz) > start_date:
                    LOGGER.info(
                        "Auction {} start date in past. "
                        "Skip it for planning".format(self.item['id']),
                        extra={
                            'MESSAGE_ID': DATA_BRIDGE_PLANNING_TENDER_SKIP
                        }
                    )
                    raise StopIteration
                elif not self.bridge.re_planning and \
                        [row.id for row in auctions_start_in_date.rows
                         if row.id == self.item['id']]:
                    LOGGER.info(
                        "Auction {} already planned on same date".format(self.item['id']),
                        extra={
                            'MESSAGE_ID': DATA_BRIDGE_PLANNING_TENDER_ALREADY_PLANNED
                        }
                    )
                    raise StopIteration
                yield ("planning", str(self.item['id']), "")
        if self.item['status'] == "cancelled":
            future_auctions = endDate_view(
                self.bridge.db, startkey=time() * 1000
            )
            if self.item["id"] in [i.id for i in future_auctions]:
                LOGGER.info('Auction {0} selected for cancellation'.format(
                    self.item['id']))
                yield ('cancel', self.item['id'], "")
        raise StopIteration
