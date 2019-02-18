# -*- coding: utf-8 -*-
import os
import logging
import requests
from random import randint

from couchdb import ResourceConflict
from datetime import timedelta, datetime
from iso8601 import parse_date


from openprocurement.auction.bridge_utils.constants import (
    TZ,
    BIDDER_TIME,
    SERVICE_TIME,
    MIN_PAUSE,
    ROUNDING,
    WORKING_DAY_START,
    CALENDAR_ID
)
from openprocurement.auction.bridge_utils.design import plan_auctions_view


LOGGER = logging.getLogger('Openprocurement Auction')

ADAPTER = requests.adapters.HTTPAdapter(pool_connections=3, pool_maxsize=3)
SESSION = requests.Session()
SESSION.mount('http://', ADAPTER)
SESSION.mount('https://', ADAPTER)


def update_logging_context(request, params):
    if not request.__dict__.get('logging_context'):
        request.logging_context = {}

    for x, j in params.items():
        request.logging_context[x.upper()] = j


def context_unpack(request, msg, params=None):
    if params:
        update_logging_context(request, params)
    logging_context = request.logging_context
    journal_context = msg
    for key, value in logging_context.items():
        journal_context["JOURNAL_" + key] = value
    return journal_context


def get_now():
    return TZ.localize(datetime.now())


def randomize(dt):
    return dt + timedelta(seconds=randint(0, 1799))


def get_manager_for_auction(auction, mapper):
    default_manager = mapper['types'].get('english', None)

    auction_type = auction.get('auctionParameters', {}).get('type', None)
    if auction_type:
        return mapper['types'].get(auction_type, default_manager)
    else:
        pmt = auction.get('procurementMethodType')
        return mapper['pmts'].get(pmt, default_manager)


def calc_auction_end_time(bids, start):
    end = start + bids * BIDDER_TIME + SERVICE_TIME + MIN_PAUSE
    seconds = (end - TZ.localize(datetime.combine(end, WORKING_DAY_START))).seconds
    roundTo = ROUNDING.seconds
    rounding = (seconds + roundTo - 1) // roundTo * roundTo
    return (end + timedelta(0, rounding - seconds, -end.microsecond)).astimezone(TZ)


def get_calendar(db, calendar_id=CALENDAR_ID):
    return db.get(calendar_id, {'_id': calendar_id})


def skipped_days(days):
    days_str = ''
    if days:
        days_str = ' Skipped {} full days.'.format(days)
    return days_str


def planning_auction(auction, mapper, start, db, quick=False, lot_id=None):
    tid = auction.get('id', '')
    mode = auction.get('mode', '')
    manager = get_manager_for_auction(auction, mapper)
    skipped_days = 0
    if quick:
        quick_start = calc_auction_end_time(0, start)
        return quick_start, 0, skipped_days
    calendar = get_calendar(db)
    streams = manager.get_streams(db)
    start += timedelta(hours=1)
    if start.time() > manager.working_day_start:
        nextDate = start.date() + timedelta(days=1)
    else:
        nextDate = start.date()
    while True:
        # skip Saturday and Sunday
        if calendar.get(nextDate.isoformat()) or nextDate.weekday() in [5, 6]:
            nextDate += timedelta(days=1)
            continue
        dayStart, stream, plan = manager.get_date(db, mode, nextDate)
        result = manager.set_end_of_auction(stream, streams, nextDate, dayStart, plan)
        if result:
            start, end, dayStart, stream, new_slot = result
            break
        nextDate += timedelta(days=1)
        skipped_days += 1
    manager.set_date(db, plan, "_".join([tid, lot_id]) if lot_id else tid, end.time(), stream, dayStart, new_slot)
    return start, stream, skipped_days


def check_auction(auction, db, mapper):
    now = get_now()
    quick = os.environ.get('SANDBOX_MODE', False) and u'quick' in auction.get('submissionMethodDetails', '')
    if not auction.get('lots') and 'shouldStartAfter' in auction.get('auctionPeriod', {}) and auction['auctionPeriod']['shouldStartAfter'] > auction['auctionPeriod'].get('startDate'):
        period = auction.get('auctionPeriod')
        shouldStartAfter = max(parse_date(period.get('shouldStartAfter'), TZ).astimezone(TZ), now)
        planned = False
        while not planned:
            try:
                auctionPeriod, stream, skip_days = planning_auction(auction, mapper, shouldStartAfter, db, quick)
                planned = True
            except ResourceConflict:
                planned = False
        auctionPeriod = randomize(auctionPeriod).isoformat()
        planned = 'replanned' if period.get('startDate') else 'planned'
        LOGGER.info(
            '{} auction for auction {} to {}. Stream {}.{}'.format(
                planned.title(), auction['id'], auctionPeriod, stream, skipped_days(skip_days)
            ),
            extra={
                'MESSAGE_ID': '{}_auction_auction'.format(planned),
                'PLANNED_DATE': auctionPeriod,
                'PLANNED_STREAM': stream,
                'PLANNED_DAYS_SKIPPED': skip_days}
        )
        return {'auctionPeriod': {'startDate': auctionPeriod}}
    elif auction.get('lots'):
        lots = []
        for lot in auction.get('lots', []):
            if lot['status'] != 'active' or 'shouldStartAfter' not in lot.get('auctionPeriod', {}) or lot['auctionPeriod']['shouldStartAfter'] < lot['auctionPeriod'].get('startDate'):
                lots.append({})
                continue
            period = lot.get('auctionPeriod')
            shouldStartAfter = max(parse_date(period.get('shouldStartAfter'), TZ).astimezone(TZ), now)
            lot_id = lot['id']
            planned = False
            while not planned:
                try:
                    auctionPeriod, stream, skip_days = planning_auction(auction, mapper, shouldStartAfter, db, quick, lot_id)
                    planned = True
                except ResourceConflict:
                    planned = False
            auctionPeriod = randomize(auctionPeriod).isoformat()
            planned = 'replanned' if period.get('startDate') else 'planned'
            lots.append({'auctionPeriod': {'startDate': auctionPeriod}})
            LOGGER.info(
                '{} auction for lot {} of auction {} to {}. Stream {}.{}'.format(
                    planned.title(), lot_id, auction['id'], auctionPeriod, stream, skipped_days(skip_days)
                ),
                extra={
                    'MESSAGE_ID': '{}_auction_lot'.format(planned),
                    'PLANNED_DATE': auctionPeriod,
                    'PLANNED_STREAM': stream,
                    'PLANNED_DAYS_SKIPPED': skip_days,
                    'LOT_ID': lot_id
                }
            )
        if any(lots):
            return {'lots': lots}
    return None


def check_inner_auction(db, auction, mapper):
    manager = get_manager_for_auction(auction, mapper)

    auction_time = auction.get('auctionPeriod', {}).get('startDate') and \
        parse_date(auction.get('auctionPeriod', {}).get('startDate'))
    lots = dict([
        (i['id'], parse_date(i.get('auctionPeriod', {}).get('startDate')))
        for i in auction.get('lots', [])
        if i.get('auctionPeriod', {}).get('startDate')
    ])
    auc_list = [
        (x.key[1], TZ.localize(parse_date(x.value, None)), x.id)
        for x in plan_auctions_view(db, startkey=[auction['id'], None],
                                    endkey=[auction['id'], 32 * "f"])
    ]
    for key, plan_time, plan_doc in auc_list:
        if not key and (not auction_time or not
                        plan_time < auction_time < plan_time +
                        timedelta(minutes=30)):
            manager.free_slot(db, plan_doc, auction['id'], plan_time)
        elif key and (not lots.get(key) or lots.get(key) and not
                      plan_time < lots.get(key) < plan_time +
                      timedelta(minutes=30)):
            manager.free_slot(db, plan_doc, "_".join([auction['id'], key]), plan_time)
