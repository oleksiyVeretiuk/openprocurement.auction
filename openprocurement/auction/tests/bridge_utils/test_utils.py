# -*- coding: utf-8 -*-
import mock
from copy import deepcopy
from datetime import datetime, timedelta

from iso8601 import parse_date

from openprocurement.auction.bridge_utils.constants import TZ
from openprocurement.auction.bridge_utils.managers import InsiderAuctionsManager, MANAGERS_MAPPING
from openprocurement.auction.bridge_utils.utils import (
    planning_auction,
    get_manager_for_auction,
    check_auction,
    check_inner_auction
)
from openprocurement.auction.tests.bridge_utils.base import BaseWebTest
from openprocurement.auction.tests.bridge_utils.data import test_auction_data, plantest


test_auction_data_quick = deepcopy(test_auction_data)
test_auction_data_quick.update({
    "enquiryPeriod": {
        'startDate': datetime.now(TZ).isoformat(),
        "endDate": datetime.now(TZ).isoformat()
    },
    'tenderPeriod': {
        'startDate': datetime.now(TZ).isoformat(),
        "endDate": datetime.now(TZ).isoformat()
    }
})
test_auction_data_test_quick = deepcopy(test_auction_data_quick)
test_auction_data_test_quick['mode'] = 'test'


class CheckAuction(BaseWebTest):

    def test_check_aution(self):
        now = datetime.now(TZ)

        my_test_auction = deepcopy(test_auction_data)
        my_test_auction['auctionPeriod'] = {}
        my_test_auction['auctionPeriod']['startDate'] = now.isoformat()
        my_test_auction['auctionPeriod']['shouldStartAfter'] = (now + timedelta(days=10)).isoformat()
        my_test_auction['procurementMethodType'] = 'dgfInsider'

        auction_period = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIn('auctionPeriod', auction_period)
        self.assertIn('startDate', auction_period['auctionPeriod'])

    def test_check_auction_without_should_start(self):
        my_test_auction = deepcopy(test_auction_data)
        my_test_auction.pop('auctionPeriod', None)

        auction_period = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIsNone(auction_period)

    def test_check_auction_start_more_should_start(self):
        now = datetime.now(TZ)

        my_test_auction = deepcopy(test_auction_data)

        my_test_auction['auctionPeriod'] = {}
        my_test_auction['auctionPeriod']['startDate'] = now.isoformat()
        my_test_auction['auctionPeriod']['shouldStartAfter'] = (now - timedelta(days=10)).isoformat()

        auction_period = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIsNone(auction_period)

    def test_check_auction_with_lots(self):
        now = datetime.now(TZ)

        my_test_auction = deepcopy(test_auction_data)
        auction_period = {
            'startDate': now.isoformat(),
            'shouldStartAfter': (now + timedelta(days=10)).isoformat()
        }
        lot = {'status': 'active', 'auctionPeriod': auction_period, 'id': '1' * 32}
        my_test_auction['lots'] = [lot, lot]

        lots = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIn('lots', lots)
        self.assertEqual(len(lots['lots']), 2)

    def test_check_auction_with_not_active_lot(self):
        now = datetime.now(TZ)

        my_test_auction = deepcopy(test_auction_data)
        auction_period = {
            'startDate': now.isoformat(),
            'shouldStartAfter': (now + timedelta(days=10)).isoformat()
        }
        lot = {'status': 'active', 'auctionPeriod': auction_period, 'id': '1' * 32}
        not_active = deepcopy(lot)
        not_active['status'] = 'pending'
        my_test_auction['lots'] = [lot, not_active]

        lots = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIn('lots', lots)
        self.assertEqual(len(lots['lots'][1].keys()), 0)

    def test_should_start_before_auction_start(self):
        now = datetime.now(TZ)

        my_test_auction = deepcopy(test_auction_data)
        auction_period = {
            'startDate': now.isoformat(),
            'shouldStartAfter': (now + timedelta(days=10)).isoformat()
        }
        lot = {'status': 'active', 'auctionPeriod': auction_period, 'id': '1' * 32}
        should_start_before = deepcopy(lot)
        should_start_before['auctionPeriod']['startDate'] = now.isoformat()
        should_start_before['auctionPeriod']['shouldStartAfter'] = (now - timedelta(days=10)).isoformat()
        my_test_auction['lots'] = [lot, should_start_before]

        lots = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIn('lots', lots)
        self.assertEqual(len(lots['lots'][1].keys()), 0)

    def test_should_start_absent(self):
        now = datetime.now(TZ)

        my_test_auction = deepcopy(test_auction_data)
        auction_period = {
            'startDate': now.isoformat(),
            'shouldStartAfter': (now + timedelta(days=10)).isoformat()
        }
        lot = {'status': 'active', 'auctionPeriod': auction_period, 'id': '1' * 32}
        no_should_start = deepcopy(lot)
        no_should_start['auctionPeriod'].pop('shouldStartAfter')
        my_test_auction['lots'] = [lot, no_should_start]

        lots = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIn('lots', lots)
        self.assertEqual(len(lots['lots'][1].keys()), 0)

    def test_check_auction_with_invalid_lots(self):
        now = datetime.now(TZ)
        now = now.replace(year=2018, month=8, day=25)

        my_test_auction = deepcopy(test_auction_data)
        auction_period = {
            'startDate': now.isoformat(),
            'shouldStartAfter': (now + timedelta(days=10)).isoformat()
        }
        lot = {'status': 'active', 'auctionPeriod': auction_period, 'id': '1' * 32}
        not_active = deepcopy(lot)
        not_active['status'] = 'pending'
        my_test_auction['lots'] = [not_active, not_active]

        lots = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIsNone(lots)

        # should start after before auction
        my_test_auction = deepcopy(test_auction_data)
        should_start_before = deepcopy(lot)
        should_start_before['auctionPeriod']['startDate'] = now.isoformat()
        should_start_before['auctionPeriod']['shouldStartAfter'] = (now - timedelta(days=10)).isoformat()
        my_test_auction['lots'] = [should_start_before, should_start_before]

        lots = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIsNone(lots)

        # should start absent
        my_test_auction = deepcopy(test_auction_data)
        no_should_start = deepcopy(lot)
        no_should_start['auctionPeriod'].pop('shouldStartAfter')
        my_test_auction['lots'] = [no_should_start, no_should_start]

        lots = check_auction(my_test_auction, self.db, self.mapper)
        self.assertIsNone(lots)


class AuctionPlanning(BaseWebTest):

    def test_auction_quick_planning(self):
        now = datetime.now(TZ)
        auctionPeriodstartDate = planning_auction(test_auction_data_test_quick, self.mapper, now, self.db, True)[0]
        self.assertTrue(now < auctionPeriodstartDate < now + timedelta(hours=1))

    def test_auction_quick_planning_insider(self):
        now = datetime.now(TZ)
        my_test_auction = deepcopy(test_auction_data_test_quick)
        my_test_auction['procurementMethodType'] = 'dgfInsider'
        auctionPeriodstartDate = planning_auction(
            my_test_auction, self.mapper, now, self.db, True
        )[0]
        self.assertTrue(
            now < auctionPeriodstartDate < now + timedelta(hours=1)
        )

    def test_auction_planning_overlow_insider(self):
        now = datetime.now(TZ)
        my_test_auction = deepcopy(test_auction_data_test_quick)
        my_test_auction['procurementMethodType'] = 'dgfInsider'
        res = planning_auction(my_test_auction, self.mapper, now, self.db)[0]
        startDate = res.date()
        count = 0
        while startDate == res.date():
            count += 1
            res = planning_auction(my_test_auction, self.mapper, now, self.db)[0]
        self.assertEqual(count, 15)

    def test_auction_planning_overlow(self):
        now = datetime.now(TZ)
        res = planning_auction(test_auction_data_test_quick, self.mapper, now, self.db)[0]
        startDate = res.date()
        count = 0
        while startDate == res.date():
            count += 1
            res = planning_auction(test_auction_data_test_quick, self.mapper, now, self.db)[0]
        self.assertEqual(count, 100)

    def test_auction_planning_free(self):
        now = datetime.now(TZ)
        test_auction_data_test_quick.pop("id")
        res = planning_auction(test_auction_data_test_quick, self.mapper, now, self.db)[0]
        startDate, startTime = res.date(), res.time()
        manager = get_manager_for_auction(test_auction_data, self.mapper)
        manager.free_slot(self.db, "plantest_{}".format(startDate.isoformat()), "", res)
        res = planning_auction(test_auction_data_test_quick, self.mapper, now, self.db)[0]
        self.assertEqual(res.time(), startTime)

    def test_auction_planning_buffer(self):
        some_date = datetime(2015, 9, 21, 6, 30)
        date = some_date.date()
        ndate = (some_date + timedelta(days=1)).date()
        res = planning_auction(test_auction_data_test_quick, self.mapper, some_date, self.db)[0]
        self.assertEqual(res.date(), date)
        some_date = some_date.replace(hour=10)
        res = planning_auction(test_auction_data_test_quick, self.mapper, some_date, self.db)[0]
        self.assertNotEqual(res.date(), date)
        self.assertEqual(res.date(), ndate)
        some_date = some_date.replace(hour=16)
        res = planning_auction(test_auction_data_test_quick, self.mapper, some_date, self.db)[0]
        self.assertNotEqual(res.date(), date)
        self.assertEqual(res.date(), ndate)

    def test_skipping_holidays(self):
        now = datetime.now(TZ)
        now = now.replace(year=2018, month=8, day=25, hour=6)
        # Set saturday
        while now.weekday() != 5:
            now = now + timedelta(days=1)
        my_test_auction = deepcopy(test_auction_data)
        my_test_auction['procurementMethodType'] = 'dgfInsider'
        res, _, skipped_days = planning_auction(my_test_auction, self.mapper, now, self.db)
        self.assertEqual(res.date(), (now + timedelta(days=2)).date())
        self.assertEqual(skipped_days, 0)

    def test_skipping_holiday_in_calendar(self):
        now = datetime.now(TZ)

        # Holiday in 2018 year
        now = now.replace(year=2018, month=12, day=25, hour=6)
        self.assertNotIn(now.weekday(), [5, 6])

        my_test_auction = deepcopy(test_auction_data)
        my_test_auction['procurementMethodType'] = 'dgfInsider'
        res, _, skipped_days = planning_auction(my_test_auction, self.mapper, now, self.db)
        self.assertEqual(res.date(), (now + timedelta(days=1)).date())
        self.assertEqual(skipped_days, 0)

    def test_skipping_day_if_time_after_working_day_start(self):
        now = datetime.now(TZ)
        now = now.replace(year=2018, month=8, day=25, hour=12)

        while now.weekday() != 1:
            now = now + timedelta(days=1)

        my_test_auction = deepcopy(test_auction_data)
        my_test_auction['procurementMethodType'] = 'dgfInsider'
        res, _, skipped_days = planning_auction(my_test_auction, self.mapper, now, self.db)
        self.assertEqual(res.date(), (now + timedelta(days=1)).date())
        self.assertEqual(skipped_days, 0)

    def test_result_set_end_none(self):
        my_test_auction = deepcopy(test_auction_data)

        now = datetime.now(TZ)
        now = now.replace(year=2018, month=8, day=25, hour=6)

        # Set friday
        while now.weekday() != 1:
            now = now + timedelta(days=1)

        # set_end_of_auction return none once
        with mock.patch('openprocurement.auction.bridge_utils.utils.get_manager_for_auction') as get_manager_mock:
            mocked_manager = mock.MagicMock()
            mocked_manager.working_day_start = InsiderAuctionsManager.working_day_start

            mocked_manager.get_date.side_effect = [
                ('1', '2', '3'),
                ('1', '2', '3'),
                ('1', '2', '3'),
            ]
            mocked_manager.set_end_of_auction.side_effect = [
                None,
                None,
                ('1', mock.MagicMock(), '3', '4', '5')
            ]

            get_manager_mock.return_value = mocked_manager

            start, _, skipped_days = planning_auction(my_test_auction, self.mapper, now, self.db)

            self.assertEqual(skipped_days, 2)


class TestCheckInnerAuction(BaseWebTest):

    def setUp(self):
        super(TestCheckInnerAuction, self).setUp()
        plantest['_id'] = 'plantest_{}'.format(
            datetime.now().date().isoformat())
        plantest_from_db = self.db.get(plantest['_id'], {})
        plantest_from_db.update(plantest)

        self.db.save(plantest_from_db)

    def test_check_inner_auction(self):
        insider_auction_id = '01fa8a7dc4b8eac3b5820747efc6fe36'
        texas_auction_id = 'dc3d950743304d05adaa1cd5b0475075'
        classic_auction_with_lots = 'da8a28ed2bdf73ee1d373e4cadfed4c5'
        classic_auction_without_lots = 'e51508cddc2c490005eaecb73c006b72'
        lots_ids = ['1c2fb1e496b317b2b87e197e2332da77',
                    'b10f9f7f26157ae2f349be8dc2106d6e']

        today = datetime.now().date().isoformat()
        time = '12:15:00'  # actually, can be any time between 12:00:00 and 12:30:00 due to existing asserts
        raw_time = ''.join([today, 'T', time])

        # datetime.datetime object prepared in the way scheduler actually does it:
        test_time = TZ.localize(parse_date(raw_time, None)).isoformat()

        auction = {
            'id': insider_auction_id,
            'procurementMethodType': 'dgfInsider',
            'auctionPeriod': {
                'startDate': test_time
            }
        }
        mapper = {
            'pmts': {
                'dgfInsider': MANAGERS_MAPPING['dutch'](),
                'landLease': MANAGERS_MAPPING['texas']()
            },
            'types': {'english': MANAGERS_MAPPING['english']()}
        }

        plantest = self.db.get('plantest_{}'.format(today))

        # Test dutch
        self.assertEqual(len(plantest.get('dutch_streams', [])), 15)
        self.assertIn(insider_auction_id, plantest.get('dutch_streams'))

        check_inner_auction(self.db, auction, mapper)
        plantest = self.db.get('plantest_{}'.format(today))
        self.assertEqual(len(plantest.get('dutch_streams', [])), 6)
        self.assertNotIn(insider_auction_id, plantest.get('dutch_streams'))

        # Test texas
        auction['id'] = texas_auction_id
        auction['procurementMethodType'] = 'landLease'

        self.assertEqual(len(plantest.get('texas_streams', [])), 20)
        self.assertIn(texas_auction_id, plantest.get('texas_streams'))

        check_inner_auction(self.db, auction, mapper)
        plantest = self.db.get('plantest_{}'.format(today))
        self.assertEqual(len(plantest.get('texas_streams', [])), 15)
        self.assertNotIn(texas_auction_id, plantest.get('texas_streams'))

        # Test classic with lots
        auction['procurementMethodType'] = 'classic'
        auction['id'] = classic_auction_with_lots
        auction['lots'] = [
            {
                'id': lots_ids[0],
                'auctionPeriod': {'startDate': test_time}
            },
            {
                'id': lots_ids[1],
                'auctionPeriod': {'startDate': test_time}
            }
        ]
        self.assertEqual(len(plantest.get('stream_1')), 10)
        self.assertEqual(len(plantest.get('stream_2')), 10)
        stream_1_none_count = len(
            [v for k, v in plantest.get('stream_1').items() if v is None])
        stream_2_none_count = len(
            [v for k, v in plantest.get('stream_2').items() if v is None])
        self.assertEqual(stream_1_none_count, 0)
        self.assertEqual(stream_2_none_count, 0)
        check_inner_auction(self.db, auction, mapper)
        plantest = self.db.get('plantest_{}'.format(today))
        self.assertEqual(len(plantest.get('stream_1')), 10)
        self.assertEqual(len(plantest.get('stream_2')), 10)
        stream_1_none_count = len(
            [v for k, v in plantest.get('stream_1').items() if v is None])
        stream_2_none_count = len(
            [v for k, v in plantest.get('stream_2').items() if v is None])
        self.assertEqual(stream_1_none_count, 3)
        self.assertEqual(stream_2_none_count, 3)
        self.assertNotIn(classic_auction_with_lots,
                         plantest.get('stream_1', {}).values())
        self.assertNotIn(classic_auction_with_lots,
                         plantest.get('stream_2', {}).values())

        # Test classic without lots
        del auction['lots']
        auction['id'] = classic_auction_without_lots
        check_inner_auction(self.db, auction, mapper)
        plantest = self.db.get('plantest_{}'.format(today))
        self.assertEqual(len(plantest.get('stream_1')), 10)
        self.assertEqual(len(plantest.get('stream_2')), 10)
        stream_1_none_count = len(
            [v for k, v in plantest.get('stream_1').items() if v is None])
        stream_2_none_count = len(
            [v for k, v in plantest.get('stream_2').items() if v is None])
        self.assertEqual(stream_1_none_count, 7)
        self.assertEqual(stream_2_none_count, 6)
        self.assertNotIn(classic_auction_without_lots,
                         plantest.get('stream_1', {}).values())
        self.assertNotIn(classic_auction_without_lots,
                         plantest.get('stream_2', {}).values())
