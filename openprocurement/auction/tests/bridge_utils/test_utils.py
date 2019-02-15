# -*- coding: utf-8 -*-
import mock
from copy import deepcopy
from datetime import datetime, timedelta


from openprocurement.auction.bridge_utils.constants import TZ
from openprocurement.auction.bridge_utils.managers import InsiderAuctionsManager
from openprocurement.auction.bridge_utils.utils import (
    planning_auction,
    get_manager_for_auction,
    check_auction
)
from openprocurement.auction.tests.bridge_utils.base import BaseWebTest
from openprocurement.auction.tests.bridge_utils.data import test_auction_data


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
