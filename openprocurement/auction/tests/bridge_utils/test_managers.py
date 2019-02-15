# -*- coding: utf-8 -*-
import mock
import unittest

from copy import deepcopy
from datetime import datetime, timedelta, time
from iso8601 import parse_date

from openprocurement.auction.bridge_utils.constants import TZ
from openprocurement.auction.bridge_utils.managers import (
    find_free_slot,
    ClassicAuctionsManager,
    InsiderAuctionsManager,
    TexasAuctionsManager
)
from openprocurement.auction.tests.bridge_utils.base import BaseWebTest
from openprocurement.auction.tests.bridge_utils.data import plantest


class FindFreeSlotTest(BaseWebTest):

    def test_find_free_slot(self):
        plan = deepcopy(plantest)
        plan['streams'] = 1
        plan['stream_1']['12:00:00'] = None
        plan_date, cur_stream = find_free_slot(plan)

        expected_date = parse_date('2017-10-03T12:00:00', None)
        expected_date = expected_date.astimezone(TZ) if expected_date.tzinfo else TZ.localize(expected_date)

        self.assertEqual(plan_date, expected_date)
        self.assertEqual(cur_stream, 1)

    def test_no_slot_founded(self):
        plan = deepcopy(plantest)
        plan['streams'] = 1
        plan['stream_1']['12:00:00'] = None
        result = find_free_slot(plantest)
        self.assertIsNone(result)


class TestClassicManager(BaseWebTest):

    def setUp(self):
        super(TestClassicManager, self).setUp()
        self.manager = ClassicAuctionsManager()

    def test_get_date(self):
        date = datetime.now().date()
        plan_date = parse_date(date.isoformat() + 'T' + self.manager.working_day_start.isoformat(), None)
        plan_date = plan_date.astimezone(TZ) if plan_date.tzinfo else TZ.localize(plan_date)

        db = mock.MagicMock()

        db.get.return_value = plantest
        now = datetime.now()

        plan_time, stream, plan = self.manager.get_date(db, '', now.date())

        self.assertEqual(plan, plantest)
        self.assertEqual(plan_time, plan_date.time())
        self.assertEqual(stream, plantest['streams'])

    def test_get_hours_and_stream(self):
        plan_date_end, streams = self.manager._get_hours_and_stream(plantest)
        self.assertEqual(plan_date_end, self.manager.working_day_start.isoformat())
        self.assertEqual(streams, plantest['streams'])

    def test_set_date(self):
        db = mock.MagicMock()
        old_plan = {'time': 'old', 'streams': 3}
        plan = deepcopy(old_plan)
        auction_id = '1' * 32
        end_time = datetime.now()
        cur_stream = 1
        start_time = datetime.now()
        stream_id = 'stream_{}'.format(cur_stream)

        self.manager.set_date(
            db, plan, auction_id, end_time,
            cur_stream, start_time, False
        )
        self.assertIn(stream_id, plan)
        stream = plan[stream_id]
        self.assertIn(start_time.isoformat(), stream)
        self.assertEqual(stream[start_time.isoformat()], auction_id)
        self.assertEqual(plan['time'], old_plan['time'])
        self.assertEqual(plan['streams'], old_plan['streams'])

        db.save.assert_called_with(plan)

    def test_date_with_new_slot(self):
        db = mock.MagicMock()
        old_plan = {'time': 'old', 'streams': 3}
        plan = deepcopy(old_plan)
        auction_id = '1' * 32
        end_time = datetime.now() + timedelta(days=1)
        cur_stream = 1
        start_time = datetime.now()
        stream_id = 'stream_{}'.format(cur_stream)

        self.manager.set_date(
            db, plan, auction_id, end_time,
            cur_stream, start_time, True
        )
        self.assertIn(stream_id, plan)
        stream = plan[stream_id]
        self.assertIn(start_time.isoformat(), stream)
        self.assertEqual(stream[start_time.isoformat()], auction_id)
        self.assertEqual(plan['time'], end_time.isoformat())
        self.assertEqual(plan['streams'], cur_stream)

        db.save.assert_called_with(plan)

    def test_free_slot(self):
        db = mock.MagicMock()
        auction_id = '1' * 32
        plan_time = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)

        plan = {
            'stream_1': {
                plan_time.time().isoformat(): auction_id
            },
            'streams': 1
        }
        plan_id = 'plan_id'
        db.get.return_value = plan

        self.manager.free_slot(db, plan_id, auction_id, plan_time)

        self.assertIsNone(plan['stream_1'][plan_time.time().isoformat()])
        db.get.assert_called_with(plan_id)
        db.save.assert_called_with(plan)

    def test_free_slot_for_wrong_auction_id(self):
        db = mock.MagicMock()
        auction_id = '1' * 32
        plan_time = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)
        wrong_auction_id = '2' * 32

        plan = {
            'stream_1': {
                plan_time.time().isoformat(): auction_id
            },
            'streams': 1
        }
        plan_id = 'plan_id'
        db.get.return_value = plan

        self.manager.free_slot(db, plan_id, wrong_auction_id, plan_time)

        self.assertEqual(plan['stream_1'][plan_time.time().isoformat()], auction_id)
        db.get.assert_called_with(plan_id)
        db.save.assert_called_with(plan)

    def test_set_end_of_auction_with_free_slot(self):
        stream = 1
        streams = 3
        next_date = datetime.now()
        day_start = time(10, 0)

        plan_time = datetime.now().replace(hour=12, minute=0, second=0, microsecond=0)
        plan = {
            '_id': 'plantest_2017-10-03',
            'stream_1': {
                plan_time.time().isoformat(): None
            },
            'stream_2': {},
            'stream_3': {},
            'streams': 3
        }
        expected_date = parse_date('2017-10-03T12:00:00', None)
        expected_date = expected_date.astimezone(TZ) if expected_date.tzinfo else TZ.localize(expected_date)

        start, end, auction_day_start, auction_stream, new_slot = self.manager.set_end_of_auction(
                                                    stream, streams, next_date, day_start, plan
                                                    )
        self.assertEqual(start, expected_date)
        self.assertEqual(end, expected_date)
        self.assertEqual(auction_day_start, expected_date.time())
        self.assertEqual(auction_stream, stream)
        self.assertFalse(new_slot)

    def test_set_end_of_auction_without_free_slot(self):
        stream = 1
        streams = 3
        next_date = datetime.now().date()
        day_start = time(8, 0)

        plan = {
            '_id': 'plantest_2017-10-03',
            'stream_1': {},
            'stream_2': {},
            'stream_3': {},
            'streams': 3
        }

        start, end, auction_day_start, auction_stream, new_slot = self.manager.set_end_of_auction(
                                                    stream, streams, next_date, day_start, plan
                                                    )

        expected_start = TZ.localize(datetime.combine(next_date, day_start))

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_start + timedelta(minutes=30))
        self.assertEqual(auction_day_start, day_start)
        self.assertEqual(auction_stream, stream)
        self.assertTrue(new_slot)

    def test_set_end_of_auction_without_free_slot_and_wrong_day_start(self):
        stream = 1
        streams = 3
        next_date = datetime.now().date()
        day_start = time(16, 0)

        plan = {
            '_id': 'plantest_2017-10-03',
            'stream_1': {},
            'stream_2': {},
            'stream_3': {},
            'streams': 3
        }

        start, end, auction_day_start, auction_stream, new_slot = self.manager.set_end_of_auction(
            stream, streams, next_date, day_start, plan
        )

        expected_start = TZ.localize(datetime.combine(next_date, self.manager.working_day_start))

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_start + timedelta(minutes=30))
        self.assertEqual(auction_day_start, self.manager.working_day_start)
        self.assertEqual(auction_stream, stream + 1)
        self.assertTrue(new_slot)

    def test_set_end_of_auction_without_free_slot_and_limit_stream(self):
        stream = 3
        streams = 3
        next_date = datetime.now().date()
        day_start = time(8, 0)

        plan = {
            '_id': 'plantest_2017-10-03',
            'stream_1': {},
            'stream_2': {},
            'stream_3': {},
            'streams': 3
        }

        start, end, auction_day_start, auction_stream, new_slot = self.manager.set_end_of_auction(
            stream, streams, next_date, day_start, plan
        )

        expected_start = TZ.localize(datetime.combine(next_date, day_start))

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_start + timedelta(minutes=30))
        self.assertEqual(auction_day_start, day_start)
        self.assertEqual(auction_stream, stream)
        self.assertTrue(new_slot)

    def test_set_end_of_auction_without_free_slot_limit_stream_and_wrong_day_start(self):
        stream = 3
        streams = 3
        next_date = datetime.now().date()
        day_start = time(16, 0)

        plan = {
            '_id': 'plantest_2017-10-03',
            'stream_1': {},
            'stream_2': {},
            'stream_3': {},
            'streams': 3
        }

        result = self.manager.set_end_of_auction(
            stream, streams, next_date, day_start, plan
        )
        self.assertIsNone(result)


class NonClassicManagerTestMixin(object):
    manager = None
    streams_key = ''

    def test_get_date(self):
        date = datetime.now().date()
        plan_date = parse_date(date.isoformat() + 'T' + self.manager.working_day_start.isoformat(), None)
        plan_date = plan_date.astimezone(TZ) if plan_date.tzinfo else TZ.localize(plan_date)

        db = mock.MagicMock()

        db.get.return_value = plantest

        plan_time, stream, plan = self.manager.get_date(db, '', date)

        self.assertEqual(plan, plantest)
        self.assertEqual(plan_time, plan_date.time())
        self.assertEqual(stream, len(plantest[self.streams_key]))

    def test_get_hours_and_stream(self):
        plan = {
            self.streams_key: [1, 2, 3, 4, 5]
        }

        plan_date, streams = self.manager._get_hours_and_stream(plan)
        self.assertEqual(plan_date, self.manager.working_day_start.isoformat())
        self.assertEqual(streams, len(plan[self.streams_key]))

    def test_set_date(self):
        db = mock.MagicMock()
        plan = {
            self.streams_key: []
        }
        auction_id = '1' * 32

        self.manager.set_date(db, plan, auction_id)
        self.assertIn(auction_id, plan[self.streams_key])
        self.assertEqual(len(plan[self.streams_key]), 1)

        db.save.assert_called_with(plan)

    def test_free_slot(self):
        plan = {
            self.streams_key: [
                '1' * 32,
                '2' * 32,
                '3' * 32,
                '1' * 32,
            ]
        }
        db = mock.MagicMock()
        plan_id = 'plan_id'
        db.get.return_value = plan
        auction_id = '1' * 32

        self.manager.free_slot(db, plan_id, auction_id)

        self.assertEqual(len(plan[self.streams_key]), 2)
        self.assertNotIn('1' * 32, plan[self.streams_key])

        db.get.assert_called_with(plan_id)
        db.save.assert_called_with(plan)

    def set_end_of_auction(self):
        stream = 1
        streams = 3
        next_date = datetime.now().date()
        day_start = time(15, 0)

        start, end, auction_day_start, auction_stream, new_slot = self.manager.set_end_of_auction(
                                                            stream, streams, next_date, day_start
                                                            )

        expected_start = TZ.localize(datetime.combine(next_date, day_start))

        self.assertEqual(start, expected_start)
        self.assertEqual(end, expected_start + self.manager.working_day_duration)
        self.assertEqual(auction_day_start, day_start)
        self.assertEqual(auction_stream, stream)
        self.assertFalse(new_slot)

    def set_end_of_auction_with_wrong_stream(self):
        stream = 10
        streams = 3
        next_date = datetime.now().date()
        day_start = time(15, 0)

        result = self.manager.set_end_of_auction(
            stream, streams, next_date, day_start
        )

        self.assertIsNone(result)


class TestInsiderAuctionManager(BaseWebTest, NonClassicManagerTestMixin):
    streams_key = 'dutch_streams'

    def setUp(self):
        super(TestInsiderAuctionManager, self).setUp()
        self.manager = InsiderAuctionsManager()


class TestTexasAuctionManager(BaseWebTest, NonClassicManagerTestMixin):
    streams_key = 'texas_streams'

    def setUp(self):
        super(TestTexasAuctionManager, self).setUp()
        self.manager = TexasAuctionsManager()
