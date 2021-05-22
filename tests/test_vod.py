from unittest import TestCase
from twitch_vod_dl.vod import validate_time
from twitch_vod_dl.vod import time_to_seconds


class TestValidateTime(TestCase):
    def test_validate_time_in_bounds(self):
        try:
            args = [[6, 0, 60], [7, 1, 8], [5, 4, 9], [58, 57, 60], [1, 1, 59], [59, 1, 59]]
            for time, lower, upper in args:
                validate_time(time, lower, upper)
        except ValueError:
            self.fail("validate_time() raised ValueError unexpectedly.")

    def test_validate_time_out_of_bounds(self):
        args = [[0, 1, 59], [60, 1, 59], [51, 60, 61], [41, 3, 40]]
        for time, lower, upper in args:
            self.assertRaises(ValueError, validate_time, time, lower, upper)


class TestTimeToSeconds(TestCase):
    def test_time_to_seconds_invalid_input(self):
        wrong_len_args = ["0", "0 0", "0 5 6 1", "1 2 2 2 2 2"]
        for wrong_len_arg in wrong_len_args:
            self.assertRaises(ValueError, time_to_seconds, wrong_len_arg)
        invalid_time_args = ["0 -1 5", "-1 0 -1", "-1-1-1", "1, 1, 1", "1:1:1 ", "0 0 60", "0 60 0", "5 60 60"]
        for invalid_time_arg in invalid_time_args:
            self.assertRaises(ValueError, time_to_seconds, invalid_time_arg)

    def test_time_to_seconds_valid_input(self):
        args = ["5 50 4", "6 8 9", "15 6 25", "20 24 24", "9 50 42", "0 0 42", "5 0 0", "0 59 0", "23 59 59"]
        answers = [21004, 22089, 54385, 73464, 35442, 42, 18000, 3540, 86399]
        for arg, answer in zip(args, answers):
            self.assertEqual(answer, time_to_seconds(arg))
