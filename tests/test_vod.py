import re
import pytest
from src.vod.vod import validate_time
from src.vod.vod import time_to_seconds


class TestValidateTime:
    @pytest.mark.parametrize('time,lower,upper', [
        (6, 0, 60), (7, 1, 8), (5, 4, 9), (58, 57, 60), (1, 1, 59), (59, 1, 59)
    ])
    def test_validate_time_in_bounds(self, time: int, lower: int, upper: int):
        try:
            validate_time(time, lower, upper)
        except ValueError:
            pytest.fail("validate_time() raised ValueError unexpectedly.")

    @pytest.mark.parametrize('time,lower,upper', [(0, 1, 59), (60, 1, 59), (51, 60, 61), (41, 3, 40)])
    def test_validate_time_out_of_bounds(self, time: int, lower: int, upper: int):
        with pytest.raises(ValueError, match="Time is out of range. Please enter a correct time."):
            validate_time(time, lower, upper)


class TestTimeToSeconds:
    @pytest.mark.parametrize('time', ["0", "0 0", "0 5 6 1", "1 2 2 2 2 2", "-1-1-1"])
    def test_time_to_seconds_wrong_len_args(self, time: str):
        with pytest.raises(ValueError, match=re.escape("Please enter time in the format [hh mm ss].")):
            time_to_seconds(time)

    @pytest.mark.parametrize('time', [
        "0 -1 5", "-1 0 -1", "-1 -1 -1", "0 0 60", "0 60 0", "5 60 60"
    ])
    def test_time_to_seconds_invalid_input(self, time: str):
        with pytest.raises(ValueError, match=re.escape("Time is out of range. Please enter a correct time.")):
            time_to_seconds(time)

    @pytest.mark.parametrize('time,expected', [
        ("5 50 4", 21004), ("6 8 9", 22089), ("15 6 25", 54385), ("20 24 24", 73464), ("9 50 42", 35442),
        ("0 0 42", 42), ("5 0 0", 18000), ("0 59 0", 3540), ("23 59 59", 86399)
    ])
    def test_time_to_seconds_valid_input(self, time: str, expected: int):
        assert time_to_seconds(time) == expected
