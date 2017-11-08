#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from raptors.skeleton import fib

__author__ = "Jeyaraj Durairaj"
__copyright__ = "Jeyaraj Durairaj"
__license__ = "none"


def test_fib():
    assert fib(1) == 1
    assert fib(2) == 1
    assert fib(7) == 13
    with pytest.raises(AssertionError):
        fib(-10)
