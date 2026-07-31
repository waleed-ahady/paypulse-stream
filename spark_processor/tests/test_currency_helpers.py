"""Tests for currency minor-unit conversion rules."""

from jobs.currency import minor_unit_divisor


def test_standard_currency_uses_two_decimals() -> None:
    assert minor_unit_divisor("gbp") == 100


# Purpose: GBP 7,999 minor units becomes £79.99.


def test_zero_decimal_currency_uses_no_division() -> None:
    assert minor_unit_divisor("JPY") == 1


# Purpose: case normalisation and zero-decimal currencies are protected from regressions.


def test_three_decimal_currency_uses_thousand() -> None:
    assert minor_unit_divisor("kwd") == 1000


# Purpose: three-decimal currencies remain accurate even though the default is two decimals.
