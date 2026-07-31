"""Display formatting helpers for currency values."""

from decimal import Decimal

CURRENCY_SYMBOLS = {
    "gbp": "£",
    "eur": "€",
    "usd": "$",
    "jpy": "¥",
}

ZERO_DECIMAL_CURRENCIES = {
    "bif",
    "clp",
    "djf",
    "gnf",
    "jpy",
    "kmf",
    "krw",
    "mga",
    "pyg",
    "rwf",
    "ugx",
    "vnd",
    "vuv",
    "xaf",
    "xof",
    "xpf",
}

THREE_DECIMAL_CURRENCIES = {"bhd", "jod", "kwd", "omr", "tnd"}


# Purpose: symbols and decimal rules keep metrics readable for more than one currency.


def format_money(value: Decimal | float | int | None, currency: str) -> str:
    """Format a database amount without converting it into another currency."""

    numeric_value = Decimal(str(value or 0))
    normalised_currency = currency.lower()
    symbol = CURRENCY_SYMBOLS.get(normalised_currency, f"{normalised_currency.upper()} ")

    if normalised_currency in ZERO_DECIMAL_CURRENCIES:
        return f"{symbol}{numeric_value:,.0f}"
    if normalised_currency in THREE_DECIMAL_CURRENCIES:
        return f"{symbol}{numeric_value:,.3f}"
    return f"{symbol}{numeric_value:,.2f}"


# Purpose: formatting changes presentation only; stored payment amounts remain untouched.
