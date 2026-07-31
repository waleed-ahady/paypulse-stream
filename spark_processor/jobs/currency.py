"""Currency minor-unit rules used by Spark and unit tests."""

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

THREE_DECIMAL_CURRENCIES = {
    "bhd",
    "jod",
    "kwd",
    "omr",
    "tnd",
}


# Purpose: currency sets avoid assuming that every Stripe amount uses exactly two decimals.


def minor_unit_divisor(currency: str) -> int:
    """Return the number used to convert minor units to a displayed major amount."""

    normalised = currency.lower()
    if normalised in ZERO_DECIMAL_CURRENCIES:
        return 1
    if normalised in THREE_DECIMAL_CURRENCIES:
        return 1000
    return 100


# Purpose: this pure helper is easy to test and documents the conversion rule used in Spark.
