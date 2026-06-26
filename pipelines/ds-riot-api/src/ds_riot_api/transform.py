import datetime as dt


def yearmonth_from_match(match_info: dict) -> str:
    datetime = dt.datetime.fromtimestamp(
        match_info['info']['gameCreation'] / 1000,
        tz=dt.timezone.utc
    )

    return f"{datetime.year}{datetime.strftime("%m")}"
