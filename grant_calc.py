from factom import Factomd
from datetime import datetime, date
import requests

factomd = Factomd(
    host='https://api.factomd.net/v2'  # Defaulted to Open Node. Change to your Factomd location
)


def current_round():
    """Determines the current grant round for today's UTC date and time"""
    time_date = datetime.utcnow()
    year = time_date.strftime("%Y")
    y = 2000  # dummy leap year to allow input X-02-29 (leap day)
    rounds = [(f"{year}.1", (date(y, 1, 1), date(y, 2, 29))),
              (f"{year}.2", (date(y, 3, 21), date(y, 6, 1))),
              (f"{year}.3", (date(y, 6, 21), date(y, 9, 22))),
              (f"{year}.4", (date(y, 9, 23), date(y, 12, 20))),
              (f"{year}.1", (date(y, 12, 1), date(y, 12, 31)))]

    def get_round(now):
        if isinstance(now, datetime):
            now = now.date()
        now = now.replace(year=y)
        return next(grant_round for grant_round, (start, end) in rounds
                    if start <= now <= end)
    return get_round(date.today())


def payout_date():
    """Returns the activation date given the current grant round"""
    time_date = datetime.utcnow()
    year = time_date.strftime("%Y")
    dates = {f"{year}.1": date(int(year), 3, 1),
             f"{year}.2": date(int(year), 6, 1),
             f"{year}.3": date(int(year), 9, 1),
             f"{year}.4": date(int(year), 12, 1)}
    paydate = dates[current_round()]
    return paydate


def current_block():
    """Determines the current Factom block height"""
    # Gets current block height
    current_height = factomd.heights()['directoryblockheight']
    # Returns the current block height
    return current_height


def activation_countdown():
    """Determines the seconds until the target activation date given the current grant round"""
    d1 = datetime.utcnow().replace(microsecond=0)
    year = payout_date().year
    month = payout_date().month
    day = payout_date().day
    d2 = datetime(year, month, day, 12, 0, 1)
    delta = d2-d1
    return delta


def payout_block():
    """Calculates the target activation block given the current grant round and target date"""
    delta = activation_countdown()
    blocktime_in_seconds = 600
    days_in_seconds = delta.days * 24 * 60 * 60
    hours_in_seconds = delta.seconds
    seconds_until_payout = days_in_seconds + hours_in_seconds
    blocks = seconds_until_payout / blocktime_in_seconds
    target_block = round((current_block() + blocks))
    # Validates if the block is divisible by 25, then adds 1
    while True:
        if target_block % 25 == 0:
            payoutblock = (target_block + 1)
            return payoutblock
        else:
            target_block = (target_block - 1)


def activation_block():
    """Determines the activation block for the given payout block"""
    # Activation block is 1000 blocks prior to the payout to allow for coinbase cancel
    activate_block = payout_block() - 1000
    return activate_block


def daily_grant_pool_contribution():
    """Determines daily grant pool contribution from Luciap.ca authority set datapoint"""
    url = "https://luciap.ca/api/v1/authority-set/summary"
    response = requests.get(url)
    authority_set = response.json()
    grant_pool_per_day = 0
    for factoidsPerDay in authority_set:
        try:
            if factoidsPerDay['entity']['disabled']:  # Removes any disabled entities
                pass
            else:
                pass
        except:  # adds all calculated grant pool contributions
            sumdata = factoidsPerDay['factoidsPerDay']['grantPool']
            grant_pool_per_day += sumdata
    return grant_pool_per_day


def grant_pool_contribution_at_payout_block():
    """Determines the value of all contributions to grant pool from now until payout block"""
    current_blk = current_block()
    pay_blk = payout_block()
    blocks_until_payout = pay_blk - current_blk
    daily = daily_grant_pool_contribution()
    block_contribution = daily / 144  # 144 blocks per day
    grant_addition_until_payout = round(block_contribution * blocks_until_payout)
    return grant_addition_until_payout


def existing_grant_pool():
    """Determines the current grant pool valuation from factoshi.io datapoint"""
    # Will be added once Factoshi API is updated
    pass


def grant_pool_value():
    """Determines the grant pool value at the next grant round payout block"""
    # Will be activated once Factoshi API is connected
    # # # grant_pool_value = existing_grant_pool() + grant_pool_contribution_at_payout_block()
    # # # return grant_pool_value
    pass
