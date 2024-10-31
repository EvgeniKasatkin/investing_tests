import os
import time
import pandas as pd
from datetime import datetime, timedelta
from tinkoff.invest import (
    CandleInstrument,
    Client,
    MarketDataRequest,
    SubscribeCandlesRequest,
    SubscriptionAction,
    SubscriptionInterval,
)
import multiprocessing
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())
TOKEN = os.getenv('token')


class Streaming:

    @staticmethod
    def request_iterator(figi_):
        yield MarketDataRequest(
            subscribe_candles_request=SubscribeCandlesRequest(
                waiting_close=True,
                subscription_action=SubscriptionAction.SUBSCRIPTION_ACTION_SUBSCRIBE,
                instruments=[
                    CandleInstrument(
                        figi=figi_,
                        interval=SubscriptionInterval.SUBSCRIPTION_INTERVAL_ONE_MINUTE,
                    )
                ],
            )
        )
        while True:
            time.sleep(1)

    @classmethod
    def streaming_printing(cls, figi, ticker, start_time, limit_second):
        with Client(TOKEN) as client:
            for marketdata in client.market_data_stream.market_data_stream(cls.request_iterator(figi)):
                try:
                    if len(str(marketdata.candle.close.nano)) == 8:
                        close_nano = '0' + str(marketdata.candle.close.nano)
                    else:
                        close_nano = str(marketdata.candle.close.nano)

                    if len(str(marketdata.candle.open.nano)) == 8:
                        open_nano = '0' + str(marketdata.candle.open.nano)
                    else:
                        open_nano = str(marketdata.candle.open.nano)

                    if len(str(marketdata.candle.high.nano)) == 8:
                        high_nano = '0' + str(marketdata.candle.high.nano)
                    else:
                        high_nano = str(marketdata.candle.high.nano)

                    if len(str(marketdata.candle.low.nano)) == 8:
                        low_nano = '0' + str(marketdata.candle.low.nano)
                    else:
                        low_nano = str(marketdata.candle.low.nano)

                    candle_datetime = datetime.strptime(str(marketdata.candle.time)[0:19], '%Y-%m-%d %H:%M:%S') + timedelta(hours=3)
                    candle_datetime = candle_datetime.strftime('%Y-%m-%d %H:%M:%S')

                    with open(str(os.getenv('path_to_dir')) + '/test_of_streaming.txt', "a+", encoding="utf-8") as f:
                        f.write(str(marketdata.candle.figi) + '\t' + str(marketdata.candle.open.units) + '.' + open_nano + '\t' + str(marketdata.candle.high.units) + '.' + high_nano + '\t' + str(marketdata.candle.low.units) + '.' + low_nano + '\t' + str(marketdata.candle.close.units) + '.' + close_nano + '\t' + str(marketdata.candle.volume) + '\t' + str(marketdata.candle.time)[0:19] + '\t' + str(marketdata.candle.last_trade_ts) + '\n' )
                    with open(str(os.getenv('path_to_dir')) + '/daily_txt_files_streaming/' + str(ticker) + '.txt', "a+", encoding="utf-8") as f:
                        f.write(str(candle_datetime) + ',' + str(marketdata.candle.close.units) + '.' + close_nano + ',' + str(ticker) + ',' + str(figi) + '\n')

                except:
                    pass

                if time.time() - start_time > limit_second:
                    print(time.time())
                    break



if __name__ == "__main__":

    today = time.strftime("%Y%m%d", time.localtime())
    daily_df = pd.read_csv(str(os.getenv('path_to_dir')) + '/daily_dframes/' + time.strftime("%Y%m%d", time.localtime()) + '.txt')
    list_of_figi = list(daily_df['figi'])
    list_of_tickers = list(daily_df['tiker'])

    len_of_lists = len(list_of_tickers)
    limit_second = 60
    test_param = 0
    while True:
        start_time = time.time()
        time_list, limit_second_list = [], []
        try:
            for ind_ in range(len_of_lists):
                time_list.append(start_time)
                limit_second_list.append(limit_second)
            pool = multiprocessing.Pool(len_of_lists)
            with multiprocessing.Pool(len_of_lists) as pool:
                results = pool.starmap(Streaming.streaming_printing, zip(list_of_figi, list_of_tickers, time_list, limit_second_list))
        except:
            time.sleep(12)

        test_param += 1
        if test_param > 3000:
            break


