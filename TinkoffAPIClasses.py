import os
from dotenv import load_dotenv, find_dotenv
from decimal import Decimal
from datetime import datetime, timedelta
import telebot
from telebot import types
import pandas as pd
import time
from tinkoff.invest import (
    Client,
    InstrumentIdType,
    StopOrderDirection,
    StopOrderExpirationType,
    StopOrderType,
    RequestError, OrderDirection, OrderType, Quotation
)
from tinkoff.invest.exceptions import InvestError
from tinkoff.invest.utils import decimal_to_quotation, quotation_to_decimal
import traceback
from pytz import timezone


load_dotenv(find_dotenv())
TOKEN = os.getenv('token')

class Tgmessage:
    def __init__(self, send, chatid):
        self.send = send
        self.chatid = chatid

    def message_alarm(self):
        bot = telebot.TeleBot(str(os.getenv('bot_id')))
        bot.send_message(self.chatid, self.send)



class PortfolioStats:
    @staticmethod
    def portfolio_dataframe():
        figi_list, average_position_price_list, current_price_list, quantity_lost_list, lots_list = [], [], [], [], []
        try:
            with Client(TOKEN) as client:

                response = client.users.get_accounts()
                account, *_ = response.accounts
                account_id = account.id
                PortfolioResponse = client.operations.get_portfolio(account_id=account_id)

                for pos in list(PortfolioResponse.positions):
                    if pos.instrument_type == 'share':

                        if len(str(pos.current_price.nano)) == 9:
                            current_kopeeks = str(pos.current_price.nano)
                        else:
                            current_kopeeks = '0' + str(pos.current_price.nano)

                        if len(str(pos.average_position_price.nano)) == 9:
                            average_position_kopeeks = str(pos.average_position_price.nano)
                        else:
                            average_position_kopeeks = '0' + str(pos.average_position_price.nano)

                        figi_list.append(pos.figi)
                        average_position_price_list.append(
                            str(pos.average_position_price.units) + '.' + average_position_kopeeks)
                        current_price_list.append(str(pos.current_price.units) + '.' + current_kopeeks)
                        quantity_lost_list.append(pos.quantity_lots.units)
                        lots_list.append(
                            client.instruments.get_instrument_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI,
                                                                 id=pos.figi).instrument.lot)

                    if pos.figi == 'RUB000UTSTOM':
                        if len(str(pos.quantity.nano)) == 9:
                            kopeeks = str(pos.quantity.nano)
                        else:
                            kopeeks = '0' + str(pos.quantity.nano)

                        figi_list.append(pos.figi)
                        average_position_price_list.append(str(pos.quantity.units) + '.' + kopeeks)
                        current_price_list.append(str(pos.quantity.units) + '.' + kopeeks)
                        quantity_lost_list.append(str(pos.quantity.units) + '.' + kopeeks)
                        lots_list.append(0)

        except RequestError as e:
            print(str(e))

        df = pd.DataFrame({
            'figi': figi_list,
            'average_position_price': average_position_price_list,
            'current_price': current_price_list,
            'quantity': quantity_lost_list,
            'lots': lots_list
        })
        rubles_free_volume = round(float(list(df[df['figi'] == 'RUB000UTSTOM']['quantity'])[0])) - int(os.getenv('limit_rubles'))
        buyers_attempts = rubles_free_volume // int(os.getenv('limit_of_positions')) #limit_of_positions
        return df, buyers_attempts

    @staticmethod
    def get_operations():
        tz = timezone('Europe/Moscow')
        with Client(TOKEN) as client:
            response = client.users.get_accounts()
            account, *_ = response.accounts
            account_id = account.id

            OperationsResponse = client.operations.get_operations(
                account_id=account_id,
                from_=datetime.now() - timedelta(days=1),
                to=datetime.now(tz)
            )

        figi_list, type_list, quantity_list, date_list, state_list = [], [], [], [], []
        for ind_ in range(len(OperationsResponse.operations)):
            figi_list.append(OperationsResponse.operations[ind_].figi)
            type_list.append(OperationsResponse.operations[ind_].type)
            quantity_list.append(OperationsResponse.operations[ind_].quantity)
            date_list.append(OperationsResponse.operations[ind_].date)
            state_list.append(OperationsResponse.operations[ind_].state)
        df = pd.DataFrame({
            'figi': figi_list,
            'type': type_list,
            'quantity': quantity_list,
            'date': date_list,
            'state': state_list
        })
        df['date'] = df['date'].apply(lambda x: x.astimezone(tz))
        return df


class BuyOrder:
    def __init__(self, ticker, buy_price_str):
        self.ticker = ticker
        self.buy_price_str = buy_price_str

    def buy(self):
        try:
            with Client(TOKEN) as client:
                response = client.users.get_accounts()
                account, *_ = response.accounts
                account_id = account.id

                daily_df = pd.read_csv(str(os.getenv('path_to_dir')) + '/daily_dframes/' + time.strftime("%Y%m%d", time.localtime()) + '.txt')
                FIGI = list(daily_df[daily_df['tiker'] == self.ticker]['figi'])[0]

                buy_price = self.buy_price_str
                buy_price = float(buy_price)
                min_price_increment = client.instruments.get_instrument_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id = FIGI).instrument.min_price_increment
                number_digits_after_point = 9 - len(str(min_price_increment.nano)) + 1
                min_price_increment = quotation_to_decimal(min_price_increment)
                ceil = buy_price / float(min_price_increment)
                buy_price = round(ceil) * float(min_price_increment)
                buy_price = str(buy_price)
                signs = len(buy_price[buy_price.find('.') + 1:])
                buy_price = buy_price + '0' * (9 - signs)
                buy_price = Decimal(buy_price)

                lots_value = client.instruments.get_instrument_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id = FIGI).instrument.lot

                quantity = int(os.getenv('limit_of_positions')) // (buy_price * lots_value)

                r = client.orders.post_order(
                    order_id=str(datetime.utcnow().timestamp()),
                    figi = FIGI,
                    quantity = int(quantity),
                    price = decimal_to_quotation(buy_price),
                    account_id=account_id,
                    direction=OrderDirection.ORDER_DIRECTION_BUY,
                    order_type=OrderType.ORDER_TYPE_LIMIT
                )
                return '200 - ok'
        except:
            Tgmessage(chatid=os.getenv('telegram_id'), send=str(traceback.format_exc())).message_alarm()
            pass

    def buy_average(self):
        try:
            with Client(TOKEN) as client:
                response = client.users.get_accounts()
                account, *_ = response.accounts
                account_id = account.id

                daily_df = pd.read_csv(str(os.getenv('path_to_dir')) + '/daily_dframes/' + time.strftime("%Y%m%d", time.localtime()) + '.txt')
                FIGI = list(daily_df[daily_df['tiker'] == self.ticker]['figi'])[0]

                buy_price = self.buy_price_str
                buy_price = float(buy_price)
                min_price_increment = client.instruments.get_instrument_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id=FIGI).instrument.min_price_increment
                number_digits_after_point = 9 - len(str(min_price_increment.nano)) + 1
                min_price_increment = quotation_to_decimal(min_price_increment)
                ceil = buy_price / float(min_price_increment)
                buy_price = round(ceil) * float(min_price_increment)
                buy_price = str(buy_price)
                signs = len(buy_price[buy_price.find('.') + 1:])
                buy_price = buy_price + '0' * (9 - signs)
                buy_price = Decimal(buy_price)

                lots_value = client.instruments.get_instrument_by(id_type=InstrumentIdType.INSTRUMENT_ID_TYPE_FIGI, id=FIGI).instrument.lot

                quantity = int(os.getenv('limit_of_averaging')) * int(os.getenv('limit_of_positions')) // (buy_price * lots_value)

                r = client.orders.post_order(
                        order_id=str(datetime.utcnow().timestamp()),
                        figi=FIGI,
                        quantity=int(quantity),
                        price=decimal_to_quotation(buy_price),
                        account_id=account_id,
                        direction=OrderDirection.ORDER_DIRECTION_BUY,
                        order_type=OrderType.ORDER_TYPE_LIMIT
                    )
                return '200 - ok'
        except:
            Tgmessage(chatid=os.getenv('telegram_id'), send=str(traceback.format_exc())).message_alarm()
            pass

class DailyPortfolioCheck:
    @staticmethod
    def daily_df():
        time_today = time.strftime("%Y%m%d", time.localtime())
        stats = pd.read_csv(str(os.getenv('path_to_dir')) + '/daily_dframes/all_data_' + str(time_today) + '.txt')
        portf = pd.read_csv(str(os.getenv('path_to_dir')) + '/daily_dframes/portfolio_' + str(time_today) + '.txt')
        tickers_df = pd.read_csv(str(os.getenv('path_to_dir')) + '/all_russians.txt', delimiter=',')
        portf = portf.merge(tickers_df[['figi', 'ticker']], on='figi', how='inner')
        portf = portf.merge(stats[['ticker', 'rsi']], on='ticker', how='inner')
        del portf['Unnamed: 0']
        portf['percent'] = 100 * portf['current_price'] / portf['average_position_price'] - 100
        portf['profit'] = (portf['current_price'] - portf['average_position_price']) * portf['quantity'] * portf['lots']
        return portf


