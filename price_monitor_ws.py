import asyncio
from datetime import datetime
import pprint
import pandas as pd
import os
from binance import AsyncClient, BinanceSocketManager
from binance.client import Client
from binance.enums import FuturesType

try:
    from local_config import *
except ImportError:
    from config import *

class PriceMonitorWS:
    def __init__(self):
        self.base_dir = 'price_data'
        self.price_data = {}
        self.last_save_times = {}  # 修改为字典，为每个交易对记录最后保存时间
        self.save_interval = INTERVAL  # 保存间隔，单位为秒
        self.first_data = {}  # 修改为字典，为每个交易对记录首次数据标志
        self.dynamic_interval = DYNAMIC_INTERVAL  # 动态监控间隔，单位为秒
        
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)

    async def init_clients(self, api_key, api_secret):
        """初始化异步客户端"""
        if IS_DEBUG:
            self.client = await AsyncClient.create(api_key=API_KEY, api_secret=API_KEY, testnet=not IS_DEBUG)
        else:
            self.client = await AsyncClient.create(api_key=api_key, api_secret=api_secret)
        self.bsm = BinanceSocketManager(self.client)

    async def subscribe_symbol(self, symbol, symbol_type):
        """订阅单个交易对的数据"""
        if symbol_type == 'spot':
            socket = self.bsm.depth_socket(symbol, depth=5)
        else:
            socket = self.bsm.futures_depth_socket(symbol, depth=5, futures_type=FuturesType.COIN_M)

        async with socket as ts:
            while True:
                msg = await ts.recv()
                await self.handle_message(msg, symbol_type, symbol)

    async def handle_message(self, msg, symbol_type, symbol):
        """处理WebSocket消息"""
        try:
            if symbol_type == 'spot':
                bid_price = float(msg['bids'][0][0])
                ask_price = float(msg['asks'][0][0])
            else:
                bid_price = float(msg['data']['b'][0][0])
                ask_price = float(msg['data']['a'][0][0])

            self.price_data[f"{symbol_type}_{symbol}"] = {
                'bid': bid_price,
                'ask': ask_price
            }
            await self.calculate_price_difference()
        except Exception as e:
            print(f"处理消息错误: {e}")

    async def calculate_price_difference(self):
        """计算并保存价格差异"""
        current_time = datetime.now()
        
        for pair_config in self.pair_configs:
            pair1, pair2 = pair_config['pair1'], pair_config['pair2']
            
            pair1_key = f"{pair1['type']}_{pair1['symbol']}"
            pair2_key = f"{pair2['type']}_{pair2['symbol']}"
            pair_desc = pair_config['description']
            
            # 初始化该交易对的首次数据标志和最后保存时间
            if pair_desc not in self.first_data:
                self.first_data[pair_desc] = True
                self.last_save_times[pair_desc] = current_time
            
            if pair1_key in self.price_data and pair2_key in self.price_data:
                depth1 = self.price_data[pair1_key]
                depth2 = self.price_data[pair2_key]
                
                price_diff_percentage = round((abs((depth1['bid'] - depth2['bid'])) / depth2['bid']) * 100, 2)
                
                data = {
                    'pair1_symbol': pair1['symbol'],
                    'pair2_symbol': pair2['symbol'],
                    'pair1_type': pair1['type'],
                    'pair2_type': pair2['type'],
                    'description': pair_config['description'],
                    'timestamp': current_time.strftime('%Y-%m-%d %H:%M:%S'),
                    'pair1_bid': depth1['bid'],
                    'pair2_bid': depth2['bid'],
                    'difference_percentage': price_diff_percentage
                }
                
                # 计算该交易对的时间差
                time_diff = (current_time - self.last_save_times[pair_desc]).total_seconds()
                
                # 根据价差判断保存间隔
                current_interval = self.dynamic_interval if (
                    price_diff_percentage <= LOW_PERCENTAGE_DEFAULT + PERCENTAGE_DIFF_DEFAULT or 
                    price_diff_percentage >= TOP_PERCENTAGE_DEFAULT - PERCENTAGE_DIFF_DEFAULT
                ) else self.save_interval
                
                # 第一次接收数据时立即保存，之后根据动态间隔保存数据
                if self.first_data[pair_desc] or time_diff >= current_interval:
                    await self.save_to_csv(data)
                    self.last_save_times[pair_desc] = current_time
                    self.first_data[pair_desc] = False  # 更新标志位
                
                # 实时打印最新数据
                self.print_price_data(data)
                
                # 调用交易策略
                trader = BinanceTrader()
                trader.bussiness(
                    price_diff_percentage=price_diff_percentage,
                    pair1_price=depth1['bid'],
                    pair2_price=depth2['bid'],
                    pair1_symbol=pair1['symbol'],
                    pair2_symbol=pair2['symbol']
                )


    async def save_to_csv(self, data):
        """保存数据到CSV文件"""
        try:
            filename = os.path.join(self.base_dir, f"{data['description']}.csv")
            df = pd.DataFrame([data])
            df.to_csv(filename, mode='a', header=not os.path.exists(filename), index=False)
        except Exception as e:
            print(f"保存数据错误: {e}")

    def print_price_data(self, data):
        """打印价格数据"""
        print(f"\n{data['description']} | {data['timestamp']}")
        print(f"{data['pair1_symbol']}({data['pair1_type']}) 买一: {data['pair1_bid']} | "
              f"{data['pair2_symbol']}({data['pair2_type']}) 买一: {data['pair2_bid']} | "
              f"价差: {data['difference_percentage']}%")

    async def start_monitoring(self, pair_configs):
        """开始监控价格"""
        self.pair_configs = pair_configs
        tasks = []

        for pair_config in pair_configs:
            pair1, pair2 = pair_config['pair1'], pair_config['pair2']
            tasks.append(self.subscribe_symbol(pair1['symbol'], pair1['type']))
            tasks.append(self.subscribe_symbol(pair2['symbol'], pair2['type']))

        await asyncio.gather(*tasks)

    async def stop(self):
        """停止监控"""
        await self.client.close_connection()

    async def main(self):
        await self.init_clients(
            api_key=TEST_FUTURE_API_KEY if IS_DEBUG else API_KEY,
            api_secret=TEST_FUTURE_API_SECRET if IS_DEBUG else API_SECRET
        )
        
        try:
            print("开始WebSocket价格监控...按Ctrl+C停止")
            await self.start_monitoring(PAIR_CONFIGS)
        except KeyboardInterrupt:
            print("\n程序已停止")
            await self.stop()
        except Exception as e:
            print(f"发生错误: {e}")
            await self.stop()


class BinanceTrader:
    def __init__(self):
        if IS_DEBUG:
            self.client = Client(TEST_FUTURE_API_KEY, TEST_FUTURE_API_SECRET, testnet=IS_DEBUG)
        else:
            self.client = Client(API_KEY, API_SECRET)

    def get_commission_rate(self, symbol):
        """获取交易对的手续费率"""
        data = self.client.futures_commission_rate(symbol=symbol)
        return data

    def get_account_margin(self, symbol=None, assets='BTC'):
        """获取合约账户保证金信息"""
        try:
            # 获取账户信息
            account = self.client.futures_coin_account()
            if symbol:
                # 获取特定交易对的持仓信息
                positions = [pos for pos in account['positions'] if pos['symbol'] == symbol]
                return {
                    'assets': [pos for pos in account['assets'] if pos['asset'] == assets],
                    'positions': positions,
                    'canTrade': account['canTrade'], # 是否可以交易
                    'canDeposit': account['canDeposit'], # 是否可以存款
                    'canWithdraw': account['canWithdraw'], # 是否可以提款
                    'feeTier': account['feeTier'], # 手续费档位
                }
            return account
        except Exception as e:
            print(f"获取保证金信息失败: {e}")
            return None
        
    # 保证金是否够
    def check_margin(self, symbol, quantity, assets='BTC'):
        """
        检查保证金是否足够
        :param symbol: 交易对
        :param quantity: 交易数量
        :param assets: 资产类型，默认BTC
        :return: (bool, str) - (是否足够, 详细信息)
        """
        try:
            # 获取账户信息
            account = self.get_account_margin(symbol, assets)
            if not account or not account['assets']:
                return False, "无法获取账户信息"
        
            asset_info = account['assets'][0]
            
            # 获取可用余额
            available_balance = float(asset_info['availableBalance'])
            
            # 获取钱包余额
            wallet_balance = float(asset_info['walletBalance'])
            
            # 获取未实现盈亏
            unrealized_profit = float(asset_info['unrealizedProfit'])
            
            # 获取保证金余额
            margin_balance = float(asset_info['marginBalance'])
        
            # TODO: 这里需要根据具体交易对获取实际所需保证金
            # 临时使用一个预估值，实际使用时需要根据杠杆率和价格计算
            estimated_required_margin = quantity * 0.01  # 示例：假设需要1%的保证金
            
            if available_balance < estimated_required_margin:
                return False, (
                    f"保证金不足\n"
                    f"可用余额: {available_balance} {assets}\n"
                    f"钱包余额: {wallet_balance} {assets}\n"
                    f"未实现盈亏: {unrealized_profit} {assets}\n"
                    f"保证金余额: {margin_balance} {assets}\n"
                    f"预估所需保证金: {estimated_required_margin} {assets}"
                )
            
            return True, (
                f"保证金充足\n"
                f"可用余额: {available_balance} {assets}\n"
                f"预估所需保证金: {estimated_required_margin} {assets}"
            )
        
        except Exception as e:
            return False, f"检查保证金时发生错误: {e}"

    def place_order(self, symbol, side, order_type, quantity, price=None, stop_price=None):
        """下单功能"""
        try:
            params = {
                'symbol': symbol,
                'side': side,  # BUY or SELL
                'type': order_type,  # LIMIT, MARKET, STOP, STOP_MARKET等
                'quantity': quantity,
                'timeInForce': 'GTC',
                'newOrderRespType': 'RESULT'
            }
            
            #TODO这里只保留这个
            if price:
                params['price'] = price - 15000
            if side == "SELL":
                if params["price"] < 86238.1 + 100:
                    params["price"] = 86238.1 + 100
            if side == "BUY":
                if params["price"] > 80351.4 + 500:
                    params["price"] = 80351.4 + 500

            if stop_price:
                params['stopPrice'] = stop_price

            order = self.client.futures_coin_create_order(**params)
            print(order)
            return order
        
        except Exception as e:
            print(f"下单失败: {e}")
            return None

    def get_open_orders(self, symbol=None):
        """查看当前委托订单"""
        try:
            if symbol:
                orders = self.client.futures_coin_get_open_orders(symbol=symbol)
            else:
                orders = self.client.futures_coin_get_open_orders()
            return orders
        except Exception as e:
            print(f"获取委托订单失败: {e}")
            return None

    def cancel_and_replace_order(self, symbol, order_id, new_quantity=None, new_price=None):
        """撤单并重新下单"""
        try:
            # 先获取原订单信息
            order = self.client.futures_coin_get_order(symbol=symbol, orderId=order_id)
            
            # 撤销原订单
            self.client.futures_coin_cancel_order(symbol=symbol, orderId=order_id)
            
            # 使用原订单信息创建新订单
            new_order_params = {
                'symbol': symbol,
                'side': order['side'],
                'type': order['type'],
                'quantity': new_quantity if new_quantity else order['origQty'],
                'price': new_price if new_price else order['price']
            }
            
            # 下新订单
            new_order = self.client.futures_coin_create_order(**new_order_params)
            return new_order
        
        except Exception as e:
            print(f"撤单重下失败: {e}")
            return None
        
    def bussiness(self, price_diff_percentage, pair1_price, pair2_price, pair1_symbol, pair2_symbol):
        """
        核心业务逻辑
        :param price_diff_percentage: 价差百分比
        :param pair1_price: BTCUSD_250627价格
        :param pair2_price: BTCUSD_PERP价格
        :param pair1_symbol: BTCUSD_250627
        :param pair2_symbol: BTCUSD_PERP
        """
        global MID_NUM_VALUE

        # 如果交易次数超出范围，等待下一次交易机会
        if MID_NUM_VALUE <= MIN_NUM_VALUE or MID_NUM_VALUE >= TOP_NUM_VALUE:
            return
            
        # 检查保证金是否充足
        margin_check, margin_info = self.check_margin(pair1_symbol, QUANTITY)
        if not margin_check or float(margin_info.split('\n')[1].split(':')[1].strip().split()[0]) <= MARGIN_LOW_VALUE:
            print(f"保证金不足，跳过本次交易\n{margin_info}")
            return

        try:
            if price_diff_percentage <= LOW_PERCENTAGE_DEFAULT:
                # 卖掉BTCUSD_PERP
                sell_price = pair2_price - PRICE_DIFF

                sell_order = self.place_order(
                    symbol=pair2_symbol,
                    side='SELL',
                    order_type='LIMIT',
                    quantity=QUANTITY,
                    price=sell_price
                )

                # 买入BTCUSD_250627
                buy_price = pair1_price + PRICE_DIFF

                buy_order = self.place_order(
                    symbol=pair1_symbol,
                    side='BUY',
                    order_type='LIMIT',
                    quantity=QUANTITY,
                    price=buy_price
                )

                # 处理订单
                retry_count = 0
                while retry_count < RETRY_TIMES:
                    # 检查订单状态
                    sell_status = self.client.futures_coin_get_order(symbol=pair2_symbol, orderId=sell_order['orderId'])
                    buy_status = self.client.futures_coin_get_order(symbol=pair1_symbol, orderId=buy_order['orderId'])

                    if sell_status['status'] == 'FILLED' and buy_status['status'] == 'FILLED':
                        MID_NUM_VALUE -= 1
                        print(f"交易成功，当前MID_NUM_VALUE: {MID_NUM_VALUE}")
                        break

                    # 如果订单未成交，撤销并重新下单
                    if sell_status['status'] == 'NEW':
                        self.client.futures_coin_cancel_order(symbol=pair2_symbol, orderId=sell_order['orderId'])
                        sell_price = pair2_price - PRICE_DIFF
                        sell_order = self.place_order(pair2_symbol, 'SELL', 'LIMIT', QUANTITY, sell_price)

                    if buy_status['status'] == 'NEW':
                        self.client.futures_coin_cancel_order(symbol=pair1_symbol, orderId=buy_order['orderId'])
                        buy_price = pair1_price + PRICE_DIFF
                        buy_order = self.place_order(pair1_symbol, 'BUY', 'LIMIT', QUANTITY, buy_price)

                    retry_count += 1

            elif price_diff_percentage >= TOP_PERCENTAGE_DEFAULT:
                # 买入BTCUSD_PERP
                buy_price = pair2_price + PRICE_DIFF
                buy_order = self.place_order(
                    symbol=pair2_symbol,
                    side='BUY',
                    order_type='LIMIT',
                    quantity=0.001,
                    price=buy_price
                )

                # 卖出BTCUSD_250627
                sell_price = pair1_price - PRICE_DIFF
                sell_order = self.place_order(
                    symbol=pair1_symbol,
                    side='SELL',
                    order_type='LIMIT',
                    quantity=0.001,
                    price=sell_price
                )

                # 处理订单
                retry_count = 0
                while retry_count < RETRY_TIMES:
                    # 检查订单状态
                    buy_status = self.client.futures_coin_get_order(symbol=pair2_symbol, orderId=buy_order['orderId'])
                    sell_status = self.client.futures_coin_get_order(symbol=pair1_symbol, orderId=sell_order['orderId'])

                    if buy_status['status'] == 'FILLED' and sell_status['status'] == 'FILLED':
                        MID_NUM_VALUE += 1
                        print(f"交易成功，当前MID_NUM_VALUE: {MID_NUM_VALUE}")
                        break

                    # 如果订单未成交，撤销并重新下单
                    if buy_status['status'] == 'NEW':
                        self.client.futures_coin_cancel_order(symbol=pair2_symbol, orderId=buy_order['orderId'])
                        buy_price = pair2_price + PRICE_DIFF
                        buy_order = self.place_order(pair2_symbol, 'BUY', 'LIMIT', QUANTITY, buy_price)

                    if sell_status['status'] == 'NEW':
                        self.client.futures_coin_cancel_order(symbol=pair1_symbol, orderId=sell_order['orderId'])
                        sell_price = pair1_price - PRICE_DIFF
                        sell_order = self.place_order(pair1_symbol, 'SELL', 'LIMIT', QUANTITY, sell_price)

                    retry_count += 1

        except Exception as e:
            print(f"交易执行错误: {e}")

        print(f"当前交易次数: {MID_NUM_VALUE}")


if __name__ == "__main__":
    monitor = PriceMonitorWS()
    asyncio.run(monitor.main())
    # trader = BinanceTrader()
    # pprint.pp(trader.check_margin("ADAUSD_250328", 0.0025, 'BTC'))
    # trader.get_commission_rate('BTCUSD_250627')