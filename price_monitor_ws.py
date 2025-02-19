import logging
import asyncio
from datetime import datetime
import time
import pandas as pd
import os
from binance import AsyncClient, BinanceSocketManager
from binance.client import Client
from binance.enums import FuturesType

# 配置日志
def setup_logger(name, log_file, level=logging.INFO):
    """设置日志配置"""
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # 创建logs目录
    os.makedirs('logs', exist_ok=True)
    
    # 文件处理器
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    
    # 控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

# 创建主日志记录器
logger = setup_logger('price_monitor', 'logs/price_monitor.log')

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
            self.client = await AsyncClient.create(api_key=TEST_FUTURE_API_KEY, api_secret=TEST_FUTURE_API_SECRET, testnet=IS_DEBUG)
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
            logger.error(f"处理消息错误: {e}")

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
                
                price_diff_percentage = round(((depth1['bid'] - depth2['bid']) / depth2['bid']) * 100, 2)
                
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
            logger.error(f"保存数据错误: {e}")

    def print_price_data(self, data):
        """打印价格数据"""
        logger.info(f"\n{data['description']} | {data['timestamp']}")
        logger.info(f"{data['pair1_symbol']}({data['pair1_type']}) 买一: {data['pair1_bid']} | "
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

    def change_leverage_mode(self, symbol, leverage):
        """
        切换杠杆模式
        :param symbol: 交易对
        :param leverage: 杠杆倍数
        :return: 切换结果
        """
        try:
            # 直接返回API调用结果
            result = self.client.futures_coin_change_leverage(symbol=symbol, leverage=leverage)
            result = result.get('leverage') == leverage
            logger.info(f"{symbol}已切换至{leverage}倍杠杆") if result else logger.error(f"{symbol}切换杠杆失败")
            return result
            
        except Exception as e:
            logger.error(f"切换杠杆模式失败: {e}")
            return False
        
    def change_margin_type_mode(self, symbol, margin_type):
        """
        切换保证金模式
        :param symbol: 交易对
        :param margin_type: 保证金类型
        :return: 切换结果
        """
        try:
            # 直接返回API调用结果
            result = self.client.futures_coin_change_margin_type(symbol=symbol, marginType=margin_type)
            logger.info(f"{symbol}已切换至{margin_type}保证金模式") if result else logger.error(f"{symbol}切换保证金模式失败")
            return bool(result)
            
        except Exception as e:
            logger.error(f"切换保证金模式失败: {e}")
            return False

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
                logger.warning(
                    f"保证金不足\n"
                    f"可用余额: {available_balance} {assets}\n"
                    f"钱包余额: {wallet_balance} {assets}\n"
                    f"未实现盈亏: {unrealized_profit} {assets}\n"
                    f"保证金余额: {margin_balance} {assets}\n"
                    f"预估所需保证金: {estimated_required_margin} {assets}"
                )
                return False, margin_balance, "保证金不足"
            
            logger.info(
                f"保证金充足\n"
                f"可用余额: {available_balance} {assets}\n"
            )
            return True, margin_balance, "保证金充足"
        
        except Exception as e:
            logger.error(f"检查保证金时发生错误: {e}")
            return False, 0, f"检查保证金时发生错误: {e}"

    def place_order(self, symbol, side, order_type, quantity, price=None, stop_price=None):
        """下单功能"""
        try:
            # 获取交易对的价格限制规则
            exchange_info = self.get_exchange_info(symbol)
            if not exchange_info:
                logger.error(f"无法获取{symbol}的交易规则信息")
                return None

            # 获取Mark Price
            mark_price_info = self.client.futures_coin_mark_price(symbol=symbol)[0]
            if not mark_price_info:
                logger.error(f"无法获取{symbol}的标记价格")
                return None

            mark_price = float(mark_price_info['markPrice'])
            max_price = float(exchange_info['max_price'])
            min_price = float(exchange_info['min_price'])
            
            if not self.change_leverage_mode(symbol, LEVERAGE):
                return None
            
            # 构建下单参数
            params = {
                'symbol': symbol,
                'side': side,  # BUY or SELL
                'type': order_type,  # LIMIT, MARKET, STOP, STOP_MARKET等
                'quantity': quantity,
                'newOrderRespType': 'RESULT'
            }

            if order_type == 'LIMIT':
                params["timeInForce"] = 'GTC'
            
            if price:
                # 根据标记价格和价格限制调整下单价格
                adjusted_price = price
                if side == 'BUY':
                    # 买单价格不能高于标记价格的一定比例
                    max_allowed_price = mark_price * 1.01  # 假设最大允许高于标记价格1%
                    adjusted_price = round(min(price, max_allowed_price, max_price), 1)
                else:  # SELL
                    # 卖单价格不能低于标记价格的一定比例
                    min_allowed_price = mark_price * 0.99  # 假设最小允许低于标记价格1%
                    adjusted_price = round(max(price, min_allowed_price, min_price), 1)
                
                params['price'] = adjusted_price

            if stop_price:
                params['stopPrice'] = stop_price

            logger.info(f"下单参数: {params}")
            order = self.client.futures_coin_create_order(**params)
            return order
        
        except Exception as e:
            logger.error(f"下单失败: {e}")
            return None

    def get_exchange_info(self, symbol=None):
        """获取交易对规则和交易对信息
        :param symbol: 交易对名称，如果不指定则返回所有交易对信息
        :return: 交易对规则和信息
        """
        try:
            # 获取交易对信息
            exchange_info = self.client.futures_coin_exchange_info()
            
            if symbol:
                # 如果指定了交易对，只返回该交易对的信息
                symbol_info = next((s for s in exchange_info['symbols'] if s['symbol'] == symbol), None)
                if not symbol_info:
                    return None
                # 提取交易对规则
                params = {}
                for f in symbol_info['filters']:
                    if f['filterType'] == 'PRICE_FILTER':
                        params['min_price'] = f["minPrice"]
                        params["max_price"] = f["maxPrice"]
                        params['tickSize'] = float(f['tickSize'])
                    elif f['filterType'] == 'LOT_SIZE':
                        params["max_qty"] = f["maxQty"]
                        params["min_qty"] = f["minQty"]
                        params['stepSize'] = float(f['stepSize'])
            # 返回所有交易对信息
            return params
        
        except Exception as e:
            logger.error(f"获取交易规则失败: {e}")
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
        margin_check, margin_balance, margin_info = self.check_margin(pair1_symbol, QUANTITY)
        if not margin_check or margin_balance <= MARGIN_LOW_VALUE:
            logger.warning(f"保证金不足，跳过本次交易\n{margin_info}")
            return

        try:
            if price_diff_percentage <= LOW_PERCENTAGE_DEFAULT:
                retry_count = 0
                sell_order = None
                buy_order = None
                
                while retry_count < RETRY_TIMES:
                    try:
                        # 卖掉BTCUSD_PERP
                        if not sell_order:
                            sell_price = pair2_price - PRICE_DIFF
                            sell_order = self.place_order(
                                symbol=pair2_symbol,
                                side='SELL',
                                order_type='LIMIT',
                                quantity=QUANTITY,
                                price=sell_price
                            )

                        # 买入BTCUSD_250627
                        if not buy_order:
                            buy_price = pair1_price + PRICE_DIFF
                            buy_order = self.place_order(
                                symbol=pair1_symbol,
                                side='BUY',
                                order_type='LIMIT',
                                quantity=QUANTITY,
                                price=buy_price
                            )

                        # 如果两个订单都创建成功
                        if sell_order and buy_order:
                            MID_NUM_VALUE -= 1
                            logger.info(f"订单创建成功，当前MID_NUM_VALUE: {MID_NUM_VALUE}")
                            break
                            
                    except Exception as e:
                        logger.error(f"订单创建失败，重试中: {e}")
                        retry_count += 1
                        time.sleep(1)  # 添加短暂延迟

            elif price_diff_percentage >= TOP_PERCENTAGE_DEFAULT:
                retry_count = 0
                buy_order = None
                sell_order = None
                
                while retry_count < RETRY_TIMES:
                    try:
                        # 买入BTCUSD_PERP
                        if not buy_order:
                            buy_price = pair2_price + PRICE_DIFF
                            buy_order = self.place_order(
                                symbol=pair2_symbol,
                                side='BUY',
                                order_type='LIMIT',
                                quantity=QUANTITY,
                                price=buy_price
                            )

                        # 卖出BTCUSD_250627
                        if not sell_order:
                            sell_price = pair1_price - PRICE_DIFF
                            sell_order = self.place_order(
                                symbol=pair1_symbol,
                                side='SELL',
                                order_type='LIMIT',
                                quantity=QUANTITY,
                                price=sell_price
                            )

                        # 如果两个订单都创建成功
                        if buy_order and sell_order:
                            MID_NUM_VALUE += 1
                            logger.info(f"订单创建成功，当前MID_NUM_VALUE: {MID_NUM_VALUE}")
                            break
                            
                    except Exception as e:
                        print(f"订单创建失败，重试中: {e}")
                        retry_count += 1
                        time.sleep(1)  # 添加短暂延迟

        except Exception as e:
            logger.error(f"交易执行错误: {e}")

        logger.info(f"当前交易次数: {MID_NUM_VALUE}")


if __name__ == "__main__":
    monitor = PriceMonitorWS()
    asyncio.run(monitor.main())