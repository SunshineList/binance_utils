from binance.client import Client
import pandas as pd
import time
from datetime import datetime
import os
from decimal import Decimal, ROUND_DOWN
try:
    from local_config import api_key, api_secret
except ImportError:
    from config import api_key, api_secret

class PriceMonitor:
    def __init__(self, api_key, api_secret):
        self.client = Client(api_key=api_key, api_secret=api_secret)
        self.base_dir = 'price_data'
        if not os.path.exists(self.base_dir):
            os.makedirs(self.base_dir)
        self.position = {}

        # 风险控制参数
        self.max_positions = 1  # 最大同时持仓数
        self.max_position_value = 100000  # 单个仓位最大价值
        self.total_position_limit = 300000  # 总仓位限制
        self.stop_loss_threshold = 0.02  # 止损阈值

        # 交易统计
        self.trade_history = []  # 交易历史记录
        self.total_pnl = 0  # 总盈亏，记录所有交易的累计盈亏值
        self.win_trades = 0  # 盈利交易数，记录所有盈利交易的次数
        self.total_trades = 0  # 总交易数，记录执行过的所有交易次数

        # 创建交易记录文件
        self.trade_log_file = os.path.join(self.base_dir, 'trade_history.csv')
        if not os.path.exists(self.trade_log_file):
            pd.DataFrame(columns=[
                'timestamp', 'pair', 'type', 'entry_price_diff',
                'exit_price_diff', 'pnl', 'duration', 'status'
            ]).to_csv(self.trade_log_file, index=False)

    def execute_order(self, symbol, side, order_type, quantity, price=None, is_futures=False):
        """执行订单并返回订单状态"""
        try:
            params = {
                'symbol': symbol,
                'side': side,
                'quantity': quantity
            }

            if price:
                params['price'] = self.format_price(price)
                if is_futures:
                    order = self.client.futures_coin_create_order(
                        **params,
                        type='LIMIT',
                        timeInForce='GTC'
                    )
                else:
                    order = self.client.create_order(
                        **params,
                        type='LIMIT',
                        timeInForce='GTC'
                    )
            else:
                if is_futures:
                    order = self.client.futures_coin_create_order(
                        **params,
                        type='MARKET'
                    )
                else:
                    order = self.client.create_order(
                        **params,
                        type='MARKET'
                    )

            # 检查订单状态
            if is_futures:
                status = order.get('status')
                executed_qty = float(order.get('executedQty', 0))
                avg_price = float(order.get('avgPrice', 0))
            else:
                status = order.get('status')
                executed_qty = float(order.get('executedQty', 0))
                avg_price = float(order.get('cummulativeQuoteQty', 0)) / executed_qty if executed_qty > 0 else 0

            # 监控订单状态直到完全成交或超时
            max_wait_time = 60  # 最大等待时间（秒）
            start_time = time.time()

            while status not in ['FILLED', 'CANCELED', 'REJECTED'] and time.time() - start_time < max_wait_time:
                # 更新订单状态
                if is_futures:
                    order_status = self.client.futures_coin_get_order(symbol=symbol, orderId=order.get('orderId'))
                else:
                    order_status = self.client.get_order(symbol=symbol, orderId=order.get('orderId'))

                status = order_status.get('status')
                executed_qty = float(order_status.get('executedQty', 0))

                if is_futures:
                    avg_price = float(order_status.get('avgPrice', 0))
                else:
                    avg_price = float(
                        order_status.get('cummulativeQuoteQty', 0)) / executed_qty if executed_qty > 0 else 0

                if status == 'PARTIALLY_FILLED':
                    print(f"订单部分成交: {symbol} {side} - 已成交数量: {executed_qty}")

                time.sleep(2)

            # 如果订单未完全成交且超时，尝试取消订单
            if status not in ['FILLED', 'CANCELED', 'REJECTED']:
                try:
                    if is_futures:
                        self.client.futures_coin_cancel_order(symbol=symbol, orderId=order.get('orderId'))
                    else:
                        self.client.cancel_order(symbol=symbol, orderId=order.get('orderId'))
                    print(f"订单已超时取消: {symbol} {side}")
                except Exception as e:
                    print(f"取消订单失败: {e}")

            return {
                'order_id': order.get('orderId'),
                'status': status,
                'executed_qty': executed_qty,
                'avg_price': avg_price,
                'symbol': symbol,
                'side': side
            }
        except Exception as e:
            print(f"下单错误: {e}")
            return None

    def execute_dual_leg_trade(self, pair_config, price_data, is_open=True):
        """执行双腿交易策略
        
        Args:
            pair_config: 交易对配置，包含交易对信息和交易参数
            price_data: 当前市场价格数据
            is_open: 是否为开仓操作，True表示开仓，False表示平仓
            
        Returns:
            bool: 交易执行是否成功
        """
        try:
            spot_symbol = pair_config['pair1']['symbol']
            futures_symbol = pair_config['pair2']['symbol']
            trade_params = pair_config['trade_params']

            # 确定买卖方向
            if is_open:
                spot_side = 'BUY'
                futures_side = 'SELL'
            else:
                spot_side = 'SELL'
                futures_side = 'BUY'

            # 获取当前价格
            spot_price = price_data['pair1_bid'] if spot_side == 'SELL' else price_data['pair1_ask']
            futures_price = price_data['pair2_bid'] if futures_side == 'SELL' else price_data['pair2_ask']

            # 计算仓位价值
            position_value = spot_price * trade_params['trade_amount']

            # 风险控制检查
            if is_open:
                # 检查最大持仓数限制
                if len(self.position) >= self.max_positions:
                    print("达到最大持仓数限制，无法开新仓")
                    return False

                # 计算当前总仓位价值
                total_position_value = sum(spot_price * trade_params['trade_amount']
                                           for _, pos_config in self.position.items())

                # 检查总仓位限制
                if total_position_value + position_value > self.total_position_limit:
                    print(f"总仓位价值 {total_position_value + position_value} 超过限制 {self.total_position_limit}")
                    return False
            else:
                # 检查止损条件
                entry_price_diff = self.position[pair_config['description']]['entry_price_diff']
                current_price_diff = price_data['difference_percentage']
                price_change = abs(current_price_diff - entry_price_diff)

                # 如果价差变动超过止损阈值，强制平仓
                if price_change >= self.stop_loss_threshold * 100:  # 转换为百分比
                    print(
                        f"触发止损: 初始价差 {entry_price_diff}%, 当前价差 {current_price_diff}%, 变动 {price_change}%")
                    return True  # 允许执行平仓操作

            if position_value > self.max_position_value:
                print(f"仓位价值 {position_value} 超过单个仓位限制 {self.max_position_value}")
                return False

            # 执行订单
            for _ in range(trade_params['max_retries']):
                spot_order = self.execute_order(spot_symbol, spot_side, 'LIMIT',
                                                trade_params['trade_amount'], spot_price)
                futures_order = self.execute_order(futures_symbol, futures_side, 'LIMIT',
                                                   trade_params['trade_amount'], futures_price, True)

                if spot_order and futures_order:
                    # 检查订单状态
                    if spot_order['status'] in ['FILLED', 'NEW'] and futures_order['status'] in ['FILLED', 'NEW']:
                        print(f"双腿交易执行成功: {pair_config['description']}")
                        print(f"现货{spot_side}: {spot_symbol} @ {spot_order['avg_price'] or spot_price}")
                        print(f"合约{futures_side}: {futures_symbol} @ {futures_order['avg_price'] or futures_price}")

                        # 更新交易统计
                        if not is_open:  # 平仓时更新统计
                            entry_data = self.position[pair_config['description']]
                            exit_price_diff = price_data['difference_percentage']
                            duration = (datetime.now() - entry_data['entry_time']).total_seconds() / 3600  # 小时
                            pnl = abs(exit_price_diff - entry_data['entry_price_diff'])

                            trade_record = {
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                                'pair': pair_config['description'],
                                'type': 'close',
                                'entry_price_diff': entry_data['entry_price_diff'],
                                'exit_price_diff': exit_price_diff,
                                'pnl': pnl,
                                'duration': round(duration, 2),
                                'status': 'win' if pnl > 0 else 'loss'
                            }

                            # 更新统计数据
                            self.trade_history.append(trade_record)
                            self.total_pnl += pnl
                            self.total_trades += 1
                            if pnl > 0:
                                self.win_trades += 1

                            # 保存交易记录
                            pd.DataFrame([trade_record]).to_csv(
                                self.trade_log_file, mode='a', header=False, index=False
                            )

                        return True

                # 如果订单未成功，调整价格重试
                spot_price *= (1 + trade_params['price_step']) if spot_side == 'BUY' else (
                        1 - trade_params['price_step'])
                futures_price *= (1 + trade_params['price_step']) if futures_side == 'BUY' else (
                        1 - trade_params['price_step'])

            print(f"双腿交易执行失败: {pair_config['description']}")
            return False
        except Exception as e:
            print(f"执行双腿交易错误: {e}")
            return False

    def monitor_prices(self, pair_configs, interval=2):
        """监控价格并执行交易策略"""
        print("开始价格监控和交易...按Ctrl+C停止")

        while True:
            try:
                for pair_config in pair_configs:
                    price_data = self.get_price_difference(pair_config)
                    if not price_data:
                        continue

                    self.save_to_csv(price_data)
                    self.print_price_data(price_data)

                    # 检查是否满足交易条件
                    price_diff_abs = abs(price_data['difference_percentage'])
                    pair_key = pair_config['description']
                    trade_params = pair_config['trade_params']

                    # 开仓条件：价差超过开仓阈值且当前无仓位
                    if price_diff_abs >= trade_params['open_threshold'] and pair_key not in self.position:
                        if self.execute_dual_leg_trade(pair_config, price_data, True):
                            self.position[pair_key] = {
                                'entry_price_diff': price_data['difference_percentage'],
                                'entry_time': datetime.now()
                            }

                    # 平仓条件：价差回落到平仓阈值且有持仓
                    elif price_diff_abs <= trade_params['close_threshold'] and pair_key in self.position:
                        if self.execute_dual_leg_trade(pair_config, price_data, False):
                            del self.position[pair_key]

                time.sleep(interval)
            except KeyboardInterrupt:
                print("\n程序已停止")
                break
            except Exception as e:
                print(f"监控错误: {e}")
                time.sleep(interval)

    def get_market_depth(self, pair_config):
        """获取单个交易对的市场深度数据"""
        try:
            if pair_config['type'] == 'spot':
                depth = self.client.get_order_book(symbol=pair_config['symbol'], limit=5)
            else:
                depth = self.client.futures_coin_order_book(symbol=pair_config['symbol'], limit=5)

            return {
                'bid': float(depth['bids'][0][0]),
                'ask': float(depth['asks'][0][0])
            }
        except Exception as e:
            print(f"获取{pair_config['symbol']}深度数据错误: {e}")
            return None

    def get_price_difference(self, pair_config):
        """获取价格差异数据"""
        try:
            depth1 = self.get_market_depth(pair_config['pair1'])
            depth2 = self.get_market_depth(pair_config['pair2'])

            if not depth1 or not depth2:
                return None

            # 使用买一价格计算价差
            price_diff_percentage = round(((depth2['bid'] - depth1['bid']) / depth1['bid']) * 100, 2)

            return {
                'pair1_symbol': pair_config['pair1']['symbol'],
                'pair2_symbol': pair_config['pair2']['symbol'],
                'pair1_type': pair_config['pair1']['type'],
                'pair2_type': pair_config['pair2']['type'],
                'description': pair_config['description'],
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'pair1_bid': depth1['bid'],
                'pair2_bid': depth2['bid'],
                'pair1_ask': depth1['ask'],
                'pair2_ask': depth2['ask'],
                'difference_percentage': price_diff_percentage
            }
        except Exception as e:
            print(f"计算{pair_config['description']}价差错误: {e}")
            return None

    def save_to_csv(self, data):
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

    def format_price(self, price):
        """格式化价格，去除多余小数位"""
        return float(Decimal(str(price)).quantize(Decimal('0.00000001'), rounding=ROUND_DOWN))


# 配置要监控的交易对
PAIR_CONFIGS = [
    {
        'pair1': {'symbol': 'ADAUSDT', 'type': 'spot'},
        'pair2': {'symbol': 'ADAUSD_250328', 'type': 'futures'},
        'description': 'ADA-现货vs合约',
        'trade_params': {
            'trade_amount': 10,  # 每次交易数量
            'open_threshold': 0.6,  # 开仓阈值
            'close_threshold': 0.62,  # 平仓阈值
            'price_step': 0.1,  # 价格调整步长（百分比）
            'max_retries': 3  # 最大重试次数
        }
    },
    # {
    #     'pair1': {'symbol': 'ETHUSDT', 'type': 'spot'},
    #     'pair2': {'symbol': 'ETHUSD_250627', 'type': 'futures'},
    #     'description': 'ETH-现货vs合约',
    #     'trade_params': {
    #         'trade_amount': 5,  # 每次交易数量
    #         'open_threshold': 3.5,  # 开仓阈值
    #         'close_threshold': 2.5,  # 平仓阈值
    #         'price_step': 0.1,  # 价格调整步长（百分比）
    #         'max_retries': 3  # 最大重试次数
    #     }
    # },
    # {
    #     'pair1': {'symbol': 'BTCUSD_250328', 'type': 'futures'},
    #     'pair2': {'symbol': 'BTCUSD_250627', 'type': 'futures'},
    #     'description': 'BTC-币本位vs币本位',
    #     'trade_params': {
    #         'trade_amount': 10,  # 每次交易数量
    #         'open_threshold': 2.0,  # 开仓阈值
    #         'close_threshold': 1.0,  # 平仓阈值
    #         'price_step': 0.05,  # 价格调整步长（百分比）
    #         'max_retries': 3  # 最大重试次数
    #     }
    # }
]


def main():
    monitor = PriceMonitor(
        api_key=api_key,
        api_secret=api_secret
    )
    monitor.monitor_prices(PAIR_CONFIGS)


if __name__ == "__main__":
    main()
