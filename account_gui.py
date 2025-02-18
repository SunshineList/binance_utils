from PyQt5.QtWidgets import QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, \
    QPushButton, QLabel, QTableWidget, QTableWidgetItem, QTabWidget, QMessageBox
from PyQt5.QtCore import Qt, QTimer
from binance.client import Client
from datetime import datetime
import sys

try:
    from local_config import *
except ImportError:
    from config import *


class BinanceAccountGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        # 初始化币安客户端
        self.api_key = API_KEY
        self.api_secret = API_SECRET
        self.client = Client(self.api_key, self.api_secret)

        # 初始化定时器
        self.price_timer = QTimer()
        self.price_timer.timeout.connect(self.update_price_difference)
        self.price_timer.start(5000)  # 每5秒更新一次

        self.initUI()

    def initUI(self):
        self.setWindowTitle('币安账户查询系统')
        self.setGeometry(100, 100, 800, 600)

        # 创建中央窗口部件和布局
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # 创建标签页
        tab_widget = QTabWidget()
        layout.addWidget(tab_widget)

        # 账户余额标签页
        balance_tab = QWidget()
        balance_layout = QVBoxLayout(balance_tab)

        # 创建余额表格
        self.balance_table = QTableWidget()
        self.balance_table.setColumnCount(3)
        self.balance_table.setHorizontalHeaderLabels(['资产', '可用余额', '冻结余额'])
        balance_layout.addWidget(self.balance_table)

        # 刷新按钮
        refresh_balance_btn = QPushButton('刷新余额')
        refresh_balance_btn.clicked.connect(self.update_balance)
        balance_layout.addWidget(refresh_balance_btn)

        # 持仓信息标签页
        position_tab = QWidget()
        position_layout = QVBoxLayout(position_tab)

        # 创建持仓表格
        self.position_table = QTableWidget()
        self.position_table.setColumnCount(4)
        self.position_table.setHorizontalHeaderLabels(['交易对', '持仓数量', '入场价格', '当前价格'])
        position_layout.addWidget(self.position_table)

        # 刷新按钮
        refresh_position_btn = QPushButton('刷新持仓')
        refresh_position_btn.clicked.connect(self.update_positions)
        position_layout.addWidget(refresh_position_btn)

        # 交易历史标签页
        history_tab = QWidget()
        history_layout = QVBoxLayout(history_tab)

        # 创建历史记录表格
        self.history_table = QTableWidget()
        self.history_table.setColumnCount(5)
        self.history_table.setHorizontalHeaderLabels(['时间', '交易对', '方向', '价格', '数量'])
        history_layout.addWidget(self.history_table)

        # 刷新按钮
        refresh_history_btn = QPushButton('刷新历史')
        refresh_history_btn.clicked.connect(self.update_history)
        history_layout.addWidget(refresh_history_btn)

        # 价差监控标签页
        price_diff_tab = QWidget()
        price_diff_layout = QVBoxLayout(price_diff_tab)

        # 创建价差表格
        self.price_diff_table = QTableWidget()
        self.price_diff_table.setColumnCount(4)
        self.price_diff_table.setHorizontalHeaderLabels(['交易对', '现货买一', '合约买一', '价差百分比'])
        self.price_diff_table.setRowCount(1)
        price_diff_layout.addWidget(self.price_diff_table)

        # 添加标签页
        tab_widget.addTab(balance_tab, '账户余额')
        tab_widget.addTab(position_tab, '当前持仓')
        tab_widget.addTab(history_tab, '交易历史')
        tab_widget.addTab(price_diff_tab, '价差监控')

        # 初始化数据
        self.update_balance()
        self.update_positions()
        self.update_history()
        self.update_price_difference()

    def update_price_difference(self):
        try:
            # 获取现货深度数据
            spot_depth = self.client.get_order_book(symbol='ADAUSDT', limit=5)
            spot_bid = float(spot_depth['bids'][0][0])

            # 获取合约深度数据
            futures_depth = self.client.futures_coin_order_book(symbol='ADAUSD_250328', limit=5)
            futures_bid = float(futures_depth['bids'][0][0])

            # 计算价差百分比
            price_diff = ((futures_bid - spot_bid) / spot_bid) * 100

            # 更新表格
            self.price_diff_table.setItem(0, 0, QTableWidgetItem('ADA现货/合约'))
            self.price_diff_table.setItem(0, 1, QTableWidgetItem(f'{spot_bid:.8f}'))
            self.price_diff_table.setItem(0, 2, QTableWidgetItem(f'{futures_bid:.8f}'))
            self.price_diff_table.setItem(0, 3, QTableWidgetItem(f'{price_diff:.2f}%'))

        except Exception as e:
            QMessageBox.warning(self, '错误', f'获取价差数据失败：{str(e)}')

    def update_balance(self):
        try:
            # 获取账户信息
            account = self.client.get_account()
            balances = account['balances']

            # 过滤掉零余额的资产
            non_zero_balances = [b for b in balances if float(b['free']) > 0 or float(b['locked']) > 0]

            # 更新表格
            self.balance_table.setRowCount(len(non_zero_balances))
            for i, balance in enumerate(non_zero_balances):
                self.balance_table.setItem(i, 0, QTableWidgetItem(balance['asset']))
                self.balance_table.setItem(i, 1, QTableWidgetItem(balance['free']))
                self.balance_table.setItem(i, 2, QTableWidgetItem(balance['locked']))

        except Exception as e:
            QMessageBox.warning(self, '错误', f'获取余额失败：{str(e)}')

    def update_positions(self):
        try:
            # 获取所有持仓信息
            positions = self.client.futures_position_information()

            # 过滤掉空仓位
            active_positions = [p for p in positions if float(p['positionAmt']) != 0]

            # 更新表格
            self.position_table.setRowCount(len(active_positions))
            for i, pos in enumerate(active_positions):
                self.position_table.setItem(i, 0, QTableWidgetItem(pos['symbol']))
                self.position_table.setItem(i, 1, QTableWidgetItem(pos['positionAmt']))
                self.position_table.setItem(i, 2, QTableWidgetItem(pos['entryPrice']))
                self.position_table.setItem(i, 3, QTableWidgetItem(pos['markPrice']))

        except Exception as e:
            QMessageBox.warning(self, '错误', f'获取持仓失败：{str(e)}')

    def update_history(self):
        try:
            # 获取最近的交易历史
            trades = self.client.get_my_trades(limit=20, symbol="ADAUSDT")  # 最近20条交易记录

            # 更新表格
            self.history_table.setRowCount(len(trades))
            for i, trade in enumerate(trades):
                time = datetime.fromtimestamp(trade['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
                self.history_table.setItem(i, 0, QTableWidgetItem(time))
                self.history_table.setItem(i, 1, QTableWidgetItem(trade['symbol']))
                self.history_table.setItem(i, 2, QTableWidgetItem('买入' if trade['isBuyer'] else '卖出'))
                self.history_table.setItem(i, 3, QTableWidgetItem(trade['price']))
                self.history_table.setItem(i, 4, QTableWidgetItem(trade['qty']))

        except Exception as e:
            QMessageBox.warning(self, '错误', f'获取交易历史失败：{str(e)}')


def main():
    app = QApplication(sys.argv)
    window = BinanceAccountGUI()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
