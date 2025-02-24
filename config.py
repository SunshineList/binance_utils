API_KEY=""
API_SECRET=""

TEST_API_KEY = ""
TEST_API_SECRET = ""

TEST_FUTURE_API_KEY = ""
TEST_FUTURE_API_SECRET = ""

IS_DEBUG = True  # 是否使用测试环境

# type类型 futures合约 spot现货
PAIR_CONFIGS = [
    {
        'pair1': {'symbol': 'BTCUSD_250627', 'type': 'futures'},
        'pair2': {'symbol': 'BTCUSD_PERP', 'type': 'futures'},
        'description': 'BTC-BTCUSD_250627vsBTCUSD_PERP',
        'asset': 'BTC' # 保证金币种
    }
]

# 阈值
PERCENTAGE_DEFAULT = 3.0  # 中间值
TOP_PERCENTAGE_DEFAULT = 3.3 # 最大值
LOW_PERCENTAGE_DEFAULT = 2.7 # 最小值

PERCENTAGE_DIFF_DEFAULT = 0.1 # 差值 当在这个差值附近处理数据的时间会变成DYNAMIC_INTERVAL

# 买卖次数 每次停止后记录最后一次的买卖次数
TOP_NUM_VALUE = 15
MID_NUM_VALUE = 10 # 买卖交易起始值
MIN_NUM_VALUE = 5

# 获取数据时间间隔 单位s
INTERVAL = 10
DYNAMIC_INTERVAL = 0.1  # 动态间隔时间

# 差值与获取到的价格差异 买+ 卖-
PRICE_DIFF = 100

#进入委托下单后重试次数
RETRY_TIMES = 5

#数量
QUANTITY = 0.002

# 保证金阈值
MARGIN_LOW_VALUE = 0.005

# 杠杆倍数
LEVERAGE = 10

# 全仓模式
MARGIN_TYPE = "CROSSED"

# tg通知
TG_TOKEN = ""
MESSAGE_ID = ""

# 打印频次控制
PRINT_COUNT = 100