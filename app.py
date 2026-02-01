import websocket
import json
import time
import hmac
import hashlib
import base64
import urllib.parse
import requests
from datetime import datetime, timedelta, timezone

# --- 核心配置信息 ---
TARGET_SYMBOL = "ALPHA_556USDT"
STREAM_NAME = "alpha_556usdt@aggTrade"
SOCKET_URL = "wss://nbstream.binance.com/w3w/wsa/stream"

# --- 报警策略配置 ---
# 1. 单笔数量门槛: 每一笔成交必须都大于这个数 (OWL)
SINGLE_ORDER_THRESHOLD = 500000 

# 2. 连续笔数门槛: 必须连续出现多少笔大于上述门槛的单子才报警
CONSECUTIVE_LIMIT = 30

# 3. 报警冷却时间 (秒)，防止满足条件后一直刷屏
ALERT_COOLDOWN = 60 

# --- 钉钉配置 ---
# ⚠️ 请务必替换为你的真实 Token 和 Secret
DINGTALK_ACCESS_TOKEN = "6a39d3acfeadca++++++++++7706cbd4110606ae9c40d7cffaf92d7f5e9ae19"
DINGTALK_SECRET = "SECeb36f7c708cd76204++++++++++++++788a8af3cd02c0f07baac5df9973baa1"

# --- 时区配置 (新增) ---
# 定义北京时间时区 (UTC+8)
BEIJING_TZ = timezone(timedelta(hours=8))

# --- 全局变量 ---
current_consecutive_count = 0
last_alert_time = 0

def send_dingtalk_alert(count, current_price, latest_qty):
    """发送钉钉带签名的报警消息"""
    try:
        timestamp = str(round(time.time() * 1000))
        secret_enc = DINGTALK_SECRET.encode('utf-8')
        string_to_sign = '{}\n{}'.format(timestamp, DINGTALK_SECRET)
        string_to_sign_enc = string_to_sign.encode('utf-8')
        hmac_code = hmac.new(secret_enc, string_to_sign_enc, digestmod=hashlib.sha256).digest()
        sign = urllib.parse.quote_plus(base64.b64encode(hmac_code))
        
        webhook_url = f"https://oapi.dingtalk.com/robot/send?access_token={DINGTALK_ACCESS_TOKEN}&timestamp={timestamp}&sign={sign}"
        
        # 获取当前的北京时间用于报警显示
        bj_time_str = datetime.now(BEIJING_TZ).strftime('%H:%M:%S')
        
        # 构造消息内容
        msg_content = (
            f"🚨 **OWL 连续大单报警** 🚨\n\n"
            f"⏱️ **北京时间**: {bj_time_str}\n"
            f"🔥 **触发条件**: 连续 **{count}** 笔交易数量 > {SINGLE_ORDER_THRESHOLD/1000:.0f}k\n"
            f"🌊 **最新一笔**: {latest_qty:,.0f} OWL\n"
            f"💰 **当前价格**: ${current_price:.6f}\n"
            f"💡 **提示**: 出现连续密集大单，主力正在通过 WSS 频繁成交，建议关注！"
        )
        
        payload = {
            "msgtype": "markdown",
            "markdown": {
                "title": "OWL 连续大单监控",
                "text": msg_content
            }
        }
        
        requests.post(webhook_url, json=payload)
        print(f"🔔 [钉钉通知已发送] 监测到连续 {count} 笔大单")
        
    except Exception as e:
        print(f"❌ 钉钉发送失败: {e}")

def check_consecutive_orders(quantity, price):
    """
    检查是否满足“连续 N 条记录每条数量都 > 500k”
    """
    global current_consecutive_count, last_alert_time
    current_time = time.time()
    
    # 核心判断逻辑
    if quantity > SINGLE_ORDER_THRESHOLD:
        current_consecutive_count += 1
        print(f"   >>> 🔥 连续大单计数: {current_consecutive_count}/{CONSECUTIVE_LIMIT}")
    else:
        # 如果中间断了一笔小单，计数器直接清零
        if current_consecutive_count > 0:
            print(f"   >>> ❄️ 大单中断 (此前连续 {current_consecutive_count} 笔)")
        current_consecutive_count = 0
        
    # 触发报警检查
    if current_consecutive_count >= CONSECUTIVE_LIMIT:
        # 检查冷却时间
        if current_time - last_alert_time > ALERT_COOLDOWN:
            print(f"\n🚀 达成目标! 连续 {current_consecutive_count} 笔大单!")
            send_dingtalk_alert(current_consecutive_count, price, quantity)
            last_alert_time = current_time

def on_message(ws, message):
    try:
        data_json = json.loads(message)
        
        if 'data' not in data_json:
            return

        payload = data_json['data']
        symbol = payload.get('s')
        
        if symbol != TARGET_SYMBOL:
            return

        price = float(payload.get('p', 0))
        quantity = float(payload.get('q', 0))
        timestamp = payload.get('T')
        is_buyer_maker = payload.get('m')
        
        # 转换时间用于显示 (强制转换为北京时间)
        # fromtimestamp 接收 timezone 参数，直接转为目标时区
        dt_object = datetime.fromtimestamp(timestamp / 1000, BEIJING_TZ)
        time_str = dt_object.strftime("%H:%M:%S")
        
        side = "🔴 卖出" if is_buyer_maker else "🟢 买入"
        
        # 打印单笔详情
        prefix = "🔥" if quantity > SINGLE_ORDER_THRESHOLD else "  "
        print(f"{prefix} [{time_str}] {side} | 价格: {price:.8f} | 数量: {quantity:,.0f}")
        
        # ---> 进入核心逻辑
        check_consecutive_orders(quantity, price)

    except Exception as e:
        print(f"解析错误: {e}")

def on_error(ws, error):
    print(f"Websocket 错误: {error}")

def on_close(ws, close_status_code, close_msg):
    print("连接已断开")

def on_open(ws):
    print(f"已连接，开始监控 {TARGET_SYMBOL}...")
    print(f"策略: 连续 {CONSECUTIVE_LIMIT} 笔交易，每笔数量都 > {SINGLE_ORDER_THRESHOLD/1000:.0f}k 则报警")
    
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [STREAM_NAME],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))
    print("-" * 60)

if __name__ == "__main__":
    # 使用 while True 循环作为简单的守护进程机制
    while True:
        try:
            # 启动 WebSocket
            ws = websocket.WebSocketApp(
                SOCKET_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            # run_forever 会阻塞在这里，直到连接断开
            ws.run_forever()
            
            # 如果运行到这里，说明连接断开了
            print("⚠️ 连接意外断开，5秒后尝试自动重连...")
            time.sleep(5)
            
        except KeyboardInterrupt:
            print("\n🛑 用户手动停止监控")
            break  # 只有按 Ctrl+C 才会真正退出循环
            
        except Exception as e:
            # 捕获其他所有异常，防止脚本崩溃
            print(f"❌ 发生严重错误: {e}")
            print("⚠️ 5秒后尝试自动重连...")
            time.sleep(5)
