import websocket
import json
import time
import threading
import telebot # 引入 Telegram Bot 库
from datetime import datetime, timedelta, timezone

# --- 核心配置信息 ---
TARGET_SYMBOL = "ALPHA_556USDT"
STREAM_NAME = "alpha_556usdt@aggTrade"
SOCKET_URL = "wss://nbstream.binance.com/w3w/wsa/stream"

# --- 报警策略配置 ---
# 1. 单笔数量门槛
SINGLE_ORDER_THRESHOLD = 500000 
# 2. 连续笔数门槛
CONSECUTIVE_LIMIT = 30
# 3. 报警冷却时间 (秒)
ALERT_COOLDOWN = 60 

# --- Telegram 配置 ---
# ⚠️ 请务必替换为你的真实 Token
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE" 

# 获取 Chat ID 的逻辑变更：
# 在互动模式下，机器人会自动获取发指令的群组 ID，
# 但为了防止陌生人控制，建议填入允许的 CHAT_ID 白名单（可选）
ALLOWED_CHAT_ID = "YOUR_CHAT_ID_HERE" # 如果不想限制，可以留空或注释掉相关检查逻辑

# --- 时区配置 ---
BEIJING_TZ = timezone(timedelta(hours=8))

# --- 全局状态管理 ---
class MonitorState:
    def __init__(self):
        self.is_running = False
        self.ws = None
        self.thread = None
        self.consecutive_count = 0
        self.last_alert_time = 0
        self.current_chat_id = ALLOWED_CHAT_ID # 记录当前需要发送报警的 Chat ID

# 初始化全局状态和机器人
state = MonitorState()
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# ==========================================
#   监控核心逻辑 (运行在子线程)
# ==========================================

def send_telegram_alert(count, current_price, latest_qty):
    """发送报警消息 (使用 Bot 实例发送)"""
    if not state.current_chat_id:
        return

    try:
        bj_time_str = datetime.now(BEIJING_TZ).strftime('%H:%M:%S')
        msg_content = (
            f"🚨 <b>OWL 连续大单报警</b> 🚨\n\n"
            f"⏱️ <b>北京时间</b>: {bj_time_str}\n"
            f"🔥 <b>触发条件</b>: 连续 <b>{count}</b> 笔交易数量 > {SINGLE_ORDER_THRESHOLD/1000:.0f}k\n"
            f"🌊 <b>最新一笔</b>: {latest_qty:,.0f} OWL\n"
            f"💰 <b>当前价格</b>: ${current_price:.6f}\n"
            f"💡 <b>提示</b>: 出现连续密集大单，流动性极佳，建议操作！"
        )
        bot.send_message(state.current_chat_id, msg_content, parse_mode="HTML")
        print(f"🔔 [通知已发送] 连续 {count} 笔大单")
    except Exception as e:
        print(f"❌ 发送消息失败: {e}")

def check_consecutive_orders(quantity, price):
    """检查连续大单逻辑"""
    current_time = time.time()
    
    if quantity > SINGLE_ORDER_THRESHOLD:
        state.consecutive_count += 1
        print(f"   >>> 🔥 计数: {state.consecutive_count}/{CONSECUTIVE_LIMIT}")
    else:
        if state.consecutive_count > 0:
            print(f"   >>> ❄️ 中断 (此前 {state.consecutive_count})")
        state.consecutive_count = 0
        
    if state.consecutive_count >= CONSECUTIVE_LIMIT:
        if current_time - state.last_alert_time > ALERT_COOLDOWN:
            print(f"\n🚀 达成目标! 报警!")
            send_telegram_alert(state.consecutive_count, price, quantity)
            state.last_alert_time = current_time

def on_message(ws, message):
    if not state.is_running: # 双重保险，如果停止了就不处理
        ws.close()
        return

    try:
        data_json = json.loads(message)
        if 'data' not in data_json: return

        payload = data_json['data']
        if payload.get('s') != TARGET_SYMBOL: return

        price = float(payload.get('p', 0))
        quantity = float(payload.get('q', 0))
        
        # 简单打印日志
        # prefix = "🔥" if quantity > SINGLE_ORDER_THRESHOLD else "  "
        # print(f"{prefix} Qty: {quantity:,.0f}")
        
        check_consecutive_orders(quantity, price)

    except Exception as e:
        print(f"解析错误: {e}")

def on_error(ws, error):
    if state.is_running: # 只有在运行时才报错，手动关闭时的报错忽略
        print(f"Websocket 错误: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Websocket 连接已断开")

def on_open(ws):
    print(f"✅ 已连接 WSS，开始监控 {TARGET_SYMBOL}...")
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [STREAM_NAME],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

def run_ws_loop():
    """WSS 守护线程循环"""
    while state.is_running:
        try:
            print("正在尝试连接 Binance WSS...")
            # 建立连接，设置 ping_interval 防止自动断开
            state.ws = websocket.WebSocketApp(
                SOCKET_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            state.ws.run_forever(ping_interval=60, ping_timeout=10)
            
            # 如果 run_forever 退出且 status 仍为 True，说明是异常断开，需要重连
            if state.is_running:
                print("⚠️ 连接异常断开，5秒后重连...")
                time.sleep(5)
        except Exception as e:
            print(f"线程异常: {e}")
            time.sleep(5)
    print("🛑 监控线程已完全退出")

# ==========================================
#   Telegram 机器人指令处理 (主线程)
# ==========================================

@bot.message_handler(commands=['start_monitor'])
def handle_start(message):
    # 获取当前的 Chat ID
    current_chat_id = str(message.chat.id)
    
    # 安全检查：只允许特定群组或个人控制 (如果配置了 ALLOWED_CHAT_ID)
    if ALLOWED_CHAT_ID and current_chat_id != str(ALLOWED_CHAT_ID):
        print(f"⚠️ 拒绝访问! 请将此 Chat ID 填入配置: {current_chat_id}")
        bot.reply_to(message, "⛔️ 你没有权限执行此操作。")
        return

    if state.is_running:
        bot.reply_to(message, "⚠️ 监控已经在运行中了！")
        return

    # 更新状态
    state.is_running = True
    state.current_chat_id = message.chat.id # 更新为当前发送指令的群组
    
    # 启动监控线程
    state.thread = threading.Thread(target=run_ws_loop, daemon=True)
    state.thread.start()
    
    # 修改 parse_mode 为 HTML，避免下划线报错
    bot.reply_to(message, f"🟢 <b>监控已启动</b>\n目标: {TARGET_SYMBOL}\n策略: 连续 {CONSECUTIVE_LIMIT} 笔 > {SINGLE_ORDER_THRESHOLD/1000:.0f}k", parse_mode="HTML")
    print(f"收到指令: 启动监控 (Chat ID: {message.chat.id})")

@bot.message_handler(commands=['stop_monitor'])
def handle_stop(message):
    # 获取当前的 Chat ID
    current_chat_id = str(message.chat.id)
    
    if ALLOWED_CHAT_ID and current_chat_id != str(ALLOWED_CHAT_ID):
        print(f"⚠️ 拒绝访问! 请将此 Chat ID 填入配置: {current_chat_id}")
        return

    if not state.is_running:
        bot.reply_to(message, "⚠️ 当前没有正在运行的监控任务。")
        return

    # 更新状态
    state.is_running = False
    
    # 强制关闭 WebSocket 连接以打破 run_forever 的阻塞
    if state.ws:
        state.ws.close()
    
    # 修改 parse_mode 为 HTML
    bot.reply_to(message, "🛑 <b>监控已停止</b>\n休息一下，等待下次指令。", parse_mode="HTML")
    print("收到指令: 停止监控")

@bot.message_handler(commands=['status'])
def handle_status(message):
    status_text = "🟢 运行中" if state.is_running else "🔴 已停止"
    # 修改 parse_mode 为 HTML
    bot.reply_to(message, f"📊 <b>当前状态</b>: {status_text}\n最后报警间隔: {int(time.time() - state.last_alert_time)}秒前", parse_mode="HTML")

# ==========================================
#   主程序入口
# ==========================================

if __name__ == "__main__":
    print("🤖 Telegram 机器人已启动，正在等待指令...")
    print("请在 Telegram 群组中发送 /start_monitor 或 /stop_monitor")
    
    # 启动机器人轮询 (这将阻塞主线程)
    try:
        bot.infinity_polling()
    except Exception as e:
        print(f"Bot 轮询发生错误: {e}")
