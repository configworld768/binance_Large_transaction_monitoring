import websocket
import json
import time
import threading
import telebot # 引入 Telegram Bot 库
import requests # 引入 requests 用于获取 API
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime, timedelta, timezone

# --- 基础配置 ---
SOCKET_URL = "wss://nbstream.binance.com/w3w/wsa/stream"
API_URL = "https://www.binance.com/bapi/defi/v1/public/wallet-direct/buw/wallet/cex/alpha/all/token/list"

# --- 报警策略配置 ---
# 1. 单笔数量门槛
# ⚠️ 注意：不同代币价格差异大，50万个OWL可能只要2.5万U，但50万个WMTX可能要4万U。
# 如果发现WMTX报警太少，可以尝试调低这个数值，比如 100000
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
ALLOWED_CHAT_ID = "YOUR_CHAT_ID_HERE" 

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
        self.current_chat_id = ALLOWED_CHAT_ID
        
        # 动态监控目标 (启动时选择)
        self.target_symbol = ""    # 预期目标 (用于显示)
        self.stream_name = ""      # 订阅流名称
        self.token_display_name = "" # 显示名称

# 初始化全局状态和机器人
state = MonitorState()
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# ==========================================
#   API 获取逻辑 (4倍积分代币)
# ==========================================

def fetch_4x_tokens():
    """获取所有 4倍积分 (mulPoint=4) 的代币列表"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Clienttype": "web"
    }
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        if response.status_code != 200:
            return []
        
        data_json = response.json()
        token_list = data_json.get("data", [])
        
        target_tokens = []
        for token in token_list:
            # 筛选 mulPoint 为 4 的代币
            if str(token.get("mulPoint", 0)) == "4":
                target_tokens.append({
                    "symbol": token.get("symbol"),
                    "name": token.get("name"),
                    "alphaId": token.get("alphaId"),
                    "chain": token.get("chainName")
                })
        return target_tokens
    except Exception as e:
        print(f"❌ API 请求失败: {e}")
        return []

# ==========================================
#   监控核心逻辑 (运行在子线程)
# ==========================================

def send_telegram_alert(count, current_price, latest_qty, actual_symbol):
    """发送报警消息"""
    if not state.current_chat_id:
        return

    try:
        bj_time_str = datetime.now(BEIJING_TZ).strftime('%H:%M:%S')
        # 如果实际 Symbol 和显示名称不同，都显示一下
        display_name = state.token_display_name
        if actual_symbol and actual_symbol != state.target_symbol:
            display_name = f"{state.token_display_name} ({actual_symbol})"

        msg_content = (
            f"🚨 <b>{state.token_display_name} 连续大单报警</b> 🚨\n\n"
            f"⏱️ <b>北京时间</b>: {bj_time_str}\n"
            f"🔥 <b>触发条件</b>: 连续 <b>{count}</b> 笔交易数量 > {SINGLE_ORDER_THRESHOLD/1000:.0f}k\n"
            f"🌊 <b>最新一笔</b>: {latest_qty:,.0f}\n"
            f"💰 <b>当前价格</b>: ${current_price:.6f}\n"
            f"💡 <b>提示</b>: 监测到 {display_name} 流动性活跃，积分倍率 4X！"
        )
        bot.send_message(state.current_chat_id, msg_content, parse_mode="HTML")
        print(f"🔔 [通知已发送] {display_name} 连续 {count} 笔大单")
    except Exception as e:
        print(f"❌ 发送消息失败: {e}")

def check_consecutive_orders(quantity, price, actual_symbol):
    """检查连续大单逻辑"""
    current_time = time.time()
    
    if quantity > SINGLE_ORDER_THRESHOLD:
        state.consecutive_count += 1
        # 日志已经在 on_message 打印了，这里只打印计数状态
        print(f"   >>> 🔥 计数: {state.consecutive_count}/{CONSECUTIVE_LIMIT}")
    else:
        if state.consecutive_count > 0:
            print(f"   >>> ❄️ 中断 (此前 {state.consecutive_count})")
        state.consecutive_count = 0
        
    if state.consecutive_count >= CONSECUTIVE_LIMIT:
        if current_time - state.last_alert_time > ALERT_COOLDOWN:
            print(f"\n🚀 达成目标! 报警!")
            send_telegram_alert(state.consecutive_count, price, quantity, actual_symbol)
            state.last_alert_time = current_time

def on_message(ws, message):
    if not state.is_running:
        ws.close()
        return

    try:
        data_json = json.loads(message)
        if 'data' not in data_json: return

        payload = data_json['data']
        received_symbol = payload.get('s')
        price = float(payload.get('p', 0))
        quantity = float(payload.get('q', 0))

        # --- 核心修改：打印所有接收到的数据到控制台 ---
        # 即使数量很小也会打印，证明脚本活着
        prefix = "🔥" if quantity > SINGLE_ORDER_THRESHOLD else "  "
        print(f"{prefix} [{received_symbol}] Qty: {quantity:,.0f} | Price: {price}")
        
        # 只要流是对的，数据通常是对的。直接检查逻辑。
        check_consecutive_orders(quantity, price, received_symbol)

    except Exception as e:
        print(f"解析错误: {e}")

def on_error(ws, error):
    if state.is_running:
        print(f"Websocket 错误: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Websocket 连接已断开")

def on_open(ws):
    print(f"✅ 已连接 WSS，开始监控 {state.token_display_name} (流: {state.stream_name})...")
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [state.stream_name],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

def run_ws_loop():
    """WSS 守护线程循环"""
    while state.is_running:
        try:
            print(f"正在尝试连接 Binance WSS (Target: {state.token_display_name})...")
            state.ws = websocket.WebSocketApp(
                SOCKET_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            state.ws.run_forever(ping_interval=60, ping_timeout=10)
            
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
def handle_start_command(message):
    current_chat_id = str(message.chat.id)
    if ALLOWED_CHAT_ID and current_chat_id != str(ALLOWED_CHAT_ID):
        print(f"⚠️ 拒绝访问! 请将此 Chat ID 填入配置: {current_chat_id}")
        bot.reply_to(message, "⛔️ 你没有权限执行此操作。")
        return

    if state.is_running:
        bot.reply_to(message, f"⚠️ 监控已经在运行中了！\n当前目标: <b>{state.token_display_name}</b>", parse_mode="HTML")
        return

    msg = bot.reply_to(message, "🔄 正在扫描币安 4倍积分代币，请稍候...")
    tokens = fetch_4x_tokens()
    
    if not tokens:
        bot.edit_message_text(chat_id=message.chat.id, message_id=msg.message_id, text="❌ 未找到任何 4倍积分代币，或 API 请求失败。")
        return

    markup = InlineKeyboardMarkup()
    row_btns = []
    
    token_list_str = "🎉 <b>发现以下 4倍积分代币:</b>\n\n"
    
    for t in tokens:
        symbol = t['symbol']
        alpha_id = t['alphaId']
        token_list_str += f"🔹 <b>{symbol}</b> (ID: {alpha_id})\n"
        
        callback_data = f"select|{alpha_id}|{symbol}"
        row_btns.append(InlineKeyboardButton(symbol, callback_data=callback_data))
        
        if len(row_btns) == 3:
            markup.add(*row_btns)
            row_btns = []
            
    if row_btns:
        markup.add(*row_btns)

    token_list_str += "\n👇 <b>请点击下方按钮选择要监控的代币:</b>"
    bot.edit_message_text(chat_id=message.chat.id, message_id=msg.message_id, text=token_list_str, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith('select|'))
def handle_token_selection(call):
    _, alpha_id, symbol = call.data.split('|')
    
    if state.is_running:
        bot.answer_callback_query(call.id, "监控已经在运行中，请先停止！")
        return

    # 关键逻辑：清洗 alpha_id，确保只保留数字
    clean_alpha_id = str(alpha_id).replace("ALPHA_", "").replace("alpha_", "")
    
    state.target_symbol = f"ALPHA_{clean_alpha_id}USDT"
    state.stream_name = f"alpha_{clean_alpha_id}usdt@aggTrade"
    state.token_display_name = symbol
    
    state.is_running = True
    state.consecutive_count = 0
    state.last_alert_time = 0
    state.current_chat_id = call.message.chat.id

    state.thread = threading.Thread(target=run_ws_loop, daemon=True)
    state.thread.start()
    
    bot.answer_callback_query(call.id, f"已选择 {symbol}")
    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text=f"🟢 <b>监控已启动</b>\n\n🎯 <b>目标代币</b>: {symbol}\n🆔 <b>Alpha ID</b>: {clean_alpha_id}\n📝 <b>实际 Symbol</b>: {state.target_symbol}\n🌊 <b>流名称</b>: {state.stream_name}\n\n如需切换，请先发送 /stop_monitor",
        parse_mode="HTML"
    )
    print(f"启动监控: {symbol} (ID: {clean_alpha_id})")

@bot.message_handler(commands=['stop_monitor'])
def handle_stop(message):
    current_chat_id = str(message.chat.id)
    if ALLOWED_CHAT_ID and current_chat_id != str(ALLOWED_CHAT_ID):
        return

    if not state.is_running:
        bot.reply_to(message, "⚠️ 当前没有正在运行的监控任务。")
        return

    state.is_running = False
    state.consecutive_count = 0
    if state.ws:
        state.ws.close()
    
    bot.reply_to(message, f"🛑 <b>监控已停止</b> ({state.token_display_name})\n休息一下，等待下次指令。", parse_mode="HTML")
    print("收到指令: 停止监控")

@bot.message_handler(commands=['status'])
def handle_status(message):
    if state.is_running:
        status_text = f"🟢 运行中 (目标: {state.token_display_name})"
        last_alert = f"{int(time.time() - state.last_alert_time)}秒前" if state.last_alert_time > 0 else "无"
    else:
        status_text = "🔴 已停止"
        last_alert = "-"
        
    bot.reply_to(message, f"📊 <b>当前状态</b>: {status_text}\n最后报警: {last_alert}", parse_mode="HTML")

if __name__ == "__main__":
    print("🤖 Telegram 机器人已启动...")
    print("请在 Telegram 群组中发送 /start_monitor 获取代币列表")
    try:
        bot.infinity_polling()
    except Exception as e:
        print(f"Bot 轮询发生错误: {e}")
