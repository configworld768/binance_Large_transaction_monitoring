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

# --- 报警策略配置 (已升级为动态阈值) ---
PRICE_PIVOT = 0.04               # 价格分界线

THRESHOLD_HIGH_PRICE = 500000    # 场景A: 价格 > 0.04 时，数量门槛 (500k)
THRESHOLD_LOW_PRICE = 1000000    # 场景B: 价格 <= 0.04 时，数量门槛 (1000k)

CONSECUTIVE_LIMIT = 30           # 连续笔数门槛
ALERT_COOLDOWN = 60              # 报警冷却时间 (秒)

# --- Telegram 配置 ---
# ⚠️ 请务必替换为你的真实 Token
TELEGRAM_BOT_TOKEN = "YOUR_BOT_TOKEN_HERE" 

# 允许的 Chat ID (可选，留空则不限制)
ALLOWED_CHAT_ID = "YOUR_CHAT_ID_HERE" 

# --- 时区配置 ---
BEIJING_TZ = timezone(timedelta(hours=8))

# --- 全局状态管理 ---
class MonitorState:
    def __init__(self):
        self.is_running = False
        self.ws = None
        self.thread = None
        self.current_chat_id = ALLOWED_CHAT_ID
        
        # 核心：使用字典存储多个监控目标
        # 结构: { "流名称": { "display_name": "OWL", "count": 0, "last_alert": 0, "symbol": "ALPHA_556USDT" } }
        self.active_monitors = {} 

# 初始化全局状态和机器人
state = MonitorState()
bot = telebot.TeleBot(TELEGRAM_BOT_TOKEN)

# ==========================================
#   API 获取逻辑
# ==========================================

def fetch_4x_tokens():
    """获取所有 4倍积分 (mulPoint=4) 的代币列表"""
    headers = {"User-Agent": "Mozilla/5.0", "Clienttype": "web"}
    try:
        response = requests.get(API_URL, headers=headers, timeout=10)
        if response.status_code != 200: return []
        
        data_json = response.json()
        target_tokens = []
        for token in data_json.get("data", []):
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
#   监控核心逻辑
# ==========================================

def send_telegram_alert(token_data, latest_qty, current_price, actual_symbol, used_threshold):
    """发送报警消息"""
    if not state.current_chat_id: return

    try:
        bj_time_str = datetime.now(BEIJING_TZ).strftime('%H:%M:%S')
        display_name = token_data['display_name']
        
        msg_content = (
            f"🚨 <b>{display_name} 连续大单报警</b> 🚨\n\n"
            f"⏱️ <b>北京时间</b>: {bj_time_str}\n"
            f"💰 <b>当前价格</b>: ${current_price:.6f}\n"
            f"🔥 <b>触发条件</b>: 连续 <b>{token_data['count']}</b> 笔交易数量 > {used_threshold/1000:.0f}k\n"
            f"🌊 <b>最新一笔</b>: {latest_qty:,.0f}\n"
            f"💡 <b>提示</b>: {display_name} 流动性活跃，建议关注！"
        )
        bot.send_message(state.current_chat_id, msg_content, parse_mode="HTML")
        print(f"🔔 [通知已发送] {display_name} 连续 {token_data['count']} 笔大单 (阈值: {used_threshold/1000:.0f}k)")
    except Exception as e:
        print(f"❌ 发送消息失败: {e}")

def process_trade_data(stream_name, quantity, price, actual_symbol):
    """处理单笔交易数据"""
    if stream_name not in state.active_monitors:
        return

    token_data = state.active_monitors[stream_name]
    current_time = time.time()
    
    # --- 动态阈值逻辑 ---
    if price > PRICE_PIVOT:
        current_threshold = THRESHOLD_HIGH_PRICE # > 0.04 使用 500k
    else:
        current_threshold = THRESHOLD_LOW_PRICE  # <= 0.04 使用 1000k
    
    if quantity > current_threshold:
        token_data['count'] += 1
        # 打印时带上当前的阈值，方便调试
        print(f"   >>> [{token_data['display_name']}] 🔥 计数: {token_data['count']}/{CONSECUTIVE_LIMIT} (Qty: {quantity:,.0f} > {current_threshold/1000:.0f}k)")
    else:
        if token_data['count'] > 0:
            print(f"   >>> [{token_data['display_name']}] ❄️ 中断 (此前 {token_data['count']}, 本笔 {quantity:,.0f} < {current_threshold/1000:.0f}k)")
        token_data['count'] = 0
        
    if token_data['count'] >= CONSECUTIVE_LIMIT:
        if current_time - token_data['last_alert'] > ALERT_COOLDOWN:
            print(f"\n🚀 [{token_data['display_name']}] 达成目标! 报警!")
            # 传递 current_threshold 以便在消息中显示正确的门槛
            send_telegram_alert(token_data, quantity, price, actual_symbol, current_threshold)
            token_data['last_alert'] = current_time

# ==========================================
#   WebSocket 事件处理
# ==========================================

def on_message(ws, message):
    if not state.is_running:
        ws.close()
        return

    try:
        data_json = json.loads(message)
        # 1. 检查是否是数据推送 (包含 data 和 stream 字段)
        if 'data' not in data_json or 'stream' not in data_json:
            return

        payload = data_json['data']
        stream_name = data_json['stream'] # 关键：通过 stream 字段识别是哪个代币的数据
        
        received_symbol = payload.get('s')
        price = float(payload.get('p', 0))
        quantity = float(payload.get('q', 0))

        # 打印心跳日志 (可选，如果不想要刷屏可以注释掉)
        # prefix = "🔥" 
        # # 这里为了简单判断打印前缀，可以粗略用 500k 判断，或者完全不打印
        # if quantity > 500000:
        #     print(f"{prefix} [{received_symbol}] Qty: {quantity:,.0f} | P: {price}")
        
        # 将数据分发给对应的处理逻辑
        process_trade_data(stream_name, quantity, price, received_symbol)

    except Exception as e:
        print(f"解析错误: {e}")

def on_error(ws, error):
    if state.is_running:
        print(f"Websocket 错误: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Websocket 连接已断开")

def on_open(ws):
    print("✅ WebSocket 连接成功，正在恢复订阅...")
    # 重新订阅所有活跃的代币 (用于断线重连)
    if state.active_monitors:
        streams = list(state.active_monitors.keys())
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": 1
        }
        ws.send(json.dumps(subscribe_message))
        print(f"已批量订阅: {streams}")

def run_ws_loop():
    """WSS 守护线程循环"""
    while state.is_running:
        try:
            print("正在连接 Binance WSS...")
            state.ws = websocket.WebSocketApp(
                SOCKET_URL,
                on_open=on_open,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            state.ws.run_forever(ping_interval=60, ping_timeout=10)
            
            if state.is_running:
                print("⚠️ 连接断开，5秒后尝试重连...")
                time.sleep(5)
        except Exception as e:
            print(f"线程异常: {e}")
            time.sleep(5)
    print("🛑 监控线程已完全退出")

def ensure_ws_running():
    """确保 WSS 线程正在运行"""
    if not state.is_running:
        state.is_running = True
        state.thread = threading.Thread(target=run_ws_loop, daemon=True)
        state.thread.start()
        print("启动 WSS 线程...")

# ==========================================
#   Telegram 指令处理
# ==========================================

@bot.message_handler(commands=['start_monitor'])
def handle_start_command(message):
    current_chat_id = str(message.chat.id)
    if ALLOWED_CHAT_ID and current_chat_id != str(ALLOWED_CHAT_ID):
        bot.reply_to(message, "⛔️ 无权操作")
        return
    
    # 更新当前 Chat ID
    state.current_chat_id = message.chat.id

    msg = bot.reply_to(message, "🔄 正在获取代币列表...")
    tokens = fetch_4x_tokens()
    
    if not tokens:
        bot.edit_message_text(chat_id=message.chat.id, message_id=msg.message_id, text="❌ 获取失败")
        return

    markup = InlineKeyboardMarkup()
    row_btns = []
    token_list_str = "🎉 <b>请选择要添加监控的代币 (支持多选):</b>\n\n"
    
    for t in tokens:
        symbol = t['symbol']
        alpha_id = t['alphaId']
        clean_id = str(alpha_id).replace("ALPHA_", "").replace("alpha_", "")
        
        # 标记当前是否已在监控中
        stream_name = f"alpha_{clean_id}usdt@aggTrade"
        status_icon = "✅" if stream_name in state.active_monitors else "⬜"
        
        callback_data = f"add|{clean_id}|{symbol}"
        row_btns.append(InlineKeyboardButton(f"{status_icon} {symbol}", callback_data=callback_data))
        
        if len(row_btns) == 3:
            markup.add(*row_btns)
            row_btns = []
            
    if row_btns: markup.add(*row_btns)

    bot.edit_message_text(chat_id=message.chat.id, message_id=msg.message_id, text=token_list_str, reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith('add|'))
def handle_add_token(call):
    _, alpha_id, symbol = call.data.split('|')
    stream_name = f"alpha_{alpha_id}usdt@aggTrade"
    
    ensure_ws_running()
    
    # 如果已经在监控，就不重复添加
    if stream_name in state.active_monitors:
        bot.answer_callback_query(call.id, f"{symbol} 已经在监控中了！")
        return

    # 添加到监控列表
    state.active_monitors[stream_name] = {
        "display_name": symbol,
        "count": 0,
        "last_alert": 0,
        "symbol": f"ALPHA_{alpha_id}USDT"
    }
    
    # 发送订阅指令
    if state.ws and state.ws.sock and state.ws.sock.connected:
        sub_msg = {"method": "SUBSCRIBE", "params": [stream_name], "id": int(time.time())}
        state.ws.send(json.dumps(sub_msg))
        print(f"动态订阅: {symbol}")
    
    bot.answer_callback_query(call.id, f"已添加 {symbol}")
    
    # 更新状态消息
    status_msg = "\n".join([f"🟢 {v['display_name']}" for v in state.active_monitors.values()])
    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text=f"📊 <b>当前监控列表:</b>\n\n{status_msg}\n\n👇 点击下方按钮继续添加，或使用 /stop_monitor 移除",
        parse_mode="HTML",
        reply_markup=call.message.reply_markup # 保持按钮不动，方便继续点
    )

@bot.message_handler(commands=['stop_monitor'])
def handle_stop_command(message):
    if not state.active_monitors:
        bot.reply_to(message, "⚠️ 当前没有监控任何代币。")
        return
        
    markup = InlineKeyboardMarkup()
    for stream, data in state.active_monitors.items():
        # 按钮回调: stop|流名称
        markup.add(InlineKeyboardButton(f"🛑 移除 {data['display_name']}", callback_data=f"stop|{stream}"))
    
    markup.add(InlineKeyboardButton("☠️ 停止所有监控", callback_data="stop|ALL"))
    
    bot.reply_to(message, "👇 <b>请选择要移除的监控项:</b>", reply_markup=markup, parse_mode="HTML")

@bot.callback_query_handler(func=lambda call: call.data.startswith('stop|'))
def handle_stop_token(call):
    _, target = call.data.split('|')
    
    if target == "ALL":
        streams = list(state.active_monitors.keys())
        state.active_monitors.clear()
        state.is_running = False # 停止线程
        if state.ws: state.ws.close()
        bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="🛑 已停止所有监控任务。")
        return

    # 移除单个
    if target in state.active_monitors:
        name = state.active_monitors[target]['display_name']
        del state.active_monitors[target]
        
        # 发送取消订阅指令
        if state.ws and state.ws.sock and state.ws.sock.connected:
            unsub_msg = {"method": "UNSUBSCRIBE", "params": [target], "id": int(time.time())}
            state.ws.send(json.dumps(unsub_msg))
            print(f"取消订阅: {name}")
            
        bot.answer_callback_query(call.id, f"已移除 {name}")
        
        # 刷新列表
        if state.active_monitors:
            current_list = "\n".join([f"🟢 {v['display_name']}" for v in state.active_monitors.values()])
            bot.edit_message_text(
                chat_id=call.message.chat.id, 
                message_id=call.message.message_id, 
                text=f"👇 <b>请选择要移除的监控项:</b>\n\n剩余监控:\n{current_list}", 
                reply_markup=call.message.reply_markup,
                parse_mode="HTML"
            )
        else:
            bot.edit_message_text(chat_id=call.message.chat.id, message_id=call.message.message_id, text="🛑 监控列表已清空。")
            state.is_running = False # 如果空了，也可以选择不关线程，看需求。这里为了省资源可以关。

@bot.message_handler(commands=['status'])
def handle_status(message):
    if not state.active_monitors:
        bot.reply_to(message, "🔴 当前未监控任何代币。")
        return
        
    msg = "📊 <b>正在监控中:</b>\n\n"
    for stream, data in state.active_monitors.items():
        last_alert = f"{int(time.time() - data['last_alert'])}秒前" if data['last_alert'] > 0 else "无"
        msg += f"🔹 <b>{data['display_name']}</b> (连续计数: {data['count']}, 上次报警: {last_alert})\n"
        
    bot.reply_to(message, msg, parse_mode="HTML")

if __name__ == "__main__":
    print("🤖 Telegram 机器人已启动 (支持动态阈值)...")
    try:
        bot.infinity_polling()
    except Exception as e:
        print(f"Bot 错误: {e}")
