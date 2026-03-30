# QMT 转发服务（Windows ↔ Ubuntu）

> **【🤖 AI 助手上下文说明 / AI Context】**
> **核心架构**：由于 Linux 无法安装 `xtquant`，本系统采用 **HTTP RPC 远程透传** 机制。Linux 侧所有 QMT 逻辑均已重构，**严禁在 Linux 尝试直接 `import xtquant`**。
> **详细指南**：请优先查阅 [QMT_Linux集成手册.md](QMT_Linux集成手册.md) 了解当前架构和代码调用规范。
> **底层驱动**：`src/zvt/broker/qmt/qmt_remote.py` 是通信核心，负责将本地调用代理到 Windows 端。

> **背景**：这是一个基于 HTTP 的轻量级 RPC（远程过程调用）机制，用于在 Ubuntu 上穿透调用 Windows 上的 QMT（xtquant）功能（量化策略与数据拉取）。
> **核心修复（请勿破坏以下机制）**：
> 1. **序列化机制**：服务端 `_try_serialize_result` 中不要对 `json.dumps` 使用 `default=str`，否则会使 Pandas DataFrame/Numpy 数组变成无用的排版字符串。不支持 JSON 的对象会自动降级触发 `pickle` 序列化进行二进制传输。
> 2. **内存自动回收**：客户端 `RemoteObject` 实现了 `__del__` 方法，当代理对象被回收时，会主动向服务端发请求释放 `_remote_objects` 中的真实对象引用，防止内存泄漏。
> 3. **多线程非阻塞**：服务端使用了 `ThreadingHTTPServer`，避免单个耗时 QMT 请求阻塞整个通信。
> 4. **递归解析**：客户端在参数序列化 `_encode_value` 等函数中，已全面支持 `list` 和 `tuple` 的判断和遍历，确保发送请求时嵌套的 `RemoteObject` 能够被正确转为 `__object_id`。

在 **Windows** 上运行转发服务，使 **Ubuntu**（或其它机器）能通过「模块名 + 函数/类名 + 参数」穿透调用本机上的 QMT（xtquant），用于量化策略与数据拉取。

## 目录结构

- `config.py` - Windows 端配置（监听地址、端口、可选 Token）
- `server.py` - Windows 端转发服务（在装 QMT 的机器上运行）
- `client.py` - Ubuntu 端客户端（透明调用接口）
- `requirements.txt` - 依赖说明

## 使用步骤

### 1. Windows 端（本机，已安装 QMT）

1. 确保已安装 QMT 及 xtquant 环境。
2. 修改 `config.py` 中的 `HOST`、`PORT`（默认 `0.0.0.0:8000`），如需鉴权可设置 `AUTH_TOKEN`。
3. 在项目根目录下执行：
   ```bash
   cd qmt_forward
   python server.py
   ```
4. 若 Ubuntu 与 Windows 不在同一台机器，请把 Windows 防火墙放行 `8000` 端口，或将 `HOST` 改为 `0.0.0.0` 以接受外网连接（仅建议内网使用）。

### 2. Ubuntu 端（策略与数据拉取）

1. 将本目录中的 `client.py` 拷贝到 Ubuntu 项目中使用。
2. 把 `QMTForwardClient` 的 `base_url` 改为你 Windows 机器的 IP，例如：
   ```python
   from client import QMTForwardClient, get_xtdata_proxy, create_forward_trader

   BASE = "http://192.168.48.207:8000"  # Windows 机 IP

   # 方式一：直接按「模块.函数」调用
   client = QMTForwardClient(BASE)
   full_tick = client.call("xtquant.xtdata.get_full_tick", ["600000.SH"])

   # 方式二：xtdata 代理，写法接近本地
   xt = get_xtdata_proxy(BASE)
   data = xt.get_market_data(["close"], ["600000.SH"], period="1m", start_time="20230701")

   # 方式三：创建 Trader 并调用方法（返回远程对象代理）
   trader = create_forward_trader(BASE, r"C:\国金证券QMT交易端\userdata_mini", 123456)
   trader.start()
   trader.connect()
   # 订阅账户等后续调用与本地一致
   ```

## 协议说明

- 请求：`POST /rpc`，JSON 体：
  - **按路径调用**：`{"path": "xtquant.xtdata.get_full_tick", "args": [...], "kwargs": {...}}`
  - **按远程对象调用**：`{"object_id": "uuid", "method": "connect", "args": [], "kwargs": {}}`
- 返回：`{"ok": true, "type": "json"|"pickle"|"object_id", ...}`，客户端会自动反序列化或返回远程对象代理。
- 参数/返回值若不可 JSON 序列化，会使用 pickle（由服务端/客户端自动处理）；无法序列化的对象会以 `object_id` 形式保存在 Windows 端，供后续方法调用。

## 无法序列化的对象在 Ubuntu 上怎么用？

当 Windows 端返回的是**无法序列化**的对象（例如 `XtQuantTrader` 实例）时，客户端会得到一只 **`RemoteObject`**，你在 Ubuntu 上**就当成本地对象用**即可：

1. **直接调方法**：和本地写法的习惯一致。
   ```python
   from client import QMTForwardClient

   client = QMTForwardClient("http://192.168.48.207:8000")
   # 创建 Trader 得到的是 RemoteObject，不是真实对象，但用法一样
   trader = client.call("xtquant.xttrader.XtQuantTrader", min_path, session_id)
   trader.start()
   trader.connect()
   res = trader.subscribe(acc)  # 返回值若是可序列化的会直接得到
   ```

2. **作为参数传给别的调用**：也可以直接把 `RemoteObject` 当参数传，客户端会自动把它转成 `object_id` 发给 Windows，服务端会解析成真实对象再执行。
   ```python
   # 例如某函数需要传入 trader 实例
   result = client.call("some.module.some_function", trader, other_arg)
   ```

3. **需要传复杂对象（如 StockAccount）时**：若该类型在 Ubuntu 上不存在或不能 JSON 序列化，可以改为在 Windows 上创建再拿引用，例如：
   ```python
   acc = client.call("xtquant.xttype.StockAccount", "8885660116")  # 在 Windows 上创建
   trader.subscribe(acc)  # acc 若是 RemoteObject 会自动按 object_id 传
   ```

## 注意事项与局限性（重要）

- **无法使用 Callback（回调函数）**：QMT 中的订阅机制和交易模块严重依赖异步回调（例如订阅实时行情传入 `on_stock_trade`），但在这套 HTTP RPC 机制下，客户端无法将自己的 Python 函数序列化传给 Windows。
- **只能使用拉取/轮询（Polling）**：因此通过客户端只能主动拉取（例如主动调用 `get_full_tick`、`query_stock_orders` 等），**无法通过订阅接收推送**。
- **解决方案**：如果你的策略极度依赖实时订阅事件驱动，需要把包含回调的策略主体直接运行在 Windows 机器上；或者在 Windows 上二次开发，借助 Redis / MQ / WebSocket 等技术进行消息的异步推流桥接。

## 安全提示

- 本服务会在 Windows 上执行任意传入的模块路径与参数，**仅适合在内网或完全可信的环境使用**。
- 若需简单鉴权，在 `config.py` 中设置 `AUTH_TOKEN`，并在客户端构造 `QMTForwardClient(base_url, token=...)`。

## 进阶架构：HTTP RPC 与 Redis 的双通道协同

在成熟的跨机量化架构中，通常采用 **左手 HTTP（主动） + 右手 Redis（被动）** 的黄金组合。当前的 `qmt_forward` (HTTP RPC) 适合“一问一答”同步拉取历史状态，而对于高频异步推送（如实盘回调回调），在 Ubuntu 与 Windows 之间引入一层 Redis 消息队列是业界标准实践。

### 1. 两者的适用场景对比

* **HTTP RPC (`qmt_forward` 机制)**
  * **适用场景**：获取历史数据（`get_market_data`）、下达交易指令单（如发单并等待返回 order_id）、主动查询资金及持仓。
  * **特点**：客户端请求 -> 服务端执行 -> 响应返回，原生等待并拿到结果。此行为简单直接，不需要 Redis 即可在两块网卡间流畅获取大数据对象（如 Pandas DataFrame）。
* **Redis Pub/Sub (发布/订阅 机制)**
  * **适用场景**：基于事件监听的高频 Tick 推送、实盘状态与成交回报监听（如 `on_stock_trade`、`on_quote`）。
  * **特点**：应对突发、爆发量大和单向的消息特性。Windows 充当发布方，将数据无脑推给 Redis 不会引起主进程阻塞，Ubuntu 在后台随时监听，将“回调机制”实现完美解耦跨平台。

### 2. Redis 方案落地细节与优势

* **为什么选择 Redis？** 纯内存读写，是处理这类毫秒级 Tick 推送的最佳利刃。虽然称作“内存中间件”，但因其在 Pub/Sub 模式下并不进行数据的持久化存储，处理消息均为“立刻分发”，因此一台空载（或只进行转发操作的） Redis 进程几乎不随数据量增加而扩大内存吃紧（长年仅占用几 MB）。
* **部署简易**：在 Ubuntu 环境中只需 `sudo apt update && sudo apt install redis-server -y` 极简安装。Windows 平台不需启建服务，仅需在 Python 运行环境中 `pip install redis`。

### 3. 具体实施代码参考 (基于 Redis)

在保留现有 HTTP RPC 透明调用的基础上，你可以**不修改** `client.py` 内部逻辑，只需在 Windows 挂载额外的回调代理即可。

**Windows 端（新建一个文件如 callback_handler.py）:**

```python
import redis
from xtquant.xttrader import XtQuantTraderCallback

class MyRedisCallback(XtQuantTraderCallback):
    def __init__(self, redis_host, redis_port):
        # 指向 Ubuntu (或公用局域网内) 的 Redis
        self.rd = redis.Redis(host=redis_host, port=redis_port, db=0)
        
    def on_stock_trade(self, trade):
        # 发生交易时，将信息推送到 Redis 的 qmt_trade_events 频道
        data = {"stock": trade.stock_code, "price": trade.traded_price, "volume": trade.traded_volume}
        self.rd.publish("qmt_trade_events", str(data))

def register_redis_callback(trader, redis_host, redis_port):
    """供 Ubuntu 远程穿透调用的注册函数"""
    cb = MyRedisCallback(redis_host, redis_port)
    trader.register_callback(cb)
    return True
```

**Ubuntu 端策略脚本:**

```python
from client import QMTForwardClient, create_forward_trader

BASE = "http://192.168.48.207:8000"
client = QMTForwardClient(BASE)

# 1. 初始化并连接远端 trader
trader = create_forward_trader(BASE, r"C:\国金证券QMT交易端\userdata_mini", 123456)
trader.start()
trader.connect()

# 2. 通过 RPC 穿透，让 Windows 在本地挂载上面写的 RedisCallback
# (假设 Ubuntu IP 是 192.168.48.100)
client.call("callback_handler.register_redis_callback", trader, "192.168.48.100", 6379)
```

**Ubuntu 端监听服务 (后台常驻):**

```python
import redis

# 监听 Ubuntu 本机的 Redis 服务
r = redis.Redis(host='localhost', port=6379)
p = r.pubsub()
p.subscribe('qmt_trade_events')

print("开始收听 Windows 的 QMT 实盘推送...")
for message in p.listen():
    if message['type'] == 'message':
        print("接收到回调数据:", message['data'])
        # 触发后续的交易策略 / 通知
```
