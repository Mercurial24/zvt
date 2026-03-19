# -*- coding: utf-8 -*-
"""
QMT 转发客户端 - Ubuntu / 任意远程端
通过「模块名+函数/类名+参数」穿透调用 Windows 上的 QMT（xtquant 等），
接口与本地调用一致。
"""

import base64
import json
import pickle
import urllib.request
import urllib.error
import warnings


def _encode_value(v):
    """把参数里的 RemoteObject 转成服务端可识别的 __object_id 引用。"""
    if isinstance(v, RemoteObject):
        return {"__object_id": v._object_id}
    if isinstance(v, (list, tuple)):
        return [_encode_value(x) for x in v]
    if isinstance(v, dict):
        return {k: _encode_value(x) for k, x in v.items()}
    return v


class QMTForwardClient:
    """穿透调用 Windows 端 QMT 的客户端。"""

    def __init__(self, base_url="http://192.168.48.207:8000", token=None):
        """
        base_url: Windows 转发服务地址，例如 http://192.168.48.207:8000
        token: 若服务端配置了 AUTH_TOKEN，此处填写相同 token
        """
        self.base_url = base_url.rstrip("/")
        self.token = token

    def _request(self, body):
        req = urllib.request.Request(
            self.base_url + "/rpc",
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        if self.token:
            req.add_header("X-Token", self.token)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8")
                return json.loads(err_body)
            except Exception:
                return {"ok": False, "error": str(e), "detail": None}
        except Exception as e:
            return {"ok": False, "error": type(e).__name__, "detail": str(e)}

    def _encode_args(self, args):
        """把 args（list 或 dict）里可能含有的 RemoteObject 转成 __object_id。"""
        if isinstance(args, (list, tuple)):
            return [_encode_value(x) for x in args]
        if isinstance(args, dict):
            return {k: _encode_value(v) for k, v in args.items()}
        return args

    def _decode_result(self, data):
        if not data.get("ok"):
            raise RuntimeError(
                data.get("error", "Unknown") + ": " + str(data.get("detail", ""))
            )
        t = data.get("type", "json")
        if t == "json":
            return data.get("result")
        if t == "pickle":
            raw = base64.b64decode(data["result_base64"])
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=DeprecationWarning, message=".*numpy.*")
                return pickle.loads(raw)
        if t == "object_id":
            return RemoteObject(self, data["object_id"])
        raise ValueError("未知响应类型: " + str(t))

    def call(self, path, *args, **kwargs):
        """
        穿透调用 Windows 上的模块.函数 或 模块.类。
        例如: client.call("xtquant.xtdata.get_market_data", ["close"], ["600000.SH"], period="1d", ...)
        返回值若是无法序列化的对象，会得到 RemoteObject，直接对其调方法即可。
        """
        body = {
            "path": path,
            "args": self._encode_args(list(args)),
            "kwargs": self._encode_args(kwargs),
        }
        return self._decode_result(self._request(body))

    def call_with_pickle_args(self, path, args, kwargs):
        """当参数含不可 JSON 序列化的对象时，用 pickle 序列化后发送。"""
        body = {
            "path": path,
            "args_pickle_b64": base64.b64encode(pickle.dumps((args, kwargs))).decode(
                "ascii"
            ),
        }
        return self._decode_result(self._request(body))


class RemoteObject:
    """
    远程对象代理：Windows 上无法序列化的对象在 Ubuntu 端会变成 RemoteObject。
    用法：和本地对象一样直接调方法即可，例如：
        trader = client.call("xtquant.xttrader.XtQuantTrader", path, session_id)
        trader.start()
        trader.connect()
        trader.subscribe(acc)
    若要把该对象作为参数传给其它调用，直接传即可，客户端会自动转为 object_id。
    """

    def __init__(self, client, object_id):
        self._client = client
        self._object_id = object_id

    def __getattr__(self, name):
        def method(*args, **kwargs):
            body = {
                "object_id": self._object_id,
                "method": name,
                "args": self._client._encode_args(args),
                "kwargs": self._client._encode_args(kwargs),
            }
            return self._client._decode_result(self._client._request(body))

        return method

    def __del__(self):
        """当本地代理对象被回收时，通知服务端释放真实对象，防止内存泄漏"""
        try:
            body = {
                "object_id": self._object_id,
                "method": "__del__",
            }
            req = urllib.request.Request(
                self._client.base_url + "/rpc",
                data=json.dumps(body).encode("utf-8"),
                headers={"Content-Type": "application/json; charset=utf-8"},
                method="POST",
            )
            if self._client.token:
                req.add_header("X-Token", self._client.token)
            urllib.request.urlopen(req, timeout=2)
        except Exception:
            pass


# ---------- 便捷封装 ----------

def create_forward_trader(base_url, min_path, session_id, token=None):
    """
    在 Windows 上创建 XtQuantTrader 并返回远程代理：
    trader = create_forward_trader("http://192.168.48.207:8000", r"C:\\...\\userdata_mini", 123456)
    trader.start()
    trader.connect()
    trader.subscribe(acc)
    """
    client = QMTForwardClient(base_url, token)
    return client.call("xtquant.xttrader.XtQuantTrader", min_path, session_id)


def get_xtdata_proxy(base_url, token=None):
    """
    获取 xtdata 的穿透代理：
    xt = get_xtdata_proxy("http://192.168.48.207:8000")
    full_tick = xt.get_full_tick(["600000.SH"])
    """
    class XtDataProxy:
        def __init__(self, c, t=None):
            self._client = QMTForwardClient(c, t)

        def __getattr__(self, name):
            def fn(*args, **kwargs):
                return self._client.call("xtquant.xtdata." + name, *args, **kwargs)
            return fn

    return XtDataProxy(base_url, token)


if __name__ == "__main__":
    BASE = "http://192.168.48.207:8000"
    client = QMTForwardClient(BASE)
    # 数据量很小的示例：1 只股票、1 个字段、约 1～2 天日线
    try:
        result = client.call(
            "xtquant.xtdata.get_market_data",
            ["close"],
            ["600000.SH"],
            period="1d",
            start_time="20250210",
            end_time="20250212",
        )
        print("get_market_data 结果:", result)
    except Exception as e:
        print("请先在本机启动 server.py，错误:", e)
