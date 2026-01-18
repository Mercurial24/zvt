# -*- coding: utf-8 -*-
"""
QMT Forward Client for ZVT
Integrates with the external QMT HTTP RPC Server.
"""

import base64
import json
import pickle
import warnings
import requests

def _encode_value(v):
    if isinstance(v, RemoteObject):
        return {"__object_id": v._object_id}
    if isinstance(v, (list, tuple)):
        return [_encode_value(x) for x in v]
    if isinstance(v, dict):
        return {k: _encode_value(x) for k, x in v.items()}
    return v


class QMTForwardClient:
    def __init__(self, base_url="http://192.168.48.207:8000", token=None):
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._session = requests.Session()
        self._session.headers.update({"Content-Type": "application/json; charset=utf-8"})
        if self.token:
            self._session.headers.update({"X-Token": self.token})

    def _request(self, body):
        try:
            resp = self._session.post(self.base_url + "/rpc", json=body, timeout=60)
            if resp.status_code != 200:
                try:
                    return resp.json()
                except Exception:
                    return {"ok": False, "error": f"HTTP {resp.status_code}", "detail": resp.text}
            return resp.json()
        except requests.exceptions.Timeout:
            return {"ok": False, "error": "TimeoutError", "detail": "timed out"}
        except Exception as e:
            return {"ok": False, "error": type(e).__name__, "detail": str(e)}

    def _encode_args(self, args):
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
        body = {
            "path": path,
            "args": self._encode_args(list(args)),
            "kwargs": self._encode_args(kwargs),
        }
        return self._decode_result(self._request(body))

    def call_with_pickle_args(self, path, args, kwargs):
        body = {
            "path": path,
            "args_pickle_b64": base64.b64encode(pickle.dumps((args, kwargs))).decode(
                "ascii"
            ),
        }
        return self._decode_result(self._request(body))


class RemoteObject:
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
        try:
            body = {
                "object_id": self._object_id,
                "method": "__del__",
            }
            self._client._session.post(self._client.base_url + "/rpc", json=body, timeout=2)
        except Exception:
            pass

def get_xtdata_proxy(base_url="http://192.168.48.207:8000", token=None):
    class XtDataProxy:
        def __init__(self, c, t=None):
            self._client = QMTForwardClient(c, t)

        def __getattr__(self, name):
            def fn(*args, **kwargs):
                return self._client.call("xtquant.xtdata." + name, *args, **kwargs)
            return fn

    return XtDataProxy(base_url, token)

xtdata = get_xtdata_proxy()
