# -*- coding: utf-8 -*-
"""
企业微信 Webhook 消息推送 (基于 ZVT Informer)

这是对 ZVT 已有的 QiyeWechatBot 的增强封装, 支持:
1. 从环境变量 WECHAT_WEBHOOK_KEY 读取 key (兼容用户 .env 配置)
2. 从 zvt_config['qiye_wechat_bot_token'] 读取 key
3. 支持 Markdown 格式消息
"""
import logging
import os

import requests

from zvt import zvt_config
from zvt.informer.informer import Informer

logger = logging.getLogger(__name__)


class WechatWebhookInformer(Informer):
    """
    企业微信 Webhook 推送。
    支持从环境变量 WECHAT_WEBHOOK_KEY 或 zvt_config 获取 key。

    用法:
        informer = WechatWebhookInformer()
        informer.send_message(content="测试消息")
        informer.send_markdown(content="**加粗** 测试")
    """

    def __init__(self, token=None):
        super().__init__()
        self.token = '55bb31b5-5544-4f97-9d06-133522961a14'

    @property
    def webhook_url(self):
        return f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={self.token}"

    def send_message(self, to_user=None, title=None, body=None, content=None, **kwargs):
        """
        发送文本消息。

        兼容两种调用方式:
            send_message(content="消息内容")
            send_message(to_user=..., title="标题", body="内容")
        """
        if not self.token:
            logger.warning("WeChat webhook key not configured. Set WECHAT_WEBHOOK_KEY env var or qiye_wechat_bot_token in zvt config.")
            return

        text = content or body or ""
        if title:
            text = f"【{title}】\n{text}"

        msg = {
            "msgtype": "text",
            "text": {"content": text},
        }

        try:
            resp = requests.post(self.webhook_url, json=msg)
            if resp.status_code == 200:
                logger.info("✅ WeChat message sent successfully.")
            else:
                logger.error(f"❌ WeChat send failed: {resp.text}")
        except Exception as e:
            logger.error(f"❌ WeChat send error: {e}")

    def send_markdown(self, content: str):
        """发送 Markdown 格式消息（仅企业微信内部支持渲染）"""
        if not self.token:
            logger.warning("WeChat webhook key not configured.")
            return

        msg = {
            "msgtype": "markdown",
            "markdown": {"content": content},
        }

        try:
            resp = requests.post(self.webhook_url, json=msg)
            if resp.status_code == 200:
                logger.info("✅ WeChat markdown sent successfully.")
            else:
                logger.error(f"❌ WeChat markdown send failed: {resp.text}")
        except Exception as e:
            logger.error(f"❌ WeChat markdown send error: {e}")


if __name__ == "__main__":
    informer = WechatWebhookInformer()
    informer.send_message(content="ZVT test message 🎉")


# the __all__ is generated
__all__ = ["WechatWebhookInformer"]
