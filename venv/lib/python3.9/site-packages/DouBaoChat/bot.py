"""
对字节豆包API的简单封装，支持多用户和上下文
"""
import os
import asyncio
import traceback
from typing import AsyncGenerator, Optional, Dict, List, Union
from volcenginesdkarkruntime import Ark
from volcenginesdkarkruntime._streaming import Stream
from volcenginesdkarkruntime.types.chat import ChatCompletion, ChatCompletionChunk

from DouBaoChat.typings import *

ENDPOINTS = [endpoint.name for endpoint in ENDPOINT]


class ChatBot:
    """
    字节豆包API的聊天机器人
    """

    def __init__(
            self,
            api_key: Optional[str] = None,
            endpoint: Optional[str] = None,
            bot_id: Optional[str] = None,
            timeout: Optional[float] = None,
            temperature: Optional[float] = None,
            top_p: Optional[float] = None,
            frequency_penalty: Optional[float] = None,
            system_prompt: Optional[str] = None,
    ) -> None:
        self._bot_id = bot_id
        self._endpoint = None
        if self._bot_id is None:
            endpoint = endpoint or ENDPOINT.DOUBAO_PRO_32K.name
            if endpoint.upper() not in ENDPOINTS:
                raise ValueError(f"非法模型名: {endpoint}，请调用ChatBot.show_available_models()查看可用模型名")
            self._endpoint: str = getattr(ENDPOINT, endpoint.upper()).value

        self._api_key = api_key or os.environ["ARK_API_KEY"]
        self._timeout = timeout
        self._temperature = temperature
        self._top_p = top_p
        self._frequency_penalty = frequency_penalty
        self._system_prompt = system_prompt or "你是豆包，是由字节跳动开发的 AI 人工智能助手"

        self._client = Ark(
            api_key=self._api_key,
            timeout=self._timeout,
        )

        chat = self._client.bot_chat if self._bot_id else self._client.chat
        self._chat_function = chat.completions.create

        self._conversation: Dict[str, List[Dict]] = {
            "default": [
                {"role": ROLE.SYSTEM.value, "content": self._system_prompt}
            ]
        }

    def _add_to_conversation(
            self,
            role: ROLE,
            message: str,
            convo_id: str = "default"
    ) -> None:
        """
        添加对话到对话列表
        """
        if convo_id not in self._conversation:
            raise ValueError(f"对话ID {convo_id} 不存在")
        self._conversation[convo_id].append({
            "role": role.value,
            "content": message
        })

    def _rollback_conversation(self, convo_id: str = "default") -> None:
        """
        回滚对话
        """
        if convo_id not in self._conversation:
            raise ValueError(f"对话ID {convo_id} 不存在")
        self._conversation[convo_id].pop()

    def _send_message(
            self,
            convo_id: str = "default",
            **kwargs,
    ) -> Union[ChatCompletion, Stream[ChatCompletionChunk]]:
        """
        发送消息
        """
        if convo_id not in self._conversation:
            raise ValueError(f"对话ID {convo_id} 不存在")
        completion = self._chat_function(
            model=self._bot_id if self._bot_id else self._endpoint,
            messages=self._conversation[convo_id],
            stream=True,
            temperature=kwargs.get("temperature", self._temperature),
            top_p=kwargs.get("top_p", self._top_p),
            frequency_penalty=kwargs.get("frequency_penalty", self._frequency_penalty),
            # user=convo_id
        )
        return completion

    async def _ask_stream(
            self,
            message: str,
            convo_id: str = "default",
            **kwargs
    ) -> AsyncGenerator[str, None]:
        """
        流式对话
        """
        if convo_id not in self._conversation:
            self.reset(convo_id=convo_id)
        self._add_to_conversation(role=ROLE.USER, message=message, convo_id=convo_id)
        completion = self._send_message(convo_id, **kwargs)
        try:
            full_response = ""
            for chunk in completion:
                if not chunk.choices:
                    continue
                partial_response = chunk.choices[0].delta.content
                full_response += partial_response
                yield partial_response
            self._add_to_conversation(message=full_response, role=ROLE.ASSISTANT, convo_id=convo_id)
        except Exception:
            traceback.print_exc()
            self._rollback_conversation(convo_id)
            yield "抱歉，当前对话模型存在问题，请稍后再试"

    async def ask(
            self,
            message: str,
            convo_id: str = "default",
            **kwargs
    ) -> str:
        """
        请求对话
        """
        response = self._ask_stream(
            message=message,
            convo_id=convo_id,
            **kwargs
        )
        full_response = "".join([chunk async for chunk in response])
        return full_response

    def reset(self, convo_id: str = "default") -> None:
        """
        重设对话
        """
        self._conversation[convo_id] = [
            {"role": ROLE.SYSTEM.value, "content": self._system_prompt}
        ]

    @staticmethod
    def show_available_models():
        """
        显示可用的模型名
        """
        from pprint import pprint
        available_models = [endpoint.lower() for endpoint in ENDPOINTS]
        pprint(available_models)


