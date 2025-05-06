"""
包含项目所需自定义类型的模块
"""
from enum import Enum, unique

__all__ = ["ENDPOINT", "ROLE"]

@unique
class ENDPOINT(Enum):
    """
    请求端点枚举类
    """
    DOUBAO_LITE_4K = "ep-20240718052500-6z78r"
    DOUBAO_LITE_32K = "ep-20240718052722-4r29f"
    DOUBAO_LITE_128K = "ep-20240718052840-lmg8q"
    DOUBAO_PRO_4K = "ep-20240718052124-q8wxf"
    DOUBAO_PRO_32K = "ep-20240717173623-tkgjh"
    DOUBAO_PRO_128K = "ep-20240717170406-gmfv8"


class ROLE(Enum):
    """
    对话角色枚举类
    """
    USER = "user"
    SYSTEM = "system"
    ASSISTANT = "assistant"



