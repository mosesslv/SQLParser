# -*- coding:utf-8 -*-
# CREATED BY: bohuai jiang
# CREATED ON: 2019/9/10
# LAST MODIFIED ON:

from service.predict_sql_review_oracle.Utility.Architecture import REC_FUNC, TransmitContainer
from service.sql_parser_graph.units import ParseUnitList
from service.predict_sql_review_oracle.Utility.Architecture import VerboseUnit
from typing import Optional, Tuple, List
import re
import service.sql_parser_graph.ParserUtiltiy as pu


class RecomSub(REC_FUNC):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __get_in_exist_SUB(self, sem_info: ParseUnitList) -> List[int]:
        out = []
        for sub in sem_info.by_type['SUB']:
            name = sub.name
            key = sub.keyword
            level = sub.level
            if key in ['IN', 'EXIST'] and 'SELECT ' in name.upper():
                out.append(level + 1)
        return out

    def __verbose(self, key: str, **kwargs) -> str:
        if key.upper() == 'IN_EXIST':
            return 'UNNEST '
        return ''

    def recom(self, info: Optional[TransmitContainer] = None) -> Tuple[TransmitContainer, VerboseUnit]:
        transmit = TransmitContainer()
        recom = VerboseUnit(self.param['semantic_info'].elements)
        sem_info = self.param['semantic_info']
        values = self.__get_in_exist_SUB(sem_info.elements)
        for v in values:
            recom.add_hint(hint=self.__verbose(key='IN_EXIST'), level=pu.find_add_hint_loc_by_lvl(v, sem_info.elements))

        return transmit, recom
