# -*- coding: UTF-8 -*-

import os
import io
import traceback
import codecs
import configparser


class CaseSensitiveConfigParser(configparser.RawConfigParser):
    def optionxform(self, optionstr):
        return optionstr


class ConfigHandle:
    """
    分析配置文件
    """
    def __init__(self, content, is_file=True):
        if is_file:
            self._init_config_by_path(content)
        else:
            self._init_config_by_content(content)

    def _init_config_by_path(self, path):
        """
        发布说明解析器
        :param path: 发布说明的完整路径
        :return: 解析好的发布说明
        """
        if not os.path.isfile(path):
            raise IOError('配置文件 {0} 不存在'.format(path))
        self._path = path
        self._msg = ""

        configContent = None
        try:
            # x = open(self._path)
            # 为应对 BOM File Header 问题，需要使用 codecs 的 open 去除 BOM 的影响。
            # BOM 常见于 Windows notepad 另存为 UTF8 时被偷偷加上
            x = codecs.open(self._path, 'rb', 'utf_8_sig')
            configContent = io.StringIO()
            for line in x.readlines():
                configContent.write(line.lstrip())
            x.close()
            configContent.flush()
            config = CaseSensitiveConfigParser(allow_no_value=True)
            self.config = config
            config.readfp(io.BytesIO(configContent.getvalue()))

        except configparser.MissingSectionHeaderError:
            self._msg = "不是认可的INI文件"
        except Exception as e:
            self._msg = "解析配置文件异常, exception: {0}; \n {1}".format(e, traceback.format_exc())
        finally:
            if configContent is not None:
                configContent.close()

    def _init_config_by_content(self, content):
        """
        发布说明解析器
        :param path: 发布说明的完整路径
        :return: 解析好的发布说明
        """
        # windows对于utf-8编码的文件自带BOM，但是其他系统utf-8编码默认不带BOM
        # 判断字符串的头, 如果包含BOM, 则去除
        if content.startswith(codecs.BOM_UTF8):
            self._content = content.replace(codecs.BOM_UTF8, "")
        else:
            self._content = content

        self._configContent = None
        self._msg = ''
        try:
            self._configContent = io.StringIO()
            self._configContent.write(self._content)
            self._configContent.flush()
            config = CaseSensitiveConfigParser(allow_no_value=True)
            self.config = config
            config.readfp(io.BytesIO(self._configContent.getvalue()))

        except configparser.MissingSectionHeaderError:
            self._msg = "不是认可的INI文件"
        except Exception as e:
            self._msg = "解析配置文件异常, exception: {0}; \n {1}".format(e, traceback.format_exc())
        finally:
            if self._configContent is not None:
                self._configContent.close()

    def get_config_key(self, section):
        """
        通过SECTION获取对应的内容
        :param section:
        :return: list
        """
        has_section = self.config.has_section(section)
        if not has_section:
            return ''

        try:
            val = self.config.options(section)
            return val
        except Exception as ex:
            self._msg = ex
            return ''

    def get_config_value(self, section, key):
        """
        获取ITEM的内容
        :param section:
        :return: str
        """
        has_section = self.config.has_section(section)
        if not has_section:
            return ''

        val = None
        for opStr in self.config.options(section):
            if opStr.upper() != key.upper():
                continue
            val = self.config.get(section, opStr)

        return val

    def get_config_sections(self):
        """
        get sections
        :return:
        """
        return self.config.sections()

    def get_config_sections_content(self, section):
        """
        get sections content
        :return:
        """
        if self.config.has_section(section):
            return self.config.options(section)
        else:
            return None

if __name__ == "__main__":

    content = '[DEP]\n  test11111111111111 = WWWWWWWWWWWW\n  test22222222222222 = EEEEEEEEEEEEEE\n  test3333333333333 = GGGGGGGGGGGG\n\n[DB1]\n    bettle4 = 7226\n    bettle4 = 7225 , 4567\n\n    bettle1 = 7225\n    bettle1 = 7223\n\n    bettle2 = 7227\n    bettle2 = 7228\n\n    # dlfjasldfjalsfja;l\n\n[DB2]\n    bettle = 7226\n    bettle = 7225\n\n[nasfile]\n\n    rel_20161107_home/template\n    rel_20161107_home/common\n\n[NOTIFY]\n    penglinwei259@lu.com\n    siruoyan647@lu.com\n\n[ADVICE]\n    WEBDEV-13396 【异常需求】PC/APP端强弹公告 for web（WCM版本方案）\n    WEBDEV-13238 【PC】页面底部通用栏优化\n\n'
    content1 = '/tmp/1.txt'
    config = ConfigHandle(content)
    # val = config.get_config_value('DEP')
    bettle_list = config.get_config_value('DB1', 'BETTLE')
    # val_list = config.get_config_item('DB1', 'BETTLE')
    print(content)


