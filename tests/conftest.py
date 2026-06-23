"""pytest 配置:让 ``import tail`` 在测试中可用。"""

import os
import sys

# 把项目根目录加入 sys.path(项目根 = tests 的上一级)。
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
