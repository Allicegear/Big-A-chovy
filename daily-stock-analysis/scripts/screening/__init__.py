"""筛选引擎的内部模块（从 a_share_daily_screen.py 逐步拆出）。

对外入口仍是 ``a_share_daily_screen``：它重新导出这里的公开名字，CLI、看板、
工作台与 tools 的既有调用方式不变。本包只按职责组织实现，不另立规则。
"""
