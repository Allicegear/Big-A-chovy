#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""金标准回归：断网回放真实录制的行情，结果必须与基线一致。

数据只在本机（``.golden_local/``，见 golden_harness.py）；没有数据或没有基线
（例如 CI）时跳过。基线由 ``golden_harness.py replay --out baseline`` 在重构
开始前生成；重构过程中的任何差异都意味着行为变化，必须先解释再继续。
"""

from __future__ import annotations

import contextlib
import io
import shutil
import unittest

import golden_harness as gh


@unittest.skipUnless(
    gh.CASSETTE_PATH.exists() and (gh.RUNS_DIR / "baseline").exists(),
    "本机无金标准数据（.golden_local/cassette.json 与 runs/baseline），跳过",
)
class GoldenMasterTest(unittest.TestCase):
    LABEL = "_unittest_candidate"

    def test_replay_matches_baseline(self):
        out = io.StringIO()
        try:
            with contextlib.redirect_stdout(out):
                gh.cmd_replay(self.LABEL, list(gh.SCENARIOS))
                rc = gh.cmd_compare("baseline", self.LABEL)
        except SystemExit as exc:  # 回放失败 / 未录制请求
            self.fail(f"{exc}\n{out.getvalue()}")
        self.assertEqual(rc, 0, out.getvalue())
        shutil.rmtree(gh.RUNS_DIR / self.LABEL, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
