"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/jira_helper.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2024-04-28T12:09:24.452662
    REVISION: ---

==============================================================================="""
import typing
import re
import logging
import subprocess
import os
from jinja2 import Template
import json
import io
from dotenv import dotenv_values


def _run_cmd(cmd: str, is_ensure_ok: bool = True) -> str:
    logging.warning(f"> {cmd}")
    ec, out = subprocess.getstatusoutput(cmd)
    if is_ensure_ok:
        assert ec == 0, (cmd, ec, out)
    return out


class JiraGetter:
    def __init__(self, cmd_tpl: str, is_stdin: bool = True, sep: str = "\n"):
        self._cmd_tpl = Template(cmd_tpl)
        self._sep = sep
        self._is_stdin = is_stdin

    def run(self, env: dict, l: list[str]) -> list[str]:
        cmd = self._cmd_tpl.render(dict(l=l))
        logging.warning(f"> {cmd}")
        p1 = subprocess.Popen(
            cmd,
            env={**os.environ, **env},
            encoding="utf-8",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **(dict(stdin=subprocess.PIPE) if self._is_stdin else {}),
        )
        logging.warning(f"l: {l}")
        out, err = p1.communicate(
            **(dict(input="\n".join(l)) if self._is_stdin else {})
        )
        out = out.strip()
        logging.warning(f"out: {out}")
        logging.warning(f"err: {err}")
        return out.split(self._sep)


class JiraHelper:
    def __init__(
        self,
        jira_exec: str = "",
        dotenv: typing.Optional[str] = None,
        getters: dict = {},
        **kwargs,
    ):
        self._jira_exec = jira_exec
        self._dotenv = {} if dotenv is None else dotenv_values(dotenv)
        self._getters = {k: JiraGetter(**v) for k, v in getters.items()}

    @property
    def jira_exec(self) -> str:
        return self._jira_exec

    def get(self, property_name: str, ids: list[str]) -> list[str]:
        return self._getters[property_name].run(self._dotenv, l=ids)

    def __str__(self) -> str:
        return str(dict(jira_exec=self.jira_exec))


def generate_symbols_between(start: str, end: str) -> list[str]:
    ms = re.match(r"(.*)([0-9]+)$", start)
    me = re.match(r"(.*)([0-9]+)$", end)
    assert ms.group(1) == me.group(1), (ms.group(1), me.group(1))
    return [f"{ms.group(1)}{i}" for i in range(int(ms.group(2)), int(me.group(2)) + 1)]
