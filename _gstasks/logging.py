"""===============================================================================

        FILE: /Users/nailbiter/Documents/forgithub/pyassistantbot2/_gstasks/logging.py

       USAGE: (not intended to be directly executed)

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: FIXME: replace with `alex_leontiev_toolbox_python`
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2025-09-12T21:04:40.412537
    REVISION: ---

==============================================================================="""
import typing
import logging
import functools
import sys


@functools.singledispatch
def make_log_format(x) -> str:
    raise NotImplementedError(x)


@make_log_format.register
def _(l: list) -> str:
    return " - ".join([f"%({x})s" for x in l])


def get_configured_logger(
    name: str,
    level: str = "DEBUG",
    log_format=make_log_format(
        [
            "name",
            "levelname",
            "asctime",
            "message",
        ]
    ),
    is_pre_clean: bool = True,
    is_propagate: bool = False,
) -> logging.Logger:
    app_logger = logging.getLogger(name)

    if not is_propagate:
        app_logger.propagate = False

    # --- Step 2: Set the logging level for YOUR logger ---
    # This logger will now process any message of DEBUG severity or higher.
    app_logger.setLevel(getattr(logging, level))

    if is_pre_clean:
        # while len(app_logger.handlers) > 0:
        #     h = app_logger.handlers[0]
        #     # dbg.debug('removing handler %s'%str(h))
        #     app_logger.removeHandler(h)
        #     # dbg.debug('%d more to go'%len(testLogger.handlers))
        app_logger.handlers.clear()

    # --- Step 3: Create a StreamHandler to output to stderr for YOUR logger ---
    # This handler will specifically handle messages from 'app_logger'.
    app_console_handler = logging.StreamHandler(
        sys.stderr
    )  # or just logging.StreamHandler()
    # You can also set a level on the handler if you want it to be more restrictive
    # than the logger itself, but typically you want it to respect the logger's level.
    # app_console_handler.setLevel(logging.DEBUG)

    # --- Step 4: Create a Formatter for better message layout (Optional but recommended) ---
    formatter = logging.Formatter(log_format)
    app_console_handler.setFormatter(formatter)

    # --- Step 5: Add the configured handler to YOUR logger ---
    app_logger.addHandler(app_console_handler)
    return app_logger
