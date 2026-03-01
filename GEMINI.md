# Cloud Run Function Summary: pyas2-habits

The Cloud Run function (deployed as `pyas2-habits`) serves as a backend for a personal assistant Telegram bot. It integrates various tracking and productivity tools into a unified interface.

### Architecture Overview
- **Entrypoint:** A **FastAPI** application (`actor.py`) that handles incoming Telegram webhooks.
- **Data Storage:** Uses **MongoDB Atlas** for persistent storage of habits, tasks, money logs, and system state.
- **Core Integration:** Combines local MongoDB-based task management with **Trello** for high-level task tracking.

### Key Components & Logic
1.  **Telegram Bot Interface (`actor.py`, `_actor.py`)**:
    - Processes commands from a authorized `CHAT_ID`.
    - Implements a routing system (`ProcessCommand`) that maps Telegram commands (e.g., `/habits`, `/ttask`, `/money`) to Python functions or sub-processes.
2.  **Habit Tracking (`heartbeat_habits.py`)**:
    - Manages recurring habits defined with **cron expressions**.
    - Calculates due dates and "punches" (execution instances) in MongoDB.
    - Automatically identifies "failed" habits if they aren't marked as done within a specific delay.
3.  **Task Management (`gstasks.py` & `tr-task.py`)**:
    - **`gstasks.py`**: A complex task manager supporting tags, relations, scheduling, and "engagement" (tracking the current active task).
    - **`tr-task.py`**: Specifically handles synchronization with a **Trello** "Todo" board, allowing the creation and archival of cards directly via Telegram.
4.  **Logging & Utilities (`_common/`)**:
    - Provides robust date/time parsing (handling relative dates like "tomorrow" or "+2d").
    - Manages MongoDB connections and Trello API authentication.

### Recent Fixes (March 2026)
- **Background Task Execution (`actor.py`)**: Refactored the webhook handler to use FastAPI `BackgroundTasks`. The function now returns a success response immediately to the caller, while the Telegram command (like `/habits`) executes in the background. This prevents timeouts when the bot is called by other Cloud Run functions.
- **Performance Optimization (`heartbeat_habits.py`)**: Disabled an expensive sanity check in `_get_habits_punch_coll` that was reading the entire `habitspunch2` collection into memory. This reduces the execution time of habit-related commands from minutes to seconds.

**Note:** Reliable background execution in Cloud Run requires the "CPU always allocated" setting to be enabled for the service.
