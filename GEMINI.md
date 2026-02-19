# GEMINI.md - pyassistantbot2 Project Context

## Project Overview

`pyassistantbot2` is a comprehensive personal assistant and task management system centered around the `gstasks` framework. It integrates various personal data tracking needs including task management, habit tracking, financial management, and nutrition tracking.

### Core Technologies
- **Language:** Python 3.x
- **Database:** MongoDB (primary data store for tasks, habits, etc.)
- **CLI Framework:** [Click](https://click.palletsprojects.com/)
- **Web Framework:** [Flask](https://flask.palletsprojects.com/)
- **Infrastructure:** Docker and Docker Compose
- **Integrations:** Jira API, Trello API, Telegram Bot API, Google Spreadsheets
- **Data Analysis:** Pandas, NumPy
- **Parsing:** Custom date/time parsing using `ply` (Lex/Yacc)
- **Utilities:** `alex_leontiev_toolbox_python` (custom utility library)

### Architecture
- **CLI Tools:** 
    - `gstasks.py`: Main entry point for task management.
    - `ttask.py`: Bridge for importing tasks from local/temporary lists.
    - `habits.py`, `money.py`, `nutrition.py`, etc.: Specialized tracking scripts.
- **Web Interface:** `gstasks-flask.py` provides a Flask-based web dashboard.
- **Internal Logic:** 
    - `_gstasks/`: Core business logic for task management, Jira/Trello helpers, and parsers.
    - `_common/`: General-purpose utilities and shared functions.
- **Scheduling:** Uses `APScheduler` and `schedule` for periodic tasks and reminders.

## Building and Running

### Development Environment
- **Environment Variables:** Configuration is managed via `.env` files. Ensure `MONGO_URL` and other service-specific keys are set.
- **Python Environment:** Virtual environment (`venv`) is recommended. Dependencies are in `requirements.txt`.

### Key Commands
- **Building Docker:** 
  ```sh
  make                 # Generates Dockerfile from Dockerfile.jinja
  docker-compose build # Builds the containers
  ```
- **Running the Web Server:**
  ```sh
  flask --app gstasks-flask run --debug
  ```
- **Running the CLI:**
  ```sh
  ./gstasks.py ls      # List tasks
  ./gstasks.py add -n "Task Name" # Add a task
  ```
- **Testing:**
  ```sh
  pytest               # Run all tests
  ```

## Project Structure & Key Files

- `gstasks.py`: The primary CLI tool for task operations.
- `ttask.py`: CLI for managing and moving tasks from temporary lists.
- `_gstasks/`: 
    - `task_list.py`: MongoDB interaction logic.
    - `jira_helper.py`: Integration with Jira.
    - `parsers/dates_parser/`: Custom natural language date parser logic.
- `_common/`: Low-level utility functions (date parsing, network requests).
- `gstasks-flask.py`: Flask application entry point.
- `habits.py` / `heartbeat_habits.py`: Habit tracking and monitoring.
- `money.py` / `money2.py`: Financial tracking logic.
- `Dockerfile.jinja` / `produce-docker.py`: Template-based Docker image generation.

## Development Conventions

- **Formatting:** Adheres to `black` for code formatting and `isort` for import sorting.
- **Testing:** Use `pytest` for functional and unit tests. New features should include corresponding test cases in the `tests/` directory or as `test__*.py` files in the root.
- **Task Management:** The system uses its own terminology for task states: `marks`, `tags`, `labels`, `flabels`, and `relations`. 
- **Time Management:** Heavy reliance on custom date parsing (`parse_cmdline_datetime`) which supports natural language queries like "next workday".
- **Documentation:** Maintain `README.md` for high-level user instructions and `GEMINI.md` for development context.
