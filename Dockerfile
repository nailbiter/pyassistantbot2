FROM python:3.9-slim-buster
LABEL maintainer="nailbiter"

RUN apt-get update && apt-get install -y git

COPY requirements.txt .
RUN pip3 install -r requirements.txt

ENV TZ=Asia/Tokyo
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV _TMP_DIR=/tmp/bd3283de_9551_41bc_a8ee_fab4d794ffd8
ENV _TRELLO_TAG=trello-v3
RUN mkdir $_TMP_DIR
RUN git clone --depth 1 https://github.com/nailbiter/for.git $_TMP_DIR --branch $_TRELLO_TAG --single-branch
ENV TRELLO_PACKAGE_PATH=$_TMP_DIR/forpython/trello

#todo
ENV TODO_TRELLO_LIST_ID="5a83f3449c950b04c540ba66"
ENV TODO_BOARD_ID="5a83f33d7c047209445249dd"
ENV TASK_LIST_ID="5a83f3449c950b04c540ba66"

RUN mkdir _common
COPY _common/__init__.py _common
COPY _common/simple_math_eval.py _common

COPY _actor.py .

COPY actor.py .
COPY tr-task.py task.py
COPY heartbeat_habits.py .
COPY heartbeat_time.py .

COPY .env .env

CMD ["python3","actor.py"]
