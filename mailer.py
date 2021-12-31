#!/usr/bin/env python3
"""===============================================================================

        FILE: forhabits/kostil/mailer.py

       USAGE: ./forhabits/kostil/mailer.py

 DESCRIPTION: 

     OPTIONS: ---
REQUIREMENTS: ---
        BUGS: ---
       NOTES: ---
      AUTHOR: Alex Leontiev (alozz1991@gmail.com)
ORGANIZATION: 
     VERSION: ---
     CREATED: 2021-09-03T13:00:15.753887
    REVISION: ---

==============================================================================="""

from __future__ import print_function
import click
import smtplib
from os.path import basename
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate
import os
import click
import tqdm
import time
import itertools
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import pandas as pd
import tqdm
from datetime import datetime
from _common import parse_cmdline_date
import inspect
import types
from typing import cast
import logging
import json
import requests

# def _send_mail(send_from, send_to, subject, text, files=None,
#              server=None):
#    """
#    adapted from https://stackoverflow.com/a/3363254
#    """
#    assert isinstance(send_to, list)
#
#    msg = MIMEMultipart()
#    msg['From'] = send_from
#    msg['To'] = COMMASPACE.join(send_to)
#    msg['Date'] = formatdate(localtime=True)
#    msg['Subject'] = subject
#
#    msg.attach(MIMEText(text))
#
#    for f in files or []:
#        with open(f, "rb") as fil:
#            part = MIMEApplication(
#                fil.read(),
#                Name=basename(f)
#            )
#        # After the file is closed
#        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(f)
#        msg.attach(part)
#
#
##    smtp = smtplib.SMTP(server)
#    if server is None:
#        server = smtplib.SMTP("127.0.0.1")
#    smtp = server
#    smtp.sendmail(send_from, send_to, msg.as_string())
#
# @click.command()
# def mailer():
#    gmail_user = os.environ["GOOGLE_ACCOUNT"]
#    gmail_password = os.environ["GOOGLE_PASS"]
#    server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
#    server.ehlo()
#    server.login(gmail_user, gmail_password)
#
# _send_mail(gmail_user, list({recipient, gmail_user}), "test subject", "test test", files=[
# f"screenshots/page_{k}_{i}.png" for k,i in itertools.product(["bottom"],range(4))], server=server)
# server.close()
##    click.echo(f"sent email to {recipient}")


_SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']


class _Media:
    def __init__(self, medias={"cli", }, slack_webhook=None):
        assert medias <= {"cli", "slack"}
        if "slack" in medias:
            assert slack_webhook is not None
        self._medias = medias
        self._slack_webhook = slack_webhook
        self._loggers = []

    def push_logger(self, logger):
        self._loggers.append(logger)

    def pop_logger(self):
        self._loggers.pop()

    def __call__(self, msg, level="STDOUT", can_omit=False):
        for media in self._medias:
            if media == "cli":
                if level == "STDOUT":
                    method = click.echo
                else:
                    method = getattr(self._loggers[-1], level.lower())
                method(msg)
            elif media == "slack":
                slack_webhook = self._slack_webhook
                if not can_omit:
                    _ = requests.post(slack_webhook, json.dumps({"text": (
                        "" if level == "STDOUT" else f"`{level}` ")+f"```{msg}```"}), headers={"Content-type": "application/json"})

    def flush(self):
        pass


@click.command()
@click.option("--token-filename", default=".token.gmail.json", type=click.Path())
@click.option("--creds-filename", default="../../credentials_google_spreadsheet.json", type=click.Path())
@click.option("-n", "--fetch-last-n-messages", type=int)
@click.option("-a", "--after-date")
@click.option("-x", "--text-in-subject")
@click.option("-m", "--media", type=click.Choice(["slack", "cli"]), multiple=True)
@click.option("--slack-webhook", envvar="SLACK_WEBHOOK")
def mailer(token_filename, creds_filename, fetch_last_n_messages, after_date, text_in_subject, media, slack_webhook):
    if not media:
        media = ["cli"]
    media = set(media)
    media = _Media(medias=media, slack_webhook=slack_webhook)

    # taken from https://stackoverflow.com/a/13514318
    this_function_name = cast(
        types.FrameType, inspect.currentframe()).f_code.co_name
    logger = logging.getLogger(__name__).getChild(this_function_name)
    media.push_logger(logger)
    after_date = parse_cmdline_date(after_date)

    # If modifying these scopes, delete the file token.json.

    """Shows basic usage of the Gmail API.
    Lists the user's Gmail labels.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists(token_filename):
        creds = Credentials.from_authorized_user_file(token_filename, _SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                creds_filename, _SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token_filename, 'w') as token:
            token.write(creds.to_json())

    service = build('gmail', 'v1', credentials=creds)
    messages_df = fetch_last_emails(
        service, n=fetch_last_n_messages, progress_bar_type="tqdm")
    if after_date is not None:
        min_date = messages_df.internal_date.min()
        if min_date >= after_date:
            media(
                f"{min_date}>={after_date} ==> maybe, you selected too small `n`(={fetch_last_n_messages})", level="WARNING")
        messages_df = messages_df[[
            internal_date >= after_date for internal_date in messages_df.internal_date]]
    if text_in_subject is not None:
        messages_df = messages_df[[
            text_in_subject in subject for subject in messages_df.subject]]
    media(messages_df, can_omit=len(messages_df) == 0)

    media.pop_logger()

#    logger.warning(f"after_date: {after_date}")

    # Call the Gmail API
#    results = service.users().labels().list(userId='me').execute()
#    labels = results.get('labels', [])
#
#    if not labels:
#        print('No labels found.')
#    else:
#        print('Labels:')
#        for label in labels:
#            print(label['name'])


def _get_tqdm(progress_bar_type):
    if progress_bar_type is None:
        return lambda x: x
    elif progress_bar_type == "tqdm":
        return tqdm.tqdm
    else:
        raise NotImplementedError(progress_bar_type)


def fetch_last_emails(service, n=None, progress_bar_type=None):
    _tqdm = _get_tqdm(progress_bar_type)
    # FIXME: generalize to >100
    if n is not None:
        assert n <= 100
    response = service.users().threads().list(userId='me', q='').execute()
    threads = response["threads"]
    user_id = "me"
    res = []
#    print(len(threads))
    if n is not None:
        threads = threads[:n]
    for thread in _tqdm(threads):
        tdata = service.users().threads().get(
            userId=user_id, id=thread['id']).execute()
        nmsgs = len(tdata['messages'])

        if nmsgs > 0:    # skip if <3 msgs in thread
            r = {}
            msg = tdata['messages'][0]
            internalDate = msg["internalDate"]
            internalDate = datetime.fromtimestamp(int(internalDate)/1000)
#            click.echo(internalDate)
            payload = msg['payload']
            subject = ''
            for header in payload['headers']:
                if header['name'] == 'Subject':
                    subject = header['value']
                    break
            if subject:  # skip if no Subject line
                #                click.echo('- %s (%d msgs)' % (subject, nmsgs))
                r["subject"] = subject
            r["internal_date"] = internalDate
            res.append(r)

    return pd.DataFrame(res)


if __name__ == "__main__":
    mailer()
