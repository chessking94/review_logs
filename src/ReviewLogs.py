import csv
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json
import logging
import os
from pathlib import Path
import re
import smtplib
import sys
import traceback


LEVEL_MAPPING = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}
NOTIFICATION_LEVEL = logging.WARNING


def get_config(key):
    filename = os.path.join(Path(__file__).parents[1], 'config.json')
    with open(filename, 'r') as t:
        key_data = json.load(t)
    val = key_data.get(key)
    return val


def log_exception(exctype, value, tb):
    write_val = {
        'type': re.sub(r'<|>', '', str(exctype)),  # remove < and > since it messes up converting to HTML for potential email notifications
        'description': str(value),
        'traceback': str(traceback.format_tb(tb, 10))
    }
    logging.critical(str(write_val))


def list_to_html(data: list, has_header: bool = True) -> str:
    # https://stackoverflow.com/a/52785746
    html = '<table border="1">'
    for i, row in enumerate(data):
        if has_header and i == 0:
            tag = 'th'
        else:
            tag = 'td'
        tds = ''.join('<{}>{}</{}>'.format(tag, cell, tag) for cell in row)
        html += '<tr>{}</tr>'.format(tds)
    html += '</table>'
    return html


def main():
    script_name = Path(__file__).stem
    log_root = get_config('logRoot')

    dte = dt.datetime.now().strftime('%Y%m%d%H%M%S')
    log_name = f'{script_name}_{dte}.log'
    log_file = os.path.join(log_root, log_name)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s\t%(funcName)s\t%(levelname)s\t%(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )

    sys.excepthook = log_exception  # force unhandled exceptions to write to the log file

    log_list = [f for f in os.listdir(log_root) if os.path.isfile(os.path.join(log_root, f))]

    # remove current log file from directory list
    if log_name in log_list:
        log_list.remove(log_name)

    # remove files with a modify timestamp within the last X minutes
    exclude_interval_minutes = 5
    cutoff_time = dt.datetime.now() - dt.timedelta(minutes=exclude_interval_minutes)
    rmv_list = [
        f for f in os.listdir(log_root)
        if os.path.isfile(os.path.join(log_root, f))
        and dt.datetime.fromtimestamp(os.path.getmtime(os.path.join(log_root, f))) > cutoff_time
    ]
    log_list = [x for x in log_list if x not in rmv_list]

    """
    ASSUMPTION: Log entries have this specific format, delimited by tabs
    Datetime / Function / Level / Message
    """

    entry_list = []
    entry_hdr = ['Script Name', 'File Timestamp', 'Log Timestamp', 'Script Function', 'Logging Level', 'Logging Message']
    entry_list.append(entry_hdr)

    for lf in log_list:
        log_orig = os.path.join(log_root, lf)

        # delete files that are empty
        if os.path.getsize(log_orig) == 0:
            try:
                os.remove(log_orig)
            except PermissionError:
                logging.info(f"Unable to delete '{lf}', file is in use")
        else:
            # ASSUMPTION: First piece of the filename is the script that created the log, second is the timestamp
            script_name = lf.split('_')[0]
            log_timestamp = os.path.splitext(lf.split('_')[1])[0]
            log_dir = os.path.join(log_root, script_name)
            if not os.path.isdir(log_dir):
                os.mkdir(log_dir)
            log_new = os.path.join(log_dir, lf)

            # notify of specified level or greater entries
            with open(log_orig, mode='r', newline='\n') as logfile:
                reader = csv.reader(logfile, delimiter='\t', quotechar='"')
                for row in reader:
                    level_weight = LEVEL_MAPPING[row[2]]
                    if level_weight >= NOTIFICATION_LEVEL:
                        entry = [script_name, log_timestamp, row[0], row[1], row[2], row[3]]
                        entry_list.append(entry)

            # archive log file
            try:
                os.rename(log_orig, log_new)
            except PermissionError:
                # can't move the file, it's in use. remove those previously added entries and move on with life
                entry_list = [f for f in entry_list if f[0] != script_name or f[1] != log_timestamp]

    # send an email if the log list has more entries than just the header
    # TODO: Integrate with SendGrid
    if len(entry_list) > 1:
        send_email = get_config('sendEmail')
        smtp_server = get_config('smtpServer')
        smtp_port = get_config('smtpPort')
        smtp_sendas = get_config('smtpEmailSendAs')
        logging_recip = get_config('loggingEmailRecip')
        logging_recip = logging_recip if isinstance(logging_recip, list) else [logging_recip]  # convert to a list if not already one

        html = list_to_html(entry_list)

        if send_email:
            subject = f'Python Logging Summary - {dte[0:8]} {dte[8:10]}:{dte[10:12]}:{dte[12:14]}'
            body = html

            message = MIMEMultipart()
            message['From'] = smtp_sendas
            message['To'] = ';'.join(logging_recip)  # need to pass emails as a single string
            message['Subject'] = subject
            message.attach(MIMEText(body, 'html'))

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.sendmail(from_addr=smtp_sendas, to_addrs=logging_recip, msg=message.as_string())
        else:
            with open(os.path.join(Path(__file__).parents[1], 'test.html'), 'w') as f:
                f.write(html)

        # TODO: Insert error entries to a database table


if __name__ == '__main__':
    main()
