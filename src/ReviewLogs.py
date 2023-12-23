import csv
import datetime as dt
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import os
from pathlib import Path
import smtplib
import sys

from automation import misc
import pandas as pd
import pyodbc as sql
import requests


LEVEL_MAPPING = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}
NOTIFICATION_LEVEL = logging.WARNING
CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


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


def insert_logsentries(data: list):
    """
    Assumption is the variable passed is a list of lists
    This list of lists is formatted specifically like the entry_hdr variable from 'main' below
    """

    conn_str = misc.get_config('connectionString_domainDB', CONFIG_FILE)
    DBCONN = sql.connect(conn_str)

    # iterate through remaining list entries, pre-process the values as needed, and perform the inserts
    for entry in data:
        scr_nm, file_dte, scr_typ, lg_dte, lg_tme, fn, lvl_id, lg_msg = preprocess_logentry(DBCONN, entry)
        csr = DBCONN.cursor()
        insert_qry = "INSERT INTO logs.Entries (ScriptName, FileDate, ScriptType, LogDate, LogTime, [Function], LevelID, [Message]) "
        insert_qry = insert_qry + f"VALUES ('{scr_nm}', '{file_dte}', '{scr_typ}', '{lg_dte}', '{lg_tme}', '{fn}', '{lvl_id}', '{lg_msg}')"
        logging.debug(insert_qry)
        csr.execute(insert_qry)
        DBCONN.commit()

    DBCONN.close()


def preprocess_logentry(conn, entry):
    scr_nm = entry[0]

    # reformat yyyymmddHHMMSS to yyyy-mm-dd HH:MM:SS
    file_dte = entry[1]
    file_dte = dt.datetime.strptime(file_dte, '%Y%m%d%H%M%S')
    file_dte = file_dte.strftime('%Y-%m-%d %H:%M:%S')

    scr_typ = 'Python'  # TODO: Come up with a way to open this to non-Python logs someday

    # reformat yyyy-mm-dd HH:MM:SS,nnn to yyyy-mm-dd and HH:MM:SS.nnn
    lg_dte = entry[2]
    lg_dte, lg_tme = lg_dte.split()
    lg_tme = lg_tme.replace(',', '.')

    fn = entry[3]

    # convert level name to level ID
    lvl_id = get_levelid(conn, entry[4])

    lg_msg = entry[5].replace("'", "''")

    processed_entry = [scr_nm, file_dte, scr_typ, lg_dte, lg_tme, fn, lvl_id, lg_msg]
    return processed_entry


def get_levelid(conn, level):
    id_qry = f"SELECT LevelID FROM logs.Levels WHERE Level = '{level}'"
    logging.debug(id_qry)
    df = pd.read_sql(id_qry, conn)
    rtn = None
    if len(df) == 0:
        logging.critical(f"no record for level '{level}'")
    else:
        rtn = df.values[0][0]
    return rtn


def validate_notiftype(notiftype):
    NOTIFTYPE_CHOICES = ['TELEGRAM', 'EMAIL', 'TEST']
    notiftype = notiftype.upper()
    if notiftype not in NOTIFTYPE_CHOICES:
        if notiftype != '':
            logging.warning(f'Invalid notiftype provided, ignoring|{notiftype}')
        notiftype = None
    return notiftype


def main():
    script_name = Path(__file__).stem
    log_root = misc.get_config('logRoot', CONFIG_FILE)

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

    sys.excepthook = misc.log_exception  # force unhandled exceptions to write to the log file

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
    notification_list = []
    notification_hdr = ['Script Name', 'File Timestamp', 'Log Timestamp', 'Script Function', 'Logging Level', 'Logging Message']
    notification_list.append(notification_hdr)

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
                    entry = [script_name, log_timestamp, row[0], row[1], row[2], row[3]]
                    entry_list.append(entry)
                    level_weight = LEVEL_MAPPING[row[2]]
                    if level_weight >= NOTIFICATION_LEVEL:
                        notification_list.append(entry)

            # archive log file
            try:
                os.rename(log_orig, log_new)
            except PermissionError:
                # can't move the file, it's in use. remove those previously added entries and move on with life
                entry_list = [f for f in entry_list if f[0] != script_name or f[1] != log_timestamp]
                notification_list = [f for f in notification_list if f[0] != script_name or f[1] != log_timestamp]

    # write to db
    if len(entry_list) > 0:
        print(entry_list)
        insert_logsentries(entry_list)

    # send notification
    if len(notification_list) > 1:
        # insert log entries to a database table, start by row-by-row since in theory there won't be a ton
        rec_ct = len(notification_list) - 1

        # figure out the notifications
        notif_type = validate_notiftype(misc.get_config('notificationType', CONFIG_FILE))
        html = list_to_html(notification_list)

        if notif_type == 'TELEGRAM':
            tg_api_key = misc.get_config('telegramAPIKey', CONFIG_FILE)
            tg_id = misc.get_config('telegramID', CONFIG_FILE)
            tg_msg = f'A total of {rec_ct} potential problems have been identified in the HuntHome logs'
            url = f'https://api.telegram.org/bot{tg_api_key}'
            params = {'chat_id': tg_id, 'text': tg_msg}
            with requests.post(url + '/sendMessage', params=params) as resp:
                cde = resp.status_code
                if cde != 200:
                    logging.error(f'Log Review Telegram Notification Failed: Response Code {cde}')
        elif notif_type == 'EMAIL':
            smtp_server = misc.get_config('smtpServer', CONFIG_FILE)
            smtp_port = misc.get_config('smtpPort', CONFIG_FILE)
            smtp_sendas = misc.get_config('smtpEmailSendAs', CONFIG_FILE)
            logging_recip = misc.get_config('loggingEmailRecip', CONFIG_FILE)
            logging_recip = logging_recip if isinstance(logging_recip, list) else [logging_recip]  # convert to a list if not already one

            subject = f'Python Logging Summary - {dte[0:8]} {dte[8:10]}:{dte[10:12]}:{dte[12:14]}'
            body = html

            message = MIMEMultipart()
            message['From'] = smtp_sendas
            message['To'] = ';'.join(logging_recip)  # need to pass emails as a single string
            message['Subject'] = subject
            message.attach(MIMEText(body, 'html'))

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.sendmail(from_addr=smtp_sendas, to_addrs=logging_recip, msg=message.as_string())
        elif notif_type == 'TEST':
            with open(os.path.join(Path(__file__).parents[1], 'test.html'), 'w') as f:
                f.write(html)
        else:
            pass  # do nothing


if __name__ == '__main__':
    main()
