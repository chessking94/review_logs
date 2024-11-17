import logging
import os
from pathlib import Path

import pandas as pd
from Utilities_Python import misc, db, notifications

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def count_errors(engine) -> int:
    query = """
SELECT
COUNT(rvw.eventID) AS Ct

FROM Workflow.temp.ErrorReviewPending rvw
JOIN Workflow.dbo.Events e ON
    rvw.eventID = e.eventID
JOIN Workflow.dbo.EventStatuses es ON
    e.eventStatusID = es.eventStatusID

WHERE es.eventStatus = 'Error'
"""
    logging.debug(query)
    df = pd.read_sql(query, engine)
    rtn = 0
    if len(df) == 0:
        logging.critical('Error count query returned no records!')
    else:
        rtn = df.values[0][0]

    return rtn


def get_lasterror(engine) -> str:
    query = """
SELECT
eventNote

FROM Workflow.dbo.Events

WHERE eventID = (
    SELECT
    MAX(e.eventID) AS eventID

    FROM Workflow.temp.ErrorReviewPending rvw
    JOIN Workflow.dbo.Events e ON
        rvw.eventID = e.eventID
    JOIN Workflow.dbo.EventStatuses es ON
        e.eventStatusID = es.eventStatusID

    WHERE es.eventStatus = 'Error'
)
"""
    logging.debug(query)
    df = pd.read_sql(query, engine)
    rtn = ''
    if len(df) == 0:
        logging.critical('LastError query returned no records!')
    else:
        rtn = df.values[0][0]

    return rtn


def main():
    script_name = Path(__file__).stem
    _ = misc.initiate_logging(script_name, CONFIG_FILE)

    with db.db(os.getenv('ConnectionStringOdbcRelease')) as c:
        csr = c.conn.cursor()

        # queue the records
        csr.execute('EXEC Workflow.temp.insertErrorReview')
        c.conn.commit()

        # process the records to notify of any errors
        rec_ct = count_errors(c.engine)

        if rec_ct > 0:
            err_msg = get_lasterror(c.engine)

            tg_msg = f'Error Notification: A total of {rec_ct} potential problems have been identified in the Workflow event history'
            tg_msg = tg_msg + f'. {err_msg}'
            notifications.SendTelegramMessage(tg_msg)

        # finalize the records
        csr.execute('EXEC Workflow.temp.updateErrorReview')
        c.conn.commit()


if __name__ == '__main__':
    main()
