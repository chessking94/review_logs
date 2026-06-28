import os
from pathlib import Path

import pyodbc
from Utilities_Python import misc, notifications

CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def get_errors(conn: pyodbc.Connection) -> list[pyodbc.Row]:
    sql_cmd = """
SELECT
    h.instance_id
    --,j.job_id
    ,j.name AS job_name
    --,h.step_name AS step_name
    --,msdb.dbo.agent_datetime(run_date, run_time) as run_datetime
    --,CASE h.run_status
    --    WHEN 0 THEN 'Failed'
    --    WHEN 1 THEN 'Succeeded'
    --    WHEN 2 THEN 'Retry'
    --    WHEN 3 THEN 'Canceled'
    --    WHEN 4 THEN 'In Progress'
    --END AS status_description
    --,h.message AS error_message

FROM msdb.dbo.sysjobhistory AS h
INNER JOIN msdb.dbo.sysjobs AS j ON h.job_id = j.job_id

WHERE h.run_status = 0  --only failures
AND h.step_id <> 0
AND j.enabled = 1

ORDER BY h.instance_id
"""
    with conn.cursor() as csr:
        csr.execute(sql_cmd)
        return csr.fetchall()


def get_lastid(conn: pyodbc.Connection) -> int:
    sql_cmd = """
SELECT
    Last_ID

FROM HuntHome.dbo.LastProcessed

WHERE Database_Name = 'msdb'
AND Schema_Name = 'dbo'
AND Table_Name = 'sysjobhistory'
"""
    with conn.cursor() as csr:
        csr.execute(sql_cmd)
        row = csr.fetchone()
        if row:
            id = row[0]
        else:
            id = 0
            sql_insert = 'INSERT INTO HuntHome.dbo.LastProcessed (Database_Name, Schema_Name, Table_Name, Last_ID) '
            sql_insert += "VALUES ('msdb', 'dbo', 'sysjobhistory', 0)"
            csr.execute(sql_insert)
            csr.commit()

    return id


def update_last_id(conn: pyodbc.Connection, max_id: int):
    with conn.cursor() as csr:
        sql_upd = 'UPDATE HuntHome.dbo.LastProcessed '
        sql_upd += f'SET Last_ID = {max_id} '
        sql_upd += "WHERE Database_Name = 'msdb' AND Schema_Name = 'dbo' AND Table_Name = 'sysjobhistory'"
        csr.execute(sql_upd)
        csr.commit()


def main():
    script_name = Path(__file__).stem
    _ = misc.initiate_logging(script_name, CONFIG_FILE)

    conn_str = os.getenv('ConnectionStringOdbcRelease')
    with pyodbc.connect(conn_str) as conn:
        last_id = get_lastid(conn)
        max_id = last_id

        err_ct = 0
        job_names = set()
        rows = get_errors(conn)
        for row in rows:
            if row.instance_id <= last_id:
                continue

            err_ct += 1
            max_id = row.instance_id
            job_names.add(row.job_name)

        if err_ct > 0:
            tg_msg = f'Error Notification: A total of {err_ct} failed SQL Agent Jobs have been identified! '
            tg_msg += f'The jobs are: {", ".join(str(job) for job in job_names)}'
            notifications.SendTelegramMessage(tg_msg)

        if max_id > last_id:
            update_last_id(conn, max_id)


if __name__ == '__main__':
    main()
