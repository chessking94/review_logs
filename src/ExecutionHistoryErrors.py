import os
from pathlib import Path

import pyodbc
from Utilities_Python import misc, notifications


CONFIG_FILE = os.path.join(Path(__file__).parents[1], 'config.json')


def prune_old_reviews(conn: pyodbc.Connection) -> None:
    """ Delete review markers older than two days, retaining recent deduplication. """
    sql_cmd = 'DELETE FROM Nexus.dbo.ExecutionHistoryErrorReview WHERE ReviewedAt < DATEADD(DAY, -2, CONVERT(date, SYSDATETIME()))'
    with conn.cursor() as csr:
        csr.execute(sql_cmd)
    conn.commit()


def get_unreviewed_failures(conn: pyodbc.Connection) -> list[pyodbc.Row]:
    """ Fetch today's failed executions not already reviewed. """
    sql_cmd = """
SELECT
    e.ExecutionHistoryID
    ,ISNULL(t.Name, r.Name) AS ExecutionName
    ,w.Name AS WorkflowName
    ,s.Name AS Status

FROM Nexus.dbo.ExecutionHistory AS e
INNER JOIN Nexus.dbo.ExecutionStatus AS s ON e.ExecutionStatusID = s.ExecutionStatusID
LEFT JOIN Nexus.dbo.TaskDefinition AS t ON e.TaskDefinitionID = t.TaskDefinitionID
LEFT JOIN Nexus.dbo.ReportDefinition AS r ON e.ReportDefinitionID = r.ReportDefinitionID
LEFT JOIN Nexus.dbo.WorkflowRun AS wr ON e.WorkflowRunID = wr.WorkflowRunID
LEFT JOIN Nexus.dbo.WorkflowDefinition AS w ON wr.WorkflowDefinitionID = w.WorkflowDefinitionID
LEFT JOIN Nexus.dbo.ExecutionHistoryErrorReview AS review ON e.ExecutionHistoryID = review.ExecutionHistoryID

WHERE s.Name IN ('Failed')
AND review.ExecutionHistoryID IS NULL
AND e.CompletedAt >= CONVERT(DATE, SYSDATETIME())
AND e.CompletedAt < DATEADD(DAY, 1, CONVERT(DATE, SYSDATETIME()))

ORDER BY e.CompletedAt, e.ExecutionHistoryID
"""
    with conn.cursor() as csr:
        csr.execute(sql_cmd)
        return csr.fetchall()


def mark_reviewed(conn: pyodbc.Connection, failures: list[pyodbc.Row]) -> None:
    """ Insert execution IDs after successful Telegram delivery. """
    if not failures:
        return
    sql_cmd = 'INSERT INTO Nexus.dbo.ExecutionHistoryErrorReview (ExecutionHistoryID) VALUES (?)'
    with conn.cursor() as csr:
        csr.executemany(sql_cmd, [(failure.ExecutionHistoryID,) for failure in failures])
    conn.commit()


def format_message(failures: list[pyodbc.Row]) -> str:
    """ Format a summary and, when there are at most ten, individual failures. """
    message = f'A total of {len(failures)} Nexus execution errors have been identified!'
    if len(failures) <= 10:
        details = []
        for failure in failures:
            name = failure.ExecutionName or 'Unknown task or report'
            workflow = f" ({failure.WorkflowName})" if failure.WorkflowName else ''
            details.append(
                f'#{failure.ExecutionHistoryID}: {name}{workflow} - {failure.Status}'
            )
        if details:
            message += '\n' + '\n'.join(details)
    return message


def main() -> None:
    script_name = Path(__file__).stem
    _ = misc.initiate_logging(script_name, CONFIG_FILE)

    conn_str = os.getenv('ConnectionStringOdbcRelease')
    with pyodbc.connect(conn_str) as conn:
        prune_old_reviews(conn)
        failures = get_unreviewed_failures(conn)
        if failures:
            notifications.SendTelegramMessage(format_message(failures))
            mark_reviewed(conn, failures)


if __name__ == '__main__':
    main()
