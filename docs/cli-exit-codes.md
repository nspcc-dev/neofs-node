
# Command Line Interface (CLI) Return Codes

The NeoFS CLI returns specific exit codes to indicate the outcome of command execution.

## Exit Codes

| Exit Code | Meaning                                        |
|-----------|------------------------------------------------|
| 0         | Command executed successfully.                 |
| 1         | Internal error or an unspecified failure.      |
| 2         | Object access denied or unauthorized.          |
| 3         | Await timeout expired for a certain condition. |
| 4         | Object already removed.                        |
| 5         | Incomplete success (like partial PUT or SEARCH).|
| 6         | Busy (can be retried in future).               |



These exit codes allow you to understand the outcome of the executed command and handle it accordingly in your scripts or automation workflows.
