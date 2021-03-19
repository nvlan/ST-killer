# README #

This lambda function looks for and kills dangling scheduled tasks.
If a scheduled task is running for over 3 hours, or if it is not of the latest
revision, it will be TERMINATED.
