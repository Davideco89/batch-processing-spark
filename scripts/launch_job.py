"""Docker entrypoint; resources are applied before starting the driver JVM."""

import json
import os
import sys
from github_analytics.launcher import submit_command

if __name__ == "__main__":
    command = submit_command(sys.argv[1:])
    print("SparkSubmit " + json.dumps(command), flush=True)
    os.execv(command[0], command)
