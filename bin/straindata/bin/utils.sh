#
set -o pipefail

# ---------------------
# Echos its arguments to the log file. Prepends a datetime stamp.
#
function logit {
    echo `date` "$*" >> ${LOGFILE}
}

# ---------------------
# Logs a message and exits with error code 1.
#
function die {
    logit "$*"
    exit 1
}

# ---------------------
# Tests an assertion. 
# If success, logs OK and returns 0.
# If failure, logs FAILED, and return 1.
# Arguments:
#    label	$1 = label to print for test (e.g., title)
#    val1	$2 = the computed valued to test (e.g., the count of protein coding genes)
#    op		$3 = the operator to use. One of: -eq -ne -gt -ge -lt -le
#    val2	$4 = the value to test against, e.g., a sanity threshhold value for number of protein coding genes
#
function assert {
    test $2 $3 $4
    if [ $? -ne 0 ]; then
        logit "FAILED ASSERTION: $1: $2 $3 $4"
	return 1
    else
	logit "OK: $1: $2 $3 $4"
	return 0
    fi
}

# ---------------------
# If the exit code ($?) is not zero, exits with a message.
#
function checkExit {
    c=$?
    if [ $c -ne 0 ]; then
        die "ERROR: Caught error exit code." 
    fi
    return 0
}

# ---------------------
function distinct {
    grep -v "^#" $1 | cut -f $2 | sort | uniq
}
