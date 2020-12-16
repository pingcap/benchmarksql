import errno
import math
import os
import sys
import time
import pip
import jaydebeapi
import re


# ----
# main
# ----
def main(argv):
    global  deviceFDs
    global  lastDeviceData
    global  first_exec
    first_exec=1
    
    global jdbc_driver_name
    jdbc_driver_name = "org.postgresql.Driver"
    global jdbc_driver_loc
    curpath=os.path.realpath(__file__)
    jdbc_driver_loc = curpath.'../../lib/postgresql-9.3-1102.jdbc41.jar' #/home/azureuser/benchmarksql-5.0/lib/postgres/postgresql-9.3-1102.jdbc41.jar'

    # Transactions
    global sql_tx
    global XACT_COMMIT
    global XACT_ROLLBACK
    global XACT_COMMIT_OLD
    global XACT_ROLLBACK_OLD
    
    XACT_COMMIT_OLD   = 0
    XACT_ROLLBACK_OLD = 0
    
    sql_tx = "SELECT SUM(xact_commit)XACT_COMMIT,SUM(xact_rollback) XACT_ROLLBACK FROM pg_stat_database WHERE datname ="

    # ----
    # Get the runID and collection interval from the command line
    # ----
    runID          = (int)(argv[0])
    interval       = (float)(argv[1])
    connect_string = (str) (argv[2])
    ssl            = (str) (argv[3])
    username       = (str) (argv[4])
    password       = (str) (argv[5])

    global sql_str
    sql_str = "select wait_event_type,wait_event,state,count(*) from pg_stat_activity where usename = '".username."' and application_name ='' group by  wait_event_type,wait_event,state"

    #Extract DBNAME from connect_string
    # Sample connect_string : jdbc:postgresql://database_server_adress.com:port/database_name
    pattern = re.compile(r'jdbc:([A-z0-9\-]+):\/\/([A-z0-9\.]+)([:0-9]*)\/([A-z0-9]+)')
    match = pattern.match(connect_string);
    #print (match)
    db = match.group(4)
    sql_tx = sql_tx+"'"+db+"'"
    if (runID == 0) :
        print(db)
    url = '{0}:user={1};password={2}'.format(connect_string, username, password)

    
    startTime = time.time();
    nextDue = startTime 
    conn = jaydebeapi.connect(jdbc_driver_name, connect_string, {'user': username, 'password': password, 'ssl' : ssl},jars=jdbc_driver_loc)
    curs    = conn.cursor()
    curs_tx = conn.cursor()

    try:
        while True:
            # ----
            # Wait until our next collection interval and calculate the
            # elapsed time in milliseconds.
            # ----
            now = time.time()
            if nextDue > now:
                time.sleep(nextDue - now)
            elapsed = (int)((nextDue - startTime) * 1000.0)

            # ----
            # Collect database informations.
            # ----
            curs.execute(sql_str)
            curs_tx.execute(sql_tx)
            rows    = curs.fetchall()
            row_tx = curs_tx.fetchall()
            if first_exec == 1 :
                first_exec = 0
                #hearders = [i[0] for i in curs.cursor.description]
                col_headers = ",".join([i[0] for i in curs.description])
                print ('runid,time,'+col_headers)
                XACT_COMMIT_OLD    = row_tx[0][0] 
                XACT_ROLLBACK_OLD  = row_tx[0][1] 
                
            # Transactions    
            XACT_COMMIT   =  row_tx[0][0] - XACT_COMMIT_OLD
            XACT_ROLLBACK =  row_tx[0][1] - XACT_ROLLBACK_OLD
            print (str(runID)+','+str(elapsed)+',XACT,XACT_COMMIT,NA,'+   str(XACT_COMMIT))
            print (str(runID)+','+str(elapsed)+',XACT,XACT_ROLLBACK,NA,'+ str(XACT_ROLLBACK))
            XACT_COMMIT_OLD    = row_tx[0][0] 
            XACT_ROLLBACK_OLD  = row_tx[0][1] 


            for row in rows:
                output = ''
                for col in row:
                    e=str(col or '')
                    output=output+e+","
                print (str(runID)+','+str(elapsed)+','+output[:-1])
            
            # Transactions
            # ----
            # Bump the time when we are next due.
            # ----
            nextDue += interval
            sys.stdout.flush()

    # ----
    # Running on the command line for test purposes?
    # ----
    except KeyboardInterrupt:
        print ("")
        return 0

    # ----
    # The DBCollector class will just close our stdout on the other
    # side, so this is expected.
    # ----
    except (BrokenPipeError, IOError) as e:
        if e.errno == errno.EPIPE:
            return 0
        else:
            raise e


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))
