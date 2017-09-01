#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, datetime
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)
from pymysqlreplication.event import QueryEvent, RotateEvent, FormatDescriptionEvent
from binlog2sql_util import command_line_args, concat_sql_from_binlogevent, create_unique_file, reversed_lines


class Binlog2sql(object):

    def __init__(self, connectionSettings, startFile=None, startPos=None, endFile=None, endPos=None, startTime=None,
                 stopTime=None, only_schemas=None, only_tables=None, nopk=False, flashback=False, stopnever=False,
                 recover=None):
        '''
        connectionSettings: {'host': 127.0.0.1, 'port': 3306, 'user': slave, 'passwd': slave}
        '''
        if not startFile:
            raise ValueError('lack of parameter,startFile.')

        self.connectionSettings = connectionSettings
        self.startFile = startFile
        self.startPos = startPos if startPos else 4 # use binlog v4
        self.endFile = endFile if endFile else startFile
        self.endPos = endPos
        self.startTime = datetime.datetime.strptime(startTime, "%Y-%m-%d %H:%M:%S") if startTime else datetime.datetime.strptime('1970-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        self.stopTime = datetime.datetime.strptime(stopTime, "%Y-%m-%d %H:%M:%S") if stopTime else datetime.datetime.strptime('2999-12-31 00:00:00', "%Y-%m-%d %H:%M:%S")

        self.schema_map = {}  # new property to store src->dest mapped schema name
        for schema in only_schemas:
            if ':' in schema:
                src_schema, dest_schema = schema.split(':')
            else:
                src_schema = dest_schema = schema
            self.schema_map[src_schema] = dest_schema

        self.only_schemas = list(self.schema_map.keys()) if only_schemas else None
        self.only_tables = only_tables if only_tables else None
        self.recover = recover

        self.nopk, self.flashback, self.stopnever = (nopk, flashback, stopnever)

        self.binlogList = []
        self.connection = pymysql.connect(**self.connectionSettings)
        try:
            cur = self.connection.cursor()
            cur.execute("SHOW MASTER STATUS")
            self.eofFile, self.eofPos = cur.fetchone()[:2]
            cur.execute("SHOW MASTER LOGS")
            binIndex = [row[0] for row in cur.fetchall()]
            if self.startFile not in binIndex:
                raise ValueError('parameter error: startFile %s not in mysql server' % self.startFile)
            binlog2i = lambda x: x.split('.')[1]
            for bin in binIndex:
                if binlog2i(bin) >= binlog2i(self.startFile) and binlog2i(bin) <= binlog2i(self.endFile):
                    self.binlogList.append(bin)

            cur.execute("SELECT @@server_id")
            self.serverId = cur.fetchone()[0]
            if not self.serverId:
                raise ValueError('need set server_id in mysql server %s:%s' % (self.connectionSettings['host'], self.connectionSettings['port']))
        finally:
            cur.close()

    def process_binlog(self):
        stream = BinLogStreamReader(connection_settings=self.connectionSettings, server_id=self.serverId,
                                    log_file=self.startFile, log_pos=self.startPos, only_schemas=self.only_schemas,
                                    only_tables=self.only_tables, resume_stream=True, date_tostr=True, blocking=True)

        cur = self.connection.cursor()
        tmpFile = create_unique_file('%s.%s' % (self.connectionSettings['host'],self.connectionSettings['port'])) # to simplify code, we do not use file lock for tmpFile.
        ftmp = open(tmpFile ,"w")
        flagLastEvent = False
        eStartPos, lastPos = stream.log_pos, stream.log_pos
        try:
            for binlogevent in stream:
                if not self.stopnever:
                    if (stream.log_file == self.endFile and stream.log_pos == self.endPos) or (stream.log_file == self.eofFile and stream.log_pos == self.eofPos):
                        flagLastEvent = True
                    elif datetime.datetime.fromtimestamp(binlogevent.timestamp) < self.startTime:
                        if not (isinstance(binlogevent, RotateEvent) or isinstance(binlogevent, FormatDescriptionEvent)):
                            lastPos = binlogevent.packet.log_pos
                        continue
                    elif (stream.log_file not in self.binlogList) or (self.endPos and stream.log_file == self.endFile and stream.log_pos > self.endPos) or (stream.log_file == self.eofFile and stream.log_pos > self.eofPos) or (datetime.datetime.fromtimestamp(binlogevent.timestamp) >= self.stopTime):
                        break
                    # else:
                    #     raise ValueError('unknown binlog file or position')

                if isinstance(binlogevent, QueryEvent) and binlogevent.query == 'BEGIN':
                    eStartPos = lastPos

                # change dst schema name

                if isinstance(binlogevent, QueryEvent):
                    binlogevent.schema = self.schema_map.get(binlogevent.schema, binlogevent.schema)
                    sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, flashback=self.flashback, nopk=self.nopk)
                    if sql:
                        print sql
                        if self.recover:
                            write_to_queue(sql)
                elif isinstance(binlogevent, WriteRowsEvent) or isinstance(binlogevent, UpdateRowsEvent) or isinstance(binlogevent, DeleteRowsEvent):
                    binlogevent.schema = self.schema_map.get(binlogevent.schema, binlogevent.schema)
                    for row in binlogevent.rows:
                        sql = concat_sql_from_binlogevent(cursor=cur, binlogevent=binlogevent, row=row, flashback=self.flashback, nopk=self.nopk, eStartPos=eStartPos)
                        if self.flashback:
                            ftmp.write(sql + '\n')
                        else:
                            print sql
                            if self.recover:
                                write_to_queue(sql)

                if not (isinstance(binlogevent, RotateEvent) or isinstance(binlogevent, FormatDescriptionEvent)):
                    lastPos = binlogevent.packet.log_pos
                if flagLastEvent:
                    break
            ftmp.close()

            if self.flashback:
                self.print_rollback_sql(tmpFile)
        finally:
            os.remove(tmpFile)
        cur.close()
        stream.close()
        return True

    def print_rollback_sql(self, fin):
        '''print rollback sql from tmpfile'''
        with open(fin) as ftmp:
            sleepInterval = 1000
            i = 0
            for line in reversed_lines(ftmp):
                print line.rstrip()
                if self.recover:
                    write_to_queue(line.rstrip())
                if i >= sleepInterval:
                    print 'SELECT SLEEP(1);'
                    i = 0
                else:
                    i += 1

    def __del__(self):
        pass

q = None
# dbconn = None
dest_dbinfo = None
def write_to_queue(sql):
    """
    write generated sqls to redis queue. it's usfull to stage the sqls that wait to replay or rollback
    rq worker or worker function (@TODO)
    """
    q.enqueue(recover_to_db, sql, dest_dbinfo)

if __name__ == '__main__':
    global q
    global dest_dbinfo
    args = command_line_args(sys.argv[1:])
    recover = 0
    if args.dest_dsn:
        from binlog2sql_util import recover_to_db
        import redis
        from rq import Queue
        dest_dbinfo = args.dest_dsn
        # dest_dbinfo = args.dest_dsn
        # dest_dbinfo['charset'] = 'utf8mb4'
        # dbconn = pymysql.Connect(**dest_dbinfo)
        # dbconn.autocommit(1)
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
        redis_conn = redis.from_url(redis_url)
        q = Queue(connection=redis_conn)
        recover = 1  # also work as a flag

    connectionSettings = {'host':args.host, 'port':args.port, 'user':args.user, 'passwd':args.password}
    binlog2sql = Binlog2sql(connectionSettings=connectionSettings, startFile=args.startFile,
                            startPos=args.startPos, endFile=args.endFile, endPos=args.endPos,
                            startTime=args.startTime, stopTime=args.stopTime, only_schemas=args.databases,
                            only_tables=args.tables, nopk=args.nopk, flashback=args.flashback, stopnever=args.stopnever,
                            recover=recover)
    binlog2sql.process_binlog()
