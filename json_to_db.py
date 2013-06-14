__author__ = 'kreddy'

import json
import sys
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Numeric
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

engine = create_engine('mysql://root@localhost/ddl', echo=False)
Session = sessionmaker(bind=engine)

class Error(Base):
    __tablename__ = 'errors'

    id = Column(Integer, primary_key=True)
    exception = Column(String)
    service = Column(String)
    event_id = Column(Integer)
    failure_type = Column(String)
    req = Column(String)
    error_when = Column(Date)
    platform = Column(String)
    host = Column(String)
    deployment = Column(String)
    duration = Column(Numeric)
    operation = Column(String)
    event = Column(String)
    tenant = Column(String)
    error_state = Column(String)

    def __init__(self, exception, service, event_id,
                 failure_type, req, when, platform,
                 host, deployment, duration, operation,
                 event, tenant, last_state):
        self.exception = exception
        self.service = service
        self.event_id = event_id
        self.failure_type = failure_type
        self.req = req
        self.error_when = when
        self.platform = platform
        self.host = host
        self.deployment = deployment
        self.duration = duration
        self.operation = operation
        self.event = event
        self.tenant = tenant
        self.error_state = last_state


def format_exception(d):
    if 'exception' not in d:
        return ''

    for k, v in d['exception'].iteritems():
        value = ', '.join(v) if isinstance(v, list) else str(v)
        exception = k + ':' + value
    return exception


def get_error_state(d):
    for detail in d['details']:
        if detail['state'] == 'error':
            return 'True'
    return 'False'

def parseAndPopulate(args):
    session = Session()

    with open(args[0]) as data_file:
        data = json.load(data_file)


    for d in data:
        if 'instances' not in d:
            exception = format_exception(d)
            error_state = get_error_state(d)
            duration = float(d['duration'].replace(' minutes', ''))
            err = Error(exception, d['service'], d['event_id'],
                        d['failure_type'], d['req'], d['when'],
                        ",".join(d['platform']), d['host'], d['deployment'],
                        duration, d['operation'], d['event'], d['tenant'],
                        error_state)
            session.add(err)
    session.commit()

def create_table():
    sql='''\
    CREATE TABLE IF NOT EXISTS errors (id int auto_increment primary key,
    exception varchar(4000),
    service varchar(255),
    event_id int,
    failure_type varchar(255),
    req varchar(255),
    error_when DATETIME,
    platform varchar(255),
    host varchar(255),
    deployment varchar(255),
    duration Decimal(65,2),
    operation varchar(255),
    event varchar(255),
    error_state varchar(255),
    tenant varchar(255))'''
    engine.execute(sql)

if __name__ == "__main__":
    create_table()
    parseAndPopulate(sys.argv[1:])
