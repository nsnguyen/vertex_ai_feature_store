import os
import sqlalchemy
import logging
import snowflake.connector

from sqlalchemy.exc import ProgrammingError
from snowflake.sqlalchemy import URL

LOGGER = logging.getLogger(__name__)

def _dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d


class BaseConnection(object):
  def __init__(self) -> None:
      self._engine = None
      self._schema = None
      
  def _init_database(self, db_type, dialect, user, pwd, host, port):
    url = db_type + '+' + dialect + '://' + user + ':' + pwd + '@' + host + ':' + str(port) + '/' + self.db_name
    self._create_engine(url)
        
  def _create_engine(self, url, **kwargs):
      kwargs['pool_pre_ping'] = True
      self._engine = sqlalchemy.create_engine(url, **kwargs)
      self._engine.connect()
        
  def _init_database_with_url(self, url, connect_args={}):
    self._create_engine(url, connect_args=connect_args)
    
  def _connect(self):
    return self._engine.connect()
        
  def fetchall(self, sql, params=()):
    conn = self._connect()
    conn.row_factory = _dict_factory
    query = conn.execute(sql, params)
    results = query.fetchall()
    results = [dict(u) for u in results]
    return results
  
  

class SnowflakeDB(BaseConnection):
  def __init__(self,
               account=None,
               user=None,
               password=None,
               schema=None,
               warehouse=None,
               database=None,
               role=None) -> None:
    super().__init__()
    self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
    self.user = user or os.getenv("SNOWFLAKE_USER")
    self.password = password or os.getenv("SNOWFLAKE_PASSWORD")
    self.schema = schema or 'PUBLIC'
    self.warehouse = warehouse or 'AD_HOC'
    self.database = database or 'DEV'
    self.role = role or 'DEV'
    
    self.conn = snowflake.connector.connect(
      user=self.user,
      password=self.password,
      account=self.account,
      warehouse=self.warehouse,
      database=self.database,
      schema=self.schema
      )
    # dict of arguments supplied in the URL
    url_args = {
      'user': self.user,
      'password': self.password,
      'account': self.account,
      'database': self.database,
      'warehouse': self.warehouse,
      'schema': self.schema,
      'role': self.role
    }
    
    # dict of arguments supplied as connect_args
    connect_args = {
      'client_session_keep_alive': True
    }
    url = URL(**url_args)
    self._init_database_with_url(url=url, connect_args=connect_args)