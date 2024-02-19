'''
Created on Nov 8, 2023

@author: tobiasdick
'''
from dbms.generic_dbms import ConfigurableDBMS

import mariadb
import os
from parameters.util import is_numerical
import time

class MariaDBconfig(ConfigurableDBMS):
    """ Represents configurable MariaDB database. Since MariaDB is based on MySQL, it 
        is nearly identical to the representation of MySQL. """
    
    def __init__(self, db, user, password, 
                 restart_cmd, recovery_cmd, timeout_s):
        """ Initialize DB connection with given credentials. 
        
        Args:
            db: name of MariaDB database
            user: name of MariaDB user
            password: password for database
            restart_cmd: command to restart server
            recovery_cmd: command to recover database
            timeout_s: per-query timeout in seconds
        """
        unit_to_size={'KB':'*1024', 'MB':'*1024*1024', 'GB':'*1024*1024*1024',
                      'K':'*1024', 'M':'*1024*1024', 'G':'*1024*1024*1024'}
        super().__init__(db, user, password, unit_to_size,
                         restart_cmd, recovery_cmd, timeout_s)
        self.global_vars = [t[0] for t in self.query_all(
            'show global variables') if is_numerical(t[1])]
        self.all_variables = self.global_vars
            
        print(f'Global variables: {self.global_vars}')
        print(f'All parameters: {self.all_variables}')
        
    @classmethod
    def from_file(cls, config):
        """ Initialize DBMS from configuration file. 
        
        Args:
            cls: class (currently: MariaDBconfig only)
            config: configuration read from file
            
        Returns:
            new MariaDB DBMS object
        """
        db_user = config['DATABASE']['user']
        db_name = config['DATABASE']['name']
        password = config['DATABASE']['password']
        restart_cmd = config['DATABASE']['restart_cmd']
        recovery_cmd = config['DATABASE']['recovery_cmd']
        timeout_s = config['LEARNING']['timeout_s']
        return cls(db_name, db_user, password, 
                   restart_cmd, recovery_cmd, timeout_s)
        
    def __del__(self):
        """ Close DBMS connection if any. """
        super().__del__()
        
    def copy_db(self, source_db, target_db):
        """ Copy source to target database. """
        mdb_clc_prefix = f'mariadb -u{self.user} -p{self.password} '
        mdb_dump_prefix = f'mariadb-dump -u{self.user} -p{self.password} '
        os.system(mdb_dump_prefix + f' {source_db} > copy_db_dump')
        print('Dumped old database')
        os.system(mdb_clc_prefix + f" -e 'drop database if exists {target_db}'")
        print('Dropped old database')
        os.system(mdb_clc_prefix + f" -e 'create database {target_db}'")
        print('Created new database')
        os.system(mdb_clc_prefix + f" {target_db} < copy_db_dump")
        print('Initialized new database')
            
    def _connect(self):
        """ Establish connection to database, returns success flag. 
        
        Returns:
            True if connection attempt is successful
        """
        print(f'Trying to connect to {self.db} with user {self.user}')
        # Need to recover in case of bad configuration
        try:
            self.connection = mariadb.connect(
                database=self.db, user=self.user, 
                password=self.password, host="0.0.0.0", port=3306)
            self.set_timeout(self.timeout_s)
            self.failed_connections = 0
            return True
        except Exception as e:
            print(f'Exception while trying to connect to MariaDB: {e}')
            self.failed_connections += 1
            print(f'Had {self.failed_connections} failed tries.')
            if self.failed_connections < 3:
                print(f'Trying recovery with "{self.recovery_cmd}" ...')
                os.system(self.recovery_cmd)
                os.system(self.restart_cmd)            
                self.reset_config()
                self.reconfigure()
            return False
        
    def _disconnect(self):
        """ Disconnect from database. """
        if self.connection:
            print('Disconnecting ...')
            self.connection.close()
    
    def exec_file(self, path):
        """ Executes all SQL queries in given file and returns error flag. """
        try:
            self.connection.autocommit = True
            with open(path) as file:
                sql = file.read()
                for query in sql.split(';'):
                    if len(query) == 0 or query.isspace():
                        continue
                    cursor = self.connection.cursor(buffered=True)
                    cursor.execute(query)
                    cursor.close()
            error = False
        except Exception as e:
            error = True
            print(f'Exception executing {path}: {e}')
        return error
    
    def query_one(self, sql):
        """ Runs SQL query_one and returns one result if it succeeds. """
        try:
            cursor = self.connection.cursor(buffered=True)
            cursor.execute(sql)
            return cursor.fetchone()[0]
        except Exception:
            return None
    
    def query_all(self, sql):
        """ Runs SQL query and returns all results if it succeeds. """
        try:
            cursor = self.connection.cursor(buffered=True)
            cursor.execute(sql)
            results = cursor.fetchall()
            cursor.close()
            return results
        except Exception as e:
            print(f'Exception in mariadb.query_all: {e}')
            return None
    
    def update(self, sql):
        """ Runs an SQL update and returns true iff the update succeeds. """
        print(f'Trying update {sql}')
        self.connection.autocommit = True
        cursor = self.connection.cursor(buffered=True)
        try:
            cursor.execute(sql)
            success = True
        except Exception as e:
            print(f'Exception during update: {e}')
            success = False
        cursor.close()
        return success
    
    def is_param(self, param):
        """ Returns True iff the given parameter can be configured. """
        return param in self.all_variables
    
    def get_value(self, param):
        """ Returns current value for given parameter. """
        if param in self.global_vars:
            return self.query_one(f'select @@{param}')
        else:
            return None
    
    def set_param(self, param, value):
        """ Set parameter to given value. """
        if param in self.global_vars:
            success = self.update(f'set global {param}={value}')
        else: 
            success = False
        if success:
            self.config[param] = value
        return success
    
    def set_timeout(self, timeout_s):
        """ Set per-query timeout. """
        timeout_ms = int(timeout_s * 1000)
        self.update(f"set session max_statement_time = {timeout_ms}")
    
    def all_params(self):
        """ Returns list of tuples, containing configuration parameters and values. """
        return self.all_variables
    
    def reset_config(self):
        """ Reset all parameters to default values. """
        self._disconnect()
        os.system(self.restart_cmd)
        self._connect()
        self.config = {}
    
    def reconfigure(self):
        """ Makes all parameter changes take effect (may require restart). 
        
        Returns:
            Whether reconfiguration was successful
        """
        # Currently, we consider no MariaDB parameters requiring restart
        return True