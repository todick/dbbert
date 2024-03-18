'''
Created on May 12, 2021

@author: immanueltrummer
'''
from dbms.postgres import PgConfig
from dbms.mysql import MySQLconfig
from dbms.mariadb import MariaDBconfig
from dbms.cockroach import CockroachConfig


def from_file(config):
    """ Initialize DBMS object from configuration file. 
    
    Args:
        config: parsed configuration file
        
    Return:
        Object representing Postgres or MySQL
    """
    dbms_name = config['DATABASE']['dbms']
    if dbms_name == 'pg':
        return PgConfig.from_file(config)
    elif dbms_name == 'ms':
        return MySQLconfig.from_file(config)
    elif dbms_name == 'md':
        return MariaDBconfig.from_file(config)
    elif dbms_name == 'cr':
        return CockroachConfig.from_file(config)
    else:        
        raise ValueError(f'DBMS {dbms_name} is not supported!')


def from_args(args):
    """ Initialize DBMS object from command line arguments.
    
    Args:
        args: dictionary containing command line arguments.
    
    Returns:
        DBMS object.
    """
    if args.dbms == 'pg':
        return PgConfig(
            args.db_name, args.db_user, args.db_pwd, args.restart_cmd, 
            args.recover_cmd)
    elif args.dbms == 'ms':
        return MySQLconfig(
            args.db_name, args.db_user, args.db_pwd, args.restart_cmd, 
            args.recover_cmd)
    elif args.dbms == 'md':
        return MariaDBconfig(
            args.db_name, args.db_user, args.db_pwd, args.restart_cmd, 
            args.recover_cmd)
    elif args.dbms == 'cr':
        return CockroachConfig(
            args.db_name, args.db_user, args.db_pwd, args.restart_cmd, 
            args.recover_cmd)
    else:
        raise ValueError(f'DBMS {args.dbms} is not supported!')