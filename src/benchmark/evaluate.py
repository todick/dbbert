'''
Created on Apr 2, 2021

@author: immanueltrummer
'''
from abc import ABC
from abc import abstractmethod
import glob
import math
import os
import sys
import pandas as pd
import psycopg2
import subprocess
import time
import json
from dbms.generic_dbms import ConfigurableDBMS

class Benchmark(ABC):
    """ Runs a benchmark to evaluate database configuration. """
    
    def __init__(self):
        """ Initializes logging. """
        self.log = []
    
    @abstractmethod
    def evaluate(self):
        """ Evaluates performance for benchmark and returns reward. """
        raise NotImplementedError()
    
    @abstractmethod
    def print_stats(self):
        """ Prints out some benchmark statistics. """
        raise NotImplementedError()
    
    def reset(self, log_path, run_ctr):
        """ Reset timestamps for logging and reset statistics. 
        
        Args:
            log_path: path for logging output
            run_ctr: number of the current run
        """
        self.run_ctr = run_ctr
        self.eval_ctr = 0
        self.start_ms = time.time() * 1000.0
        
        self.log_path = log_path
        self.log_perf_path = log_path + '_performance'
        self.log_conf_path = log_path + '_configure'
        
        if run_ctr == 0:
            with open(self.log_perf_path, 'w') as file:
                file.write('run\teval\tmillis\tbestQuality\tcurQuality\n')
            with open(self.log_conf_path, 'w') as file:
                file.write('run\teval\tmillis\tbestConf\tcurConf\n')
        
        self._init_stats()
            
    @abstractmethod
    def _init_stats(self):
        """ Initializes benchmark statistics. """
        raise NotImplementedError()
            
    def _log(self, best_quality, best_config, cur_quality, cur_config):
        """ Write quality and timestamp to log file. 
        
        Note: this method has no effect if no log file path was specified.
        
        Args:
            best_quality: quality of best current solution (e.g., w.r.t. throughput)
            best_config: description of associated configuration (as dictionary)
            cur_quality: quality of most recently tried configuration
            cur_config: most recently tried configuration
        """
        cur_ms = time.time() * 1000.0
        total_ms = cur_ms - self.start_ms
        log_entry = pd.DataFrame([{
            'Elapsed (ms)':total_ms, 'Evaluations':self.eval_ctr, 
            'Configuration':cur_config, 'Performance':cur_quality, 
            'Best Configuration':best_config, 
            'Best Performance':best_quality}])
        self.log += [log_entry]
                
        if self.log_path:
            with open(self.log_perf_path, 'a') as file:
                file.write(
                    f'{self.run_ctr}\t{self.eval_ctr}\t{total_ms}\t' +
                    f'{best_quality}\t{cur_quality}\n')
            with open(self.log_conf_path, 'a') as file:
                file.write(
                    f'{self.run_ctr}\t{self.eval_ctr}\t{total_ms}\t' +
                    f'{best_config}\t{cur_config}\n')
    
class OLAP(Benchmark):
    """ Runs an OLAP style benchmark with single queries stored in files. """
    
    def __init__(self, dbms: ConfigurableDBMS, query_path):
        """ Initialize with database and path to queries. 
        
        Args:
            dbms: interface for configurable DBMS
            query_path: path to file containing queries
        """
        super().__init__()
        self.dbms = dbms
        self.query_path = query_path
        self.log_path = None
        self._init_stats()
    
    def evaluate(self):
        """ Run all benchmark queries. 
        
        Returns:
            Dictionary containing error flag and time in milliseconds
        """
        self.print_stats()
        self.eval_ctr += 1
        start_ms = time.time() * 1000.0
        error = self.dbms.exec_file(self.query_path)
        end_ms = time.time() * 1000.0
        millis = end_ms - start_ms
        # Update statistics
        config = self.dbms.changed() if self.dbms else None
        if not error:
            if millis < self.min_time:
                self.min_time = millis
                self.min_conf = config
            if millis > self.max_time:
                self.max_time = millis
                self.max_conf = config
        # Logging
        self._log(self.min_time, self.min_conf, millis, config)
        return {'error': error, 'time': millis}
    
    def print_stats(self):
        """ Print out benchmark statistics. """
        print('--- Tuning Updates ---')
        print(f'Minimal time (ms): {self.min_time}')
        print(f'Achieved with configuration: {self.min_conf}')
        print(f'Maximal time (ms): {self.max_time}')
        print(f'Achieved with configuration: {self.max_conf}')
        
    def _init_stats(self):
        """ Initialize minimal and maximal time and configurations. """
        self.min_time = float('inf')
        self.max_time = 0
        self.min_conf = {}
        self.max_conf = {}

class Benchbase(Benchmark):
    """ Runs benchmarks using benchbase. """
    
    def __init__(self, benchbase_path, config_path, result_path, benchmark, timeout, dbms):
        """ Initialize with given paths. 
        
        Args:
            benchbase_path: path to the benchbase base directory
            config_path: path to configuration file
            result_path: store benchmark results here
            dbms: configurable DBMS (not the benchmark database)
            benchbase: the benchmark to run
        """
        super().__init__()
        self.benchbase_path = benchbase_path
        self.config_path = config_path
        self.result_path = result_path
        self.dbms = dbms
        self.template_db = "benchbase_template"
        self.target_db = "benchbase"
        self.benchmark = benchmark
        self.timeout = timeout
        self._init_stats()        
        self.log_path = None
        
    def evaluate(self):
        """ Evaluates current configuration on TPC-C benchmark.
        
        Returns:
            Dictionary containing error flag and throughput
         """
        self._remove_benchbase_results()
        self.eval_ctr += 1
        throughput = -1
        time = -1
        had_error = True
        config = self.dbms.changed() if self.dbms else None
        # Code should be reusable for throughput-based benchmarks
        try:
            # Run benchmark                
            print(f'Starting {self.benchmark} benchmark.')
            sys.stdout.flush()
            return_code = subprocess.run(\
                ['java', '-jar', 'benchbase.jar', '-b', self.benchmark, '-c', self.config_path,
                '--execute=true', '-d', self.result_path],
                cwd = self.benchbase_path, timeout=self.timeout, stdout=open(os.devnull, 'wb'))
            print(f'Benchmark return code: {return_code}')   
                
            # Extract throughput from generated files
            results_file = max(glob.iglob(f'{self.result_path}/*.summary.json'), key=os.path.getctime)
            results = json.load(open(results_file))
            
            # Throughput based benchmarks
            if(self.benchmark == "tpcc"):
                throughput = results['Throughput (requests/second)']
                if not math.isnan(throughput) and return_code != 0:
                    print(f'Measured valid throughput: {throughput}')
                    had_error = False
                else:
                    print(f'Error - throughput is NaN!')
                if not had_error:
                    if throughput > self.max_throughput:
                        self.max_throughput = throughput
                        self.max_config = config
                    if throughput < self.min_throughput:
                        self.min_throughput = throughput
                        self.min_config = config
                # Logging
                self.print_stats()
                self._log(self.max_throughput, self.max_config, throughput, config)
                return {'error': had_error, 'throughput': throughput}
            
            # Time-based benchmarks
            elif(self.benchmark == "tpch"):
                time = results['Latency Distribution']['Average Latency (microseconds)'] / 1000000.0
                if not math.isnan(time) and return_code != 0:
                    print(f'Measured average latency: {time}')
                    had_error = False
                else:
                    print(f'Error - latency is NaN!')
                if not had_error:
                    if time > self.max_time:
                        self.max_time = time
                        self.max_config = config
                    if time < self.min_time:
                        self.min_time = time
                        self.min_config = config
                # Logging
                self.print_stats()
                self._log(self.min_time, self.min_config, time, config)
                return {'error': had_error, 'time': time}
            else:
                raise ValueError(f'{self.benchmark} is currently not supported')
        except (Exception, psycopg2.DatabaseError) as e:
            print(f'Exception for {self.benchmark}: {e}')
            if(self.benchmark == "tpcc"):
                self.print_stats()
                self._log(self.max_throughput, self.max_config, throughput, config)
                return {'error': had_error, 'throughput': throughput}
            elif(self.benchmark == "tpch"):
                self.print_stats()
                self._log(self.min_time, self.min_config, time, config)
                return {'error': had_error, 'time': time}
            else:
                raise ValueError(f'{self.benchmark} is currently not supported')
    
    def print_stats(self):
        """ Print out benchmark statistics. """
        if(self.benchmark == "tpcc"):
            print(f'Minimal throughput {self.min_throughput} with configuration {self.min_config}')
            print(f'Maximal throughput {self.max_throughput} with configuration {self.max_config}')
        elif(self.benchmark == "tpch"):
            print(f'Minimal time {self.min_time} with configuration {self.min_config}')
            print(f'Maximal time {self.max_time} with configuration {self.max_config}')    
        else:
            raise ValueError(f'{self.benchmark} is currently not supported')        
        
    def _init_stats(self):
        """ Reset minimal and maximal throughput (and configurations). """
        if(self.benchmark == "tpcc"):
            self.min_throughput = float('inf')
            self.min_config = {}
            self.max_throughput = 0
            self.max_config = {}
        elif(self.benchmark == "tpch"):
            self.min_time = float('inf')
            self.min_config = {}
            self.max_time = 0
            self.max_config = {}
        else:
            raise ValueError(f'{self.benchmark} is currently not supported')
        
        
    def _remove_benchbase_results(self):
        """ Removes old result files from Benchbase benchmark. """
        files = glob.glob(f'{self.result_path}/*')
        for f in files:
            try:
                os.remove(f)
            except OSError as e:
                print("Error: %s : %s" % (f, e.strerror))