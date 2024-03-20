'''
Created on May 12, 2021

@author: immanueltrummer
'''
import benchmark.evaluate
import search.objectives

def from_args(args, dbms):
    """ Generate benchmark object from command line arguments.
    
    Args:
        args: dictionary containing command line arguments.
        dbms: represents database system executing benchmark.
    
    Returns:
        tuple of optimization objective and benchmark.
    """
    if args.benchmark_type == 'olap':
        if args.query_path is not None:
            # Tune for minimizing run time of given workload
            objective = search.objectives.Objective.TIME
            bench = benchmark.evaluate.OLAP(dbms, args.query_path)
            return objective, bench
        else:
            raise ValueError('OLAP style benchmarks need to have query path specified!')
    elif args.benchmark_type == 'benchbase':
        if(args.benchmark == "tpcc"):
            objective = search.objectives.Objective.THROUGHPUT
        else:
            objective = search.objectives.Objective.TIME
        benchbase_home = args.benchbase_home
        benchbase_config = args.benchbase_config
        benchbase_result = args.benchbase_result
        benchmark_name = args.benchmark
        timeout = args.benchbase_timeout
        
        bench = benchmark.evaluate.Benchbase(
            benchbase_home, benchbase_config, benchbase_result, 
            benchmark_name, timeout, dbms)
        return objective, bench
    else: 
        raise ValueError('OLTP style benchmarks are not supported!')