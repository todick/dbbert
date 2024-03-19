'''
Created on Aug 15, 2023

@author: immanueltrummer
'''
import os
os.environ["TOKENIZERS_PARALLELISM"] = "false"
from pybullet_utils.util import set_global_seeds

import argparse
import benchmark.factory
import dbms.factory
import numpy as np
import random
import torch


if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'text_source_path', type=str, help='Path to input text')
    parser.add_argument(
        '--max_length', type=int, default=128, 
        help='Maximal length of text chunk in characters')
    parser.add_argument(
        '--filter_params', type=int, default=1, choices={0, 1},
        help='Set to 1 to filter text passages using heuristics')
    parser.add_argument(
        '--use_implicit', type=int, default=1, choices={0, 1},
        help='Set to 1 to recognize implicit parameter references')
    parser.add_argument(
        '--generations', type=int, default=10, help='Number of generations')
    parser.add_argument(
        '--population', type=int, default=12, help='Size of the population')
    parser.add_argument(
        '--crossover', type=float, default=0.9, help='Crossover probability')
    parser.add_argument(
        '--mutations', type=float, default=3.0, help='Average number of mutations per chromosome per generation')
    parser.add_argument(
        'memory', type=int, default=8000000,
        help='Main memory of target system, measured in bytes')
    parser.add_argument(
        'disk', type=int, default=100000000,
        help='Disk space of target system, measured in bytes')
    parser.add_argument(
        'cores', type=int, default=8, help='Number of cores of target system')
    parser.add_argument(
        'dbms', type=str, choices={'pg', 'ms', 'md', 'cr'},
        help='Set to "pg" to tune PostgreSQL, "ms" to tune MySQL, "md" to tune MariaDB, "cr" to tune CockroachDB')
    parser.add_argument('db_name', type=str, help='Name of database to tune')
    parser.add_argument('db_user', type=str, help='Name of database login')
    parser.add_argument('db_pwd', type=str, help='Password for database login')
    parser.add_argument(
        'restart_cmd', type=str, 
        help='Terminal command for restarting database server')
    parser.add_argument(
        '--recover_cmd', type=str, 
        default='echo "Reset database state!"; sleep 5',
        help='Command to restore default status of database system')
    parser.add_argument(
        '--query_path', type=str, default=None, 
        help='Path to file containing SQL queries')
    parser.add_argument(
        '--result_path_prefix', type=str, default='dbbert_results',
        help='Path prefix for files containing tuning results')
    parser.add_argument(
        '--benchmark_type', type=str, default='olap', choices={'olap', 'benchbase'},
        help='The type of benchmark to run (olap or benchbase)')
    parser.add_argument(
        '--benchmark', type=str, default='tpcc', choices={'tpcc','tpch'},
        help='The benchmark to run (only for benchbase)')
    parser.add_argument(
        '--benchbase_home', type=str, default='benchbase',
        help='Path to benchbase')
    parser.add_argument(
        '--benchbase_config', type=str, default=None,
        help='Path to the benchbase config')
    parser.add_argument(
        '--benchbase_result', type=str, default='tpcc_results',
        help='Path to the output file for benchbase')
    parser.add_argument(
        '--benchbase_timeout', type=int, default=300,
        help='Timeout for benchbase benchmarks in seconds')
    args = parser.parse_args()
    print(f'Input arguments: {args}')
    # Expensive import statements after parsing arguments
    from doc.collection import DocCollection
    from search.genetic_search import GeneticExplorer
    
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    
    dbms = dbms.factory.from_args(args)
    objective, bench = benchmark.factory.from_args(args, dbms)
        
    # Initialize input documents
    docs = DocCollection(
        docs_path=args.text_source_path, dbms=dbms, 
        size_threshold=args.max_length,
        use_implicit=args.use_implicit, 
        filter_params=args.filter_params)
    
    # Initialize environment
    bench.reset(args.result_path_prefix, 0)
    random.seed(1)
    np.random.seed(1)
    torch.manual_seed(0)
    set_global_seeds(0)
    hardware = {'memory':args.memory, 'disk':args.disk, 'cores':args.cores}
    explorer = GeneticExplorer(docs, hardware, dbms, bench, objective, 
                               args.population, args.crossover, args.mutations) 
    explorer.explore(args.generations)
        