'''
Created on Apr 16, 2021

@author: immanueltrummer
'''
from collections import defaultdict
from dbms.generic_dbms import ConfigurableDBMS
from benchmark.evaluate import Benchmark
from parameters.util import is_numerical, convert_to_bytes
from search.objectives import calculate_reward
from search.search_with_hints import ParameterExplorer

class ParameterResults():
    def __init__(self):
        self.best_value = 0
        self.best_reward = -float('inf')
        self.tested_values = {}
        
    def add_result(self, value, reward):
        self.tested_values[value] = reward
        if reward > self.best_reward:
            self.best_value = value
            self.best_reward = reward
            
    def has_value(self, value):
        return value in self.tested_values

class FeatureWiseExplorer(ParameterExplorer):
    """ Explores the parameter space using previously collected tuning hints. """

    def __init__(self, dbms: ConfigurableDBMS, benchmark: Benchmark, objective):
        """ Initializes for given benchmark and database system. 
        
        Args:
            dbms: explore parameters of this database system.
            benchmark: optimize parameters for this benchmark.
            objective: goal of parameter optimization.
        """
        super().__init__(dbms, benchmark)
        self.max_reward = 0
        self.tested_parameters = {}

    def _def_conf_metrics(self):
        """ Returns metrics for running benchmark with default configuration. """
        if self.dbms and self.benchmark:
            self.dbms.reset_config()
            self.dbms.reconfigure() 
            for _ in range(2):
                # This value is taken as a reference to evaluate later configurations, so we run the
                # benchmark twice times and take the second result to account for caching and engine
                # optimizers. It is better for the performance to be estimated too fast than too slow
                res = self.benchmark.evaluate() 
            return res
        else:
            print('Warning: no DBMS or benchmark specified for parameter exploration.')
            return {'error': False, 'time': 0}
        
    def explore(self, hint_to_weight, nr_evals):
        """ Explore parameters to improve benchmark performance.
        
        Args:
            hint_to_weight: use weighted hints as guidelines for exploration
            nr_evals: evaluate so many parameter configurations
        
        Returns:
            Returns maximal improvement and associated configuration
        """
        print(f'Weighted hints: {hint_to_weight}')
        configs = self._select_configs(hint_to_weight, nr_evals)
        print(f'Selected configurations: {configs}')
        # Identify best configuration
        max_reward = 0
        best_config = {}
        for config in configs:
            reward = self._evaluate_config(config)
            if reward > max_reward:
                max_reward = reward
                best_config = config
        print(f'Obtained {max_reward} by configuration {best_config}')
        if max_reward > self.max_reward:
            self.max_reward = max_reward
            self._evaluate_parameters(best_config)    
        recommended_config = {}
        self._include_tested_parameters(recommended_config)
        if len(recommended_config) > 1:
            self._evaluate_config(recommended_config)
        return max_reward, best_config

    def _evaluate_parameters(self, best_config):            
        print('Benchmarking parameters individually')  
        # Evaluate parameters
        for p, val in best_config.items():
            if p in self.tested_parameters:
                if self.tested_parameters[p].has_value(val):
                    continue
            else:
                self.tested_parameters[p] = ParameterResults()
            reward = self._evaluate_config({p : val})
            self.tested_parameters[p].add_result(val, reward)
            print(f'Obtained {reward} by setting {p} to {val}')

    def _include_tested_parameters(self, config):
        for p, res in self.tested_parameters.items():
            if p in config:
                if config[p] in res.tested_values and res.tested_values[config[p]] < 2:
                    # Exclude parameter settings that we know affect performance negatively
                    config.pop(p)
            else:
                if res.best_reward > 2:
                    # Include parameter settings that we know affect performance positively
                   config[p] = res.best_value 

    def _select_configs(self, hint_to_weight, nr_evals):
        """ Returns set of interesting configurations, based on hints. 
        
        Args:
            hint_to_weight: maps assignments to a weight
            nr_evals: select that many configurations
            
        Returns:
            List of configurations to try out
        """
        param_to_w_vals = self._gather_values(hint_to_weight)
        configs = []
        for _ in range(nr_evals):
            config = self._next_config(configs, param_to_w_vals)  
            configs.append(config)
        for config in configs:            
            self._include_tested_parameters(config)      
        return configs      