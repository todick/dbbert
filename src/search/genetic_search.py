from dbms.generic_dbms import ConfigurableDBMS
from benchmark.evaluate import Benchmark
from search.objectives import calculate_reward
from doc.collection import DocCollection, HintType
from random import random, randint, shuffle
from collections import defaultdict

class GeneticExplorer():
    """ Explores the parameter space using previously collected tuning hints. """

    def __init__(self, docs: DocCollection, hardware, dbms: ConfigurableDBMS, benchmark: Benchmark, objective, 
                 population_size, p_crossover, n_mutations):
        """ Initializes for given benchmark and database system. 
        
        Args:
            dbms: explore parameters of this database system.
            benchmark: optimize parameters for this benchmark.
            objective: goal of parameter optimization.
        """
        self.docs = docs
        self.hardware = hardware
        self.dbms = dbms
        self.benchmark = benchmark
        self.objective = objective
        self.population_size = population_size
        self.param_to_values = self._process_hints()
        self.params, self.population = self._initialize_population()
        self.def_metrics = self._def_conf_metrics()
        self.p_mutate = n_mutations / len(self.param_to_values)
        self.p_crossover = p_crossover
    
    def _def_conf_metrics(self):
        """ Returns metrics for running benchmark with default configuration. """
        if self.dbms and self.benchmark:
            self.dbms.reset_config()
            self.dbms.reconfigure() 
            return self.benchmark.evaluate() 
        else:
            print('Warning: no DBMS or benchmark specified for parameter exploration.')
            return {'error': False, 'time': 0}
        
    def explore(self, generations):
        for generation in range(generations):            
            scores = [self._evaluate_chromosome(c) for c in self.population]
            print(f'Generation {generation}:')
            for i in range(self.population_size):
                print(f'\tChromosome {i}: config={self._chromosome_to_config(self.population[i])}, score={scores[i]}')
            
            parents = self._select_parents(scores)
            children = []
            for i in range(0, self.population_size, 2):
                # crossover and mutation
                c1, c2 = self._crossover(parents[i], parents[i+1])
                self._mutate(c1)
                self._mutate(c2)
                children.extend([c1, c2])
            print(f'Selected parents:')
            for p in parents:
                print(f'\t{self._chromosome_to_config(p)}')
            print(f'Cildren:')
            for p in children:
                print(f'\t{self._chromosome_to_config(p)}')
            self.population = children
             
    def _initialize_population(self):
        params, population = [], []
        for _ in range(self.population_size):
            chromosome = []
            for p, values in self.param_to_values.items():
                params.append(p)
                chromosome.append(randint(0, len(values)))
            population.append(chromosome)
        return params, population
    
    def _select_parents(self, scores):
        # Randomly select chromosomes from the half of the population that performs best
        scored = {scores[i] : self.population[i] for i in range(len(scores))}
        candidates = [v for _, v in sorted(scored.items())]
        candidates = candidates[int(len(candidates)/2):]
        selection = []    
        for _ in range(self.population_size):
            selection.append(candidates[randint(0, len(candidates) - 1)])
        return selection
        #selection = randint(0, self.population_size - 1)
        #for i in [randint(0, self.population_size - 1) for _ in range(k)]:
        #    if scores[i] < scores[selection]:
        #        selection = i
        #return self.population[i]

    def _crossover(self, p1, p2):
        c1, c2 = p1.copy(), p2.copy()
        if random() < self.p_crossover:
            for i in range(len(p1)):
                if random() < 0.5:
                    c1[i] = p2[i]
                    c2[i] = p1[i]
                else:
                    c1[i] = p1[i]
                    c2[i] = p2[i]
        return c1, c2
    
    def _mutate(self, chromosome):
        for i in range(len(self.param_to_values)):
            if random() < self.p_mutate:
                chromosome[i] = randint(0, self._gene_value_cap(i))
        
    def _gene_value_cap(self, index):
        return len(self.param_to_values[self.params[index]])
    
    def _evaluate_chromosome(self, chromosome: list[int]):
        config = self._chromosome_to_config(chromosome)
        if self.dbms:
            self.dbms.reset_config()
            print(f'Trying configuration: {config}')
            for param, value in config.items():
                self.dbms.set_param_smart(param, value)
            if self.dbms.reconfigure():
                metrics = self.benchmark.evaluate()
                reward = calculate_reward(metrics, self.def_metrics, self.objective)
            else: 
                reward = -10000
            print(f'Reward {reward} with {config}')
            return reward
        else:
            return 0
            
    def _chromosome_to_config(self, chromosome: list[int]):
        config = {}
        for i in range(len(self.param_to_values)):
            p = self.params[i]
            gene = chromosome[i]
            if gene != 0:
                if gene > self._gene_value_cap(i):
                    print(f'Error: Chromosome {chromosome} has invalid gene {gene} for parameter {p}')
                val = self.param_to_values[p][gene - 1]
                config[p] = val
        return config
    
    def _process_hints(self):
        param_to_hints = self.docs.param_to_hints
        param_to_values = defaultdict(lambda: [])
        for p, hints in param_to_hints.items():
            for _, hint in hints:
                if hint.hint_type == HintType.DISK_RATIO:
                    value = float(self.hardware['disk']) * hint.float_val
                elif hint.hint_type == HintType.RAM_RATIO:
                    value = float(self.hardware['memory']) * hint.float_val
                elif hint.hint_type == HintType.CORES_RATIO:
                    value = float(self.hardware['cores']) * hint.float_val
                elif hint.hint_type == HintType.ABSOLUTE:
                    value = hint.float_val
                else:
                    raise ValueError(f'Unknown hint type: {hint.hint_type}')
                if value.is_integer():                
                    value = str(int(value)) + hint.val_unit
                else:
                    value = str(value) + hint.val_unit
                success = self.dbms.can_set(p, value)
                assignment = (p, value)
                if value not in param_to_values[p]:
                    print(f'Trying assigning {p} to {value}')
                    if success:
                        print(f'Adding assignment {assignment}')
                        param_to_values[p].append(value)
                    else:
                        print(f'Assignment {assignment} was rejected')
        print(f'List of possible values: {dict(param_to_values.items())}')
        return param_to_values
        