## Temporal Generalized Association Rules

This library provides four algorithms related to Association Rule mining. 
The algorithms are:
- vertical_apriori
- vertical_cumulate
- htar
- htgar

These algorithms use a transactional dataset that is transformed to a vertical format for optimization.
Dataset MUST follow the following format:

| order_id | product_name |
|----------|--------------|
| 1        | Bread        |
| 1        | Milk         |
| 2        | Bread        |
| 2        | Beer         |
| 3        | Eggs         |

Or if timestamps are provided:

| order_id | timestamp | product_name | 
|----------|-----------|--------------|
| 1        | 852087600 | Bread        |
| 1        | 852087600 | Milk         |
| 2        | 854420400 | Bread        |
| 2        | 854420400 | Beer         |
| 3        | 854420400 | Eggs         |

Each field is separated by ","

## TGAR 

This is the main class that must be instantiated once. 

### Usage 
import TemporalGeneralizedRules

tgar = TemporalGeneralizedRules.TGAR()


## Vertical Apriori

This algorithm has four parameters:
- filepath: Filepath of the dataset in csv format with the format discussed in the previous section.
- min_supp: Minimum support threshold.
- min_conf: Minimum confidence threshold.
- parallel_count: Optional parameter that enables parallelization in candidate count phase of the algorithm.

### Usage
tgar.apriori("dataset.csv", 0.05, 0.5)

## Vertical Cumulate 

This algorithm has five parameters:
- filepath: Filepath of the dataset in csv format with the format discussed in the previous section.
- min_supp: Minimum support threshold.
- min_conf: Minimum confidence threshold.
- min_r: Minimum R-interesting threshold.
- parallel_count: Optional parameter that enables parallelization in candidate count phase of the algorithm. It can make execution faster.

### Usage
tgar.vertical_cumulate("dataset.csv", 0.05, 0.5, 1.1)

## HTAR 
This algorithm has four parameters:
- filepath: Filepath of the dataset in csv format with the format discussed in the previous section.
- min_supp: Minimum support threshold.
- min_conf: Minimum confidence threshold.
- parallel_count: Optional parameter that enables parallelization in candidate count phase of the algorithm. It can make execution faster.

### Usage
tgar.htar("dataset.csv", 0.05, 0.5)


## HTGAR 
This algorithm has five parameters:
- filepath: Filepath of the dataset in csv format with the format discussed in the previous section.
- min_supp: Minimum support threshold.
- min_conf: Minimum confidence threshold.
- min_r: Minimum R-interesting threshold.
- parallel_count: Optional parameter that enables parallelization in candidate count phase of the algorithm. It can make execution faster.

### Usage

tgar.htgar("dataset.csv", 0.05, 0.5, 1.1)


## Pypy

For a better performance we recommend using this package with Pypy, a faster implementation of python.

https://www.pypy.org/




## Bibliography

The following  were based on the following papers:
- Apriori : https://dl.acm.org/doi/10.5555/645920.672836
- Cumulate: https://www.sciencedirect.com/science/article/pii/S0167739X97000198
- HTAR: https://www.sciencedirect.com/science/article/pii/S2210832716000041