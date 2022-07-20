## Temporal Generalized Association Rules

This library provides four algorithms related to Association Rule mining. 
You can download this repository as a package with:

pip install TemporalGeneralizedRules

The algorithms are:
- Apriori
- Cumulate
- HTAR
- HTGAR

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

For taxonomy file use the following format (don't provide headers):

| child    | parent       |
|----------|--------------|
| Bread    | Dairy        |
| Milk     | Dairy        |
| Beer     | Beverage     |

One line for each child, parent

Each field is separated by ","

## TGAR 

This is the main class that must be instantiated once. 

### Usage 
import TemporalGeneralizedRules

tgar = TemporalGeneralizedRules.TGAR()


## Apriori

This algorithm has four parameters:
- filepath: Filepath of the dataset in csv format with the format discussed in the previous section.
- min_supp: Minimum support threshold.
- min_conf: Minimum confidence threshold.
- parallel_count: Optional parameter that enables parallelization in candidate count phase of the algorithm.

### Usage
tgar.apriori("dataset.csv", 0.05, 0.5)

## Cumulate 

This algorithm has six parameters:
- filepath: Filepath of the dataset in csv format with the format discussed in the previous section.
- taxonomy_filepath: Filepath of the taxonomy in csv format with the format discussed in the previous section.
- min_supp: Minimum support threshold.
- min_conf: Minimum confidence threshold.
- min_r: Minimum R-interesting threshold.
- parallel_count: Optional parameter that enables parallelization in candidate count phase of the algorithm. It can make execution faster.

### Usage
tgar.cumulate("dataset.csv", 0.05, 0.5, 1.1)

## HTAR 
This algorithm has four parameters:
- filepath: Filepath of the dataset in csv format with the format discussed in the previous section.
- min_supp: Minimum support threshold.
- min_conf: Minimum confidence threshold.
- parallel_count: Optional parameter that enables parallelization in candidate count phase of the algorithm. It can make execution faster.

### Usage
tgar.htar("dataset.csv", 0.05, 0.5)


## HTGAR 
This algorithm has six parameters:
- filepath: Filepath of the dataset in csv format with the format discussed in the previous section.
- taxonomy_filepath: Filepath of the taxonomy in csv format with the format discussed in the previous section.
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

The algorithms provided in this library were based on the following papers:

- Rakesh Agrawal and Ramakrishnan Srikant. 1994. Fast Algorithms for Mining Association Rules in Large Databases. In Proceedings of the 20th International Conference on Very Large Data Bases (VLDB '94). Morgan Kaufmann Publishers Inc., San Francisco, CA, USA, 487–499. https://dl.acm.org/doi/10.5555/645920.672836


- Ramakrishnan Srikant, Rakesh Agrawal, Mining generalized association rules, Future Generation Computer Systems, Volume 13, Issues 2–3, 1997, Pages 161-180, ISSN 0167-739X. https://www.sciencedirect.com/science/article/pii/S0167739X97000198


- R. Agrawal and J. C. Shafer, "Parallel mining of association rules," in IEEE Transactions on Knowledge and Data Engineering, vol. 8, no. 6, pp. 962-969, Dec. 1996, doi: 10.1109/69.553164. https://ieeexplore.ieee.org/document/553164


- Hong et al., 2016.Hong, T.-P., Lan, G.-C., Su, J.-H., Wu, P.-S., and Wang, S.-L. (2016). Discovery of temporal association rules with hierarchical granular framework. Applied Computing and Informatics, 12(2):134–141 https://www.sciencedirect.com/science/article/pii/S2210832716000041
