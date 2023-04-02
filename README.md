
# PySpark-PCY
This is an implementation of the PCY algorithm for frequent itemset mining using PySpark. The algorithm first runs a Pass One to generate a list of frequent singletons and to build a bitmap of frequent buckets. In Pass Two, candidate itemsets are generated using the frequent singletons and frequent buckets found in Pass One. The candidate itemsets are then filtered to identify the frequent itemsets using the support threshold.

## Installation
Clone the repository
Install PySpark and the required dependencies
Run the code
## Usage
This implementation can be used to find the frequent itemsets in large datasets using PySpark. The function get_candidate_itemset is the main function and takes in two arguments:

`basket_rdd`: The RDD containing the baskets of items
`support_threshold`: The minimum support threshold
The output of the function is an RDD containing the frequent itemsets.


## Contributing
This implementation is open to contributions. If you find any bugs or have suggestions for improvements, feel free to create an issue or a pull request.





