from pyspark import SparkContext
import time
import copy
import sys
import collections
from functools import reduce
from itertools import combinations, permutations
from operator import add

def pass_one(baskets, support):
    # Initialize a bitmap to keep track of frequent buckets
    bitmap = [0] * 70
    # Initialize a dictionary to count the occurrences of each item
    item_counts = collections.defaultdict(int)

    # Count the occurrences of items and buckets in the baskets
    for basket in baskets:
        for item in basket:
            item_counts[item] += 1
        for pair in combinations(basket, 2):
            bucket_id = sum(map(int, pair)) % 70
            bitmap[bucket_id] += 1

    # Filter out infrequent items and get the list of frequent singletons
    frequent_singletons = sorted(item for item, count in item_counts.items() if count >= support)
    # Create a bitmap of frequent buckets
    frequent_buckets_bitmap = [count >= support for count in bitmap]

    return frequent_singletons, frequent_buckets_bitmap



def PCY(baskets, original_support, length):
    
    baskets = copy.deepcopy(list(baskets))
    support = int(original_support * len(list(baskets)) / length) + 1
    
    baskets_list = list(baskets)
    candidates = {}

    frequent_singleton, bitmap = pass_one(baskets_list, support)
    index = 1
    candidate_list = frequent_singleton
    candidates[str(index)] = [tuple(item.split(",")) for item in frequent_singleton]

    
    while len(candidate_list) > 0:
        index += 1
        temp_counter = collections.defaultdict(list)
        for basket in baskets_list:
            basket = sorted(list(set(basket).intersection(set(frequent_singleton))))
            if len(basket) >= index:
                if index == 2:
                    for pair in combinations(basket, index):
                        if bitmap[sum(map(lambda x: int(x), list(pair)))%70]:
                            temp_counter[pair].append(1)

                if index >= 3:
                    for candidate_item in candidate_list:
                        if set(candidate_item).issubset(set(basket)):
                            temp_counter[candidate_item].append(1)

        
        filtered_dict = dict(filter(lambda elem: len(elem[1]) >= support, temp_counter.items()))
        
        candidate_list = get_permutes(sorted(list(filtered_dict.keys())))
        if len(filtered_dict) == 0:
            break
        candidates[str(index)] = list(filtered_dict.keys())

    yield reduce(lambda a, b: a + b, candidates.values())



def get_frequents(baskets, candidate_pairs):
    
    temp_counter = collections.defaultdict(list)
    for pairs in candidate_pairs:
        if set(pairs).issubset(set(baskets)):
            temp_counter[pairs].append(1)

    yield [tuple((key, sum(value))) for key, value in temp_counter.items()]


def format_itemsets(itemsets):
    
    formatted_itemsets = []
    prev_len = None

    for itemset in itemsets:
        if len(itemset) == 1:
            formatted_itemsets.append(f"('{itemset[0]}'),")
        else:
            if prev_len != len(itemset):
                formatted_itemsets[-1] = formatted_itemsets[-1][:-1] + "\n\n"
            formatted_itemsets.append(f"{itemset},")
            prev_len = len(itemset)

    return "".join(formatted_itemsets).rstrip(",")




def get_permutes(combination_list):
    
    permutation_list = []
    if combination_list is not None and len(combination_list) > 0:
        size = len(combination_list[0])
        for i, pair1 in enumerate(combination_list[:-1]):
            for pair2 in combination_list[i + 1:]:
                if pair1[:-1] == pair2[:-1]:
                    new_pair = tuple(sorted(set(pair1).union(set(pair2))))
                    if all(combo in combination_list for combo in combinations(new_pair, size)):
                        permutation_list.append(new_pair)
                else:
                    break

    return permutation_list
    





def create_output_file(items_list, frequent_items_list, output_file_path):
    with open(output_file_path, 'w') as output_file:
        result = 'Candidates:\n' + format_itemsets(items_list) + '\n\n' \
                     + 'Frequent Items:\n' + format_itemsets(frequent_items_list)
        output_file.write(result)




def get_basket_rdd(data, case_number):
    if case_number == 1:
        basket_rdd = data.map(lambda line: (line.split(',')[0], line.split(',')[1])) \
            .groupByKey().map(lambda user_items: (user_items[0], sorted(list(set(list(user_items[1])))))) \
            .map(lambda item_users: item_users[1])
    elif case_number == 2:
        basket_rdd = data.map(lambda line: (line.split(',')[1], line.split(',')[0])) \
            .groupByKey().map(lambda item_users: (item_users[0], sorted(list(set(list(item_users[1])))))) \
            .map(lambda item_users: item_users[1])
    else:
        raise ValueError("Invalid case number")
    return basket_rdd

def get_candidate_itemset(basket_rdd, support_threshold):
    size = basket_rdd.count()
    candidate_itemset = basket_rdd.mapPartitions(
        lambda partition: PCY(
            baskets=partition,
            original_support=int(support_threshold),
            length=size)).flatMap(lambda pairs: pairs).distinct().sortBy(lambda pairs: (len(pairs), pairs)).collect()
    return candidate_itemset

def get_frequent_itemset(basket_rdd, candidate_itemset, support_threshold):
    frequent_itemset = basket_rdd.flatMap(
        lambda partition: get_frequents(
            baskets=partition,
            candidate_pairs=candidate_itemset)).flatMap(lambda pairs: pairs).reduceByKey(add).filter(lambda pair_count: pair_count[1] >= int(support_threshold)).map(lambda pair_count: pair_count[0]).sortBy(lambda pairs: (len(pairs), pairs)).collect()
    return frequent_itemset




if __name__ == '__main__':
    
    start = time.time()
    case_number = int(sys.argv[1])
    support_threshold = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]
    partitions = 2
    
    sc = SparkContext.getOrCreate()
    raw_rdd = sc.textFile(input_file_path, partitions)
    # skip the first row => csv header
    header = raw_rdd.first()
    data = raw_rdd.filter(lambda line: line != header)

    basket_rdd = get_basket_rdd(data, case_number)

    candidate_itemset = get_candidate_itemset(basket_rdd, support_threshold)
    frequent_itemset = get_frequent_itemset(basket_rdd, candidate_itemset, support_threshold)
    create_output_file(candidate_itemset, frequent_itemset, output_file_path)

    print("Duration: " + str((time.time() - start)) +" s")
