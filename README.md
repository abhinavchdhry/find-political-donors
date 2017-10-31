# Table of Contents
1. [Approach](README.md#approach)
2. [Run Instructions](README.md#run-instruction)

# Approach
#### Running median using heaps
The algorithm to compute the running median uses a data structure consisting of 2 heaps: a min-heap which stores the smaller half of all values encountered and a max-heap which stores the larger half of all the values encountered, thus effectively bisecting the set of values at the middle.
In case the total number of values is even, each heap will contain the same number of values. If the total number of values is odd, the size of th heaps will differ by 1. This property is maintained in the data structure in the following way:

Consider S is the root of the min-heap and L is the root of the max-heap.
If a new value is greater than L, insert it into the max heap. Otherwise insert it into the min-heap.
After insertion, if the size of the heaps differ by more than 1, pop the root element from the larger heap and insert into the smaller heap.

Retrieving the median is a constant time operation:
* If both heaps are of the same size, then the median is (S + L)/2
* If the min-heap is larger, median is S
* If the max-heap is larger, median is L

Inserting a new value takes logarithmic time, since insertion into a heap has log complexity.

#### RunningMedianCounter data structure
The RunningMedianCounter class incorporates the approach described above in a data structure. The functions exposed by this data structure interface are:
- <b>insert(value)</b>: Insert a new value into the RunningMedianCounter
- <b>get_running_median()</b>: Compute the median of all values currently present
- <b>size()</b>: Get total number of values inserted into the RunningMedianCounter
- <b>get_current_total()</b>: Get total sum of all values inserted

As described, <b>insert()</b> works in log N time, the rest are constant time.

#### Implementation
Initialize 2 dictionaries (`zip_dict` and `date_dict`): 
* `zip_dict` is indexed by a combination of `cmte_id` and the 5-digit `zip_code` and stores a `RunningMedianCounter` instance which keeps track of the running median of `transaction_amt` for this combination of `cmte_id` and `zip_code`. For instance for following `CMTE_ID: C00629618, ZIP_CODE: 90017`, the key would be `C00629618-90017`
* `date_dict` is indexed by a combination of `cmte_id` and the YYYYMMDD format of the `transaction_date`. The value stored is again an instance of `RunningMedianCounter` which keeps track of the running median `transaction_amt` for that combination. For instance, for an input record having `CMTE_ID: C00629618, TRANSACTION_DT: 01032017`, the key would be `C00629618-20170103`.
The reason for converting the date from MMDDYYYY to YYYYMMDD is that it easier to chronologically sort by date.



# Run Instructions
Running is straightforward. Just run the top level run.sh script and output will be generated in the output/ folder.
No extra libraries/dependencies required.