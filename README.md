# Table of Contents
1. [Approach](README.md#approach)
2. [Run Instructions](README.md#run-instruction)

# Approach
#### Running median using heaps
The most efficient way to compute a running median in my 

#### RunningMedianCounter data structure
The RunningMedianCounter class incorporates the approach described above in a data structure. The functions exposed by this data structure interface are:
- <b>insert(value)</b>: Insert a new value into the RunningMedianCounter
- <b>get_running_median()</b>: Compute the median of all values currently present
- <b>size()</b>: Get total number of values inserted into the RunningMedianCounter
- <b>get_current_total()</b>: Get total sum of all values inserted

As described, <b>insert()</b> works in linear time, the rest are constant time.

#### Input processing


# Run Instructions
Running is straightforward. Just run the top level run.sh script and output will be generated in the output/ folder.
No extra libraries/dependencies required.