import sys
import time

# A heap class that can be initialized to work as a Max-heap or a min-heap
class Heap(object):
    def __init__(self, _isMaxHeap=True):
        self.__isMaxHeap = _isMaxHeap
        self.__heap = []

    # Checks if 0-based index is a leaf
    def __isLeaf(self, index):
        __heap_index = index + 1                            # Convert to 1-based heap indexing
        __last_non_leaf_index = int(len(self.__heap)/2)     # Last non-leaf node is parent of last element
        return(__heap_index > __last_non_leaf_index)

    # A comparator function that evaulates x < y if min-heap, or x > y if max-heap
    def __compare(self, x, y):
        if self.__isMaxHeap:
            return x > y
        else:
            return x < y

    # Sift up
    def __siftUp(self, index):
        __heap_index = index + 1                            # Convert to 1-based heap indexing
        if __heap_index != 1:                               # Not root
            parent = int(__heap_index/2)
            parent -= 1                                     # Convert to Python list indexing
            if self.__compare(self.__heap[index], self.__heap[parent]):
                self.__heap[index], self.__heap[parent] = self.__heap[parent], self.__heap[index]
                self.__siftUp(parent)

    # Insert a new value into heap
    def insert(self, val):
        self.__heap.append(val)
        self.__siftUp(len(self.__heap)-1)

    # Heapify
    def __heapify(self, index):
        if not self.__isLeaf(index):
            __heap_index = index + 1                        # Convert to 1-based heap indexing
            l = 2*__heap_index
            r = 2*__heap_index + 1

            l -= 1                                          # Convert to Python list indexing
            r -= 1                                          # Convert to Python list indexing

            best = index
            if l < len(self.__heap) and self.__compare(self.__heap[l], self.__heap[best]):
                best = l
            if r < len(self.__heap) and self.__compare(self.__heap[r], self.__heap[best]):
                best = r

            if best != index:
                self.__heap[index], self.__heap[best] = self.__heap[best], self.__heap[index]
                self.__heapify(best)


    # Removes the value at root of the heap and outputs it
    def pop(self):
        if len(self.__heap) == 0:
            return None

        out = self.__heap[0]
        self.__heap[0] = self.__heap[len(self.__heap)-1]
        self.__heap.pop(len(self.__heap)-1)

        if len(self.__heap) != 0:
            self.__heapify(0)

        return out

    # Returns value at root without removing it
    def get_root_val(self):
        return self.__heap[0]

    # Returns total number of elements in the heap
    def size(self):
        return len(self.__heap)


# A data structure that returns the current running median in constant time
# Uses 2 heaps: a max heap and a min heap. The max heap holds the smaller half of the values inserted into the data
# structure. The min heap holds the larger half of the values inserted. The 2 heaps effectively partition the set of
# inserted values at the mid-point. The heaps are maintained in such a way that after every insertion, the size of
# the 2 heaps differ by at most 1. The data structure returns the current median of stored elements in constant time
# as follows:
# If the min-heap and the max-heap have equal sizes, then the root value of the min-heap and the root value of the
# max-heap are the 2 middle elements. The median is the average of the 2. If either heap is larger than the other
# (by 1), the root of the larger heap is the median.
class RunningMedianCounter(object):
    def __init__(self):
        self.__leftMaxHeap = Heap(True)
        self.__rightMinHeap = Heap(False)
        self.__total = 0      # Holds the current sum of all values in the 2 heaps

    # Balances the heaps if the difference of their sizes exceeds 1
    def __balance_heaps(self):
        # If right min heap is larger than left max heap by more than 1
        if self.__rightMinHeap.size() - self.__leftMaxHeap.size() > 1:
            val = self.__rightMinHeap.pop()
            self.__leftMaxHeap.insert(val)
        elif self.__leftMaxHeap.size() - self.__rightMinHeap.size() > 1:
            val = self.__leftMaxHeap.pop()
            self.__rightMinHeap.insert(val)

    # Insert a value into the data structure.
    # If both the heaps are empty, insert into the min-heap
    #
    def insert(self, value):
        if self.__leftMaxHeap.size() == 0 and self.__rightMinHeap.size() == 0:
            self.__leftMaxHeap.insert(value)
        elif self.__leftMaxHeap.size() == 0:
            if value > self.__rightMinHeap.get_root_val():
                self.__rightMinHeap.insert(value)
            else:
                self.__leftMaxHeap.insert(value)
        elif self.__rightMinHeap.size() == 0:
            if value < self.__leftMaxHeap.get_root_val():
                self.__leftMaxHeap.insert(value)
            else:
                self.__rightMinHeap.insert(value)
        else:
            if value < self.__rightMinHeap.get_root_val():
                self.__leftMaxHeap.insert(value)
            else:
                self.__rightMinHeap.insert(value)

        self.__total += value
        self.__balance_heaps()
        return

    # Returns the running median
    def get_running_median(self):
        assert abs(self.__leftMaxHeap.size() - self.__rightMinHeap.size()) <= 1
        if self.__leftMaxHeap.size() == 0 and self.__rightMinHeap.size() == 0:
            return None

        if self.__leftMaxHeap.size() == self.__rightMinHeap.size():
            return round((self.__leftMaxHeap.get_root_val() + self.__rightMinHeap.get_root_val())/2)
        elif self.__leftMaxHeap.size() > self.__rightMinHeap.size():
            return round(self.__leftMaxHeap.get_root_val())
        else:
            return round(self.__rightMinHeap.get_root_val())

    # Return the total number of elements in the 2 heaps
    def size(self):
        return self.__leftMaxHeap.size() + self.__rightMinHeap.size()

    # Returns the sum of all the elements stored in the 2 heaps
    def get_current_total(self):
        return self.__total



class DataProcessor(object):
    # 0 based indices for the required columns
    CMTE_ID_INDEX = 0
    ZIP_CODE_INDEX = 10
    TRANSACTION_DT_INDEX = 13
    TRANSACTION_AMT_INDEX = 14
    OTHER_ID_INDEX = 15

    def __init__(self, inputfile=None, medianvals_by_zip_file=None, medianvals_by_date_file=None):
        self.__inputfile = inputfile
        self.__medianvals_by_zip_file = medianvals_by_zip_file
        self.__medianvals_by_date_file = medianvals_by_date_file
        self.__zip_dict = {}
        self.__date_dict = {}

    def __insert_by_zip(self, zip_key, transaction_amt):
       if zip_key not in self.__zip_dict:
           self.__zip_dict[zip_key] = RunningMedianCounter()

       self.__zip_dict[zip_key].insert(transaction_amt)
       return self.__zip_dict[zip_key].get_running_median()

    def __insert_by_date(self, date_key, transaction_amt):
        if date_key not in self.__date_dict:
            self.__date_dict[date_key] = RunningMedianCounter()

        self.__date_dict[date_key].insert(transaction_amt)
        return self.__date_dict[date_key].get_running_median()

    # Check if date string conforms to MMDDYYYY format and is valid
    def __is_valid_date(self, date):
        try:
            time.strptime(date, '%m%d%Y')
        except ValueError:
            return False
        return True

    def __is_valid_zip(self, zip):
        return len(zip) == 5

    def __process_record_for_zip(self, cmte_id, zip, t_date, t_amt):
        # Ignore record for invalid zip codes
        if self.__is_valid_zip(zip):
            by_zip_output_line = ""
            by_zip_output_line += cmte_id + "|"
            by_zip_output_line += zip + "|"

            zip_key = cmte_id + '-' + zip

            running_median_for_zip = self.__insert_by_zip(zip_key, int(t_amt))
            current_total_for_zip = self.__zip_dict[zip_key].get_current_total()
            transaction_count_for_zip = self.__zip_dict[zip_key].size()

            by_zip_output_line += str(running_median_for_zip) + "|"
            by_zip_output_line += str(transaction_count_for_zip) + "|"
            by_zip_output_line += str(current_total_for_zip)

            print(by_zip_output_line, file=self.medians_by_zip_out)

    def __process_record_for_date(self, cmte_id, zip, t_date, t_amt):
        # Ignore record for invalid dates
        if self.__is_valid_date(t_date):
            # Convert date from MMDDYYYY to YYYYMMDD to enable chronological sorting
            MMDD = t_date[:4]
            YYYY = t_date[4:]
            t_date = YYYY + MMDD

            # NOTE: this date_key is the cmte_id concatenated with the YYYYMMDD so that sorting this date_key in
            # alphabetical order automatically sorts first by cmte_id and then chronologically by date
            date_key = cmte_id + '-' + t_date
            self.__insert_by_date(date_key, int(t_amt))

    def __generate_medianvals_by_date(self):
        for key in sorted(self.__date_dict):
            by_date_output_line = ""
            cmte_id, date = key.split('-')

            # Convert date from YYYYMMDD to MMDDYYYY
            MMDD = date[4:]
            YYYY = date[:4]
            date = MMDD + YYYY

            by_date_output_line += cmte_id + "|"
            by_date_output_line += date + "|"

            running_median_for_date = self.__date_dict[key].get_running_median()
            current_total_for_date = self.__date_dict[key].get_current_total()
            transaction_count_for_date = self.__date_dict[key].size()

            by_date_output_line += str(running_median_for_date) + "|"
            by_date_output_line += str(transaction_count_for_date) + "|"
            by_date_output_line += str(current_total_for_date)

            print(by_date_output_line, file = self.medians_by_date_out)

    def process(self):
        if self.__inputfile is None:
            print("Error: No file assigned!")
            return

        f = open(self.__inputfile, 'r')
        self.medians_by_zip_out = open(self.__medianvals_by_zip_file, 'w')
        self.medians_by_date_out = open(self.__medianvals_by_date_file, 'w')

        for line in f:
            columns = line.strip().split('|')
            cmte_id = columns[self.CMTE_ID_INDEX]
            zipcode = columns[self.ZIP_CODE_INDEX][:5]
            transaction_date = columns[self.TRANSACTION_DT_INDEX]
            transaction_amt = columns[self.TRANSACTION_AMT_INDEX]
            other_id = columns[self.OTHER_ID_INDEX]

            # Process records for individual contributors only and non-NULL cmte_id and transaction_amt
            if other_id.strip() == '' and cmte_id != '' and transaction_amt != '':
                self.__process_record_for_zip(cmte_id, zipcode, transaction_date, transaction_amt)
                self.__process_record_for_date(cmte_id, zipcode, transaction_date, transaction_amt)

        self.__generate_medianvals_by_date()
        self.medians_by_zip_out.close()
        self.medians_by_date_out.close()


if __name__ == "__main__":
    # Run code
    if len(sys.argv) != 4:
        print("Format: python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt")
        exit(1)

    processor = DataProcessor(sys.argv[1], sys.argv[2], sys.argv[3])
    processor.process()
