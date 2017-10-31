import sys
import time

# A heap class that can be initialized to work as a Max-heap or a min-heap
class Heap(object):
    def __init__(self, _isMaxHeap=1):
        self.__isMaxHeap = _isMaxHeap
        self.__heap = []

    # Checks if 0-based index is a leaf
    def __isLeaf(self, index):
        __heap_index = index + 1                # Convert to heap indexing
        __last_non_leaf_index = int(len(self.__heap)/2)
        return(__heap_index > __last_non_leaf_index)

    # Sift up function for Max Heap
    def __siftUpMax(self, index):
        __heap_index = index + 1                # Convert to heap indexing
        if __heap_index != 1:                   # Not root
            parent = int(__heap_index/2)
            parent -= 1                         # Convert to Python list indexing
            if self.__heap[index] > self.__heap[parent]:
                self.__heap[index], self.__heap[parent] = self.__heap[parent], self.__heap[index]
                self.__siftUpMax(parent)

    # Sift up function for Min Heap
    def __siftUpMin(self, index):
        __heap_index = index + 1                # Convert to heap indexing
        if __heap_index != 1:                   # Not root
            parent = int(__heap_index/2)
            parent -= 1                         # Convert to Python list indexing
            if self.__heap[index] < self.__heap[parent]:
                self.__heap[index], self.__heap[parent] = self.__heap[parent], self.__heap[index]
                self.__siftUpMin(parent)


    def insert(self, val):
        self.__heap.append(val)
        if self.__isMaxHeap:
            self.__siftUpMax(len(self.__heap)-1)
        else:
            self.__siftUpMin(len(self.__heap)-1)

    # Heapify for max heap
    def __heapifyMax(self, index):
        if not self.__isLeaf(index):
            __heap_index = index + 1            # Convert to heap indexing
            l = 2*__heap_index
            r = 2*__heap_index + 1

            l -= 1                              # Convert to Python list indexing
            r -= 1                              # Convert to Python list indexing

            largest = index
            if l < len(self.__heap) and self.__heap[largest] < self.__heap[l]:
                largest = l
            if r < len(self.__heap) and self.__heap[largest] < self.__heap[r]:
                largest = r

            if largest != index:
                self.__heap[index], self.__heap[largest] = self.__heap[largest], self.__heap[index]
                self.__heapifyMax(largest)

    # Heapify for min heap
    def __heapifyMin(self, index):
        if not self.__isLeaf(index):
            __heap_index = index + 1            # Convert to heap indexing
            l = 2*__heap_index
            r = 2*__heap_index + 1

            l -= 1                              # Convert to Python list indexing
            r -= 1                              # Convert to Python list indexing

            smallest = index
            if l < len(self.__heap) and self.__heap[smallest] > self.__heap[l]:
                smallest = l
            if r < len(self.__heap) and self.__heap[smallest] > self.__heap[r]:
                smallest = r

            if smallest != index:
                self.__heap[index], self.__heap[smallest] = self.__heap[smallest], self.__heap[index]
                self.__heapifyMin(smallest)

    def pop(self):
        if len(self.__heap) == 0:
            return None

        out = self.__heap[0]
        self.__heap[0] = self.__heap[len(self.__heap)-1]
        self.__heap.pop(len(self.__heap)-1)

        if len(self.__heap) != 0:
            if self.__isMaxHeap:
                self.__heapifyMax(0)
            else:
                self.__heapifyMin(0)

        return out

    def get_root_val(self):
        return self.__heap[0]

    def size(self):
        return len(self.__heap)

    def printheap(self):
        print(self.__heap)

class RunningMedianCounter(object):
    def __init__(self):
        self.__leftMaxHeap = Heap(True)
        self.__rightMinHeap = Heap(False)
        self.__total = 0      # Holds the current sum of all values in the 2 heaps

    def __balance_heaps(self):
        # If right min heap is larger than left max heap by more than 1
        if self.__rightMinHeap.size() - self.__leftMaxHeap.size() > 1:
            val = self.__rightMinHeap.pop()
            self.__leftMaxHeap.insert(val)
        elif self.__leftMaxHeap.size() - self.__rightMinHeap.size() > 1:
            val = self.__leftMaxHeap.pop()
            self.__rightMinHeap.insert(val)

        return

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

    def size(self):
        return self.__leftMaxHeap.size() + self.__rightMinHeap.size()

    def get_current_total(self):
        return self.__total

    def print_heaps(self):
        self.__leftMaxHeap.printheap()
        self.__rightMinHeap.printheap()


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

    def __get_running_total_for_zip(self, zip_key):
        assert zip_key in self.__zip_dict
        return self.__zip_dict[zip_key].get_current_total()

    def __get_transaction_count_for_zip(self, zip_key):
        assert zip_key in self.__zip_dict
        return self.__zip_dict[zip_key].size()

    # Check if date string conforms to MMDDYYYY format
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
            current_total_for_zip = self.__get_running_total_for_zip(zip_key)
            transaction_count_for_zip = self.__get_transaction_count_for_zip(zip_key)

            by_zip_output_line += str(running_median_for_zip) + "|"
            by_zip_output_line += str(transaction_count_for_zip) + "|"
            by_zip_output_line += str(current_total_for_zip)

#            print(by_zip_output_line, file=self.medians_by_zip_out)
            print(by_zip_output_line)

    def __process_record_for_date(self, cmte_id, zip, t_date, t_amt):
        # Ignore record for invalid dates
        if self.__is_valid_date(t_date):
            ## Convert date from MMDDYYYY to YYYYMMDD to enable chronological sorting
            MMDD = t_date[:4]
            YYYY = t_date[4:]
            t_date = YYYY + MMDD

            ## NOTE: this date_key is the cmte_id concatenated with the YYYYMMDD
            ## so that sorting this date_key in alphabetical order automatically
            ## sorts first by cmte_id and then chronologically by date
            date_key = cmte_id + '-' + t_date
            self.__insert_by_date(date_key, int(t_amt))

    def __generate_medianvals_by_date(self):
        for key in sorted(self.__date_dict):
            by_date_output_line = ""
            cmte_id, date = key.split('-')

            ## Convert date from YYYYMMDD to MMDDYYYY
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

            print(by_date_output_line)

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


            if other_id.strip() == '' and cmte_id != '' and transaction_amt != '':      # Individual contributors only and NULL checks
                self.__process_record_for_zip(cmte_id, zipcode, transaction_date, transaction_amt)
                self.__process_record_for_date(cmte_id, zipcode, transaction_date, transaction_amt)

        self.__generate_medianvals_by_date()




# Run code
if len(sys.argv) != 4:
    print("Format: python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt")
    exit(1)

processor = DataProcessor(sys.argv[1], sys.argv[2], sys.argv[3])
processor.process()
