import sys
import math

# A heap class that can be initialized to work as a Max-heap or a min-heap
class Heap(object):
    def __init__(self, _isMaxHeap=1):
        self.__isMaxHeap = _isMaxHeap
        self.__heap = []

    # Checks if 0-based index is a leaf
    def __isLeaf(self, index):
        depth = math.log(len(self.__heap), 2)
        return ((index+1) >= int(math.floor(depth)) and (index+1) < int(math.ceil(depth)) )

    # Sift up function for Max Heap
    def __siftUpMax(self, index):
        if index != 0:
            parent = int(index/2)
            if self.__heap[index] > self.__heap[parent]:
                self.__heap[index], self.__heap[parent] = self.__heap[parent], self.__heap[index]
                self.__siftUpMax(parent)

    # Sift up function for Min Heap
    def __siftUpMin(self, index):
        if index != 0:
            parent = int(index/2)
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
            l = 2*index
            r = 2*index + 1

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
            l = 2*index
            r = 2*index + 1

            smallest = index
            if l < len(self.__heap) and self.__heap[smallest] > self.__heap[l]:
                smallest = l
            if r < len(self.__heap) and self.__heap[smallest] > self.__heap[r]:
                smallest = r

            if smallest != index:
                self.__heap[index], self.__heap[smallest] = self.__heap[smallest], self.__heap[index]
                self.__heapifyMax(smallest)

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

    def size(self):
        return len(self.__heap)


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

    def __insert_by_date(self, date_key, transaction_amt):


    def process(self):
        if self.__inputfile is None:
            print("Error: No file assigned!")
            return

        f = open(self.__inputfile, 'r')
        for line in f:
            columns = line.strip().split('|')
            cmte_id = columns[self.CMTE_ID_INDEX]
            zipcode = columns[self.ZIP_CODE_INDEX]
            transaction_date = columns[self.TRANSACTION_DT_INDEX]
            transaction_amt = columns[self.TRANSACTION_AMT_INDEX]
            other_id = columns[self.OTHER_ID_INDEX]

            if other_id.strip() == '':      # Individual contributors only
#                print(cmte_id, zipcode, transaction_date, transaction_amt, other_id)
                zip_key = cmte_id + '-' + zipcode
                date_key = cmte_id + '-' + transaction_date
                self.__insert_by_zip(zip_key, transaction_amt)
                self.__insert_by_date(date_key, transaction_amt)

# Run code
if len(sys.argv) != 4:
    print("Format: python ./src/find_political_donors.py ./input/itcont.txt ./output/medianvals_by_zip.txt ./output/medianvals_by_date.txt")
    exit(1)

processor = DataProcessor(sys.argv[1], sys.argv[2], sys.argv[3])
processor.process()