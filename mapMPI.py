#Jiovani Hernandez
import time
from mpi4py import MPI
import pymp



def updateDict(mainDict, newDict):
  for key in mainDict:
    mainDict[key] += newDict[key]
  return mainDict
  


files = ["shakespeare1.txt", "shakespeare2.txt", "shakespeare3.txt", "shakespeare4.txt", "shakespeare5.txt", "shakespeare6.txt", "shakespeare7.txt", "shakespeare8.txt"]

#counts times a word is in list
def counts(word, wordList):
  count = 0
  for i in wordList:
    if word in i.lower():
      count += 1
  return count
  
#return list of words found in file
def readFiles(read):
  with open(read, 'r') as file:
    found = file.read().split()
  return found

#returns list of all words in the files
def combineFiles(files):
  wordsList = []
  for i in files:
    wordsList.extend(readFiles(i))
  return wordsList

#parallel
def countWords(threads, words, files):
    words = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet", "you", "my", "blood", "poison", "macbeth", "king", "heart", "honest"]
    globalDict = dict.fromkeys(words, 0)
    # get the world communicator
    comm = MPI.COMM_WORLD

    # get our rank (process #)
    rank = comm.Get_rank()

    # get the size of the communicator in # processes
    size = comm.Get_size()
    #global dictionary
  
    #merge all files into one 
    merged = combineFiles(files)

    #count the times word appears, and put it in a dictionary
    
        #for x in i.iterate(words):
        # globalDict[x] = counts(x, merged)
        #return dictionary that has the count of words
    #return globalDict
    if rank is 0:
        print('Thread 0 distributing')

        wordsPerThread = len(merged) / size

        # first setup thread 0s slice of the list
        localList = merged[:int(wordsPerThread)]

        for process in range(1, size):
            #start and end of slice we're sending
            startOfSlice = int( wordsPerThread * process )
            endOfSlice = int( wordsPerThread * (process + 1) )

            sliceToSend = merged[startOfSlice:endOfSlice]
            comm.send(sliceToSend, dest=process, tag=0)
    #everyone else receives that message
        for key_word in words:
            globalDict[key_word] = counts(key_word, localList)
        for process in range(1,size):
            new_dic = comm.recv(source = process, tag = 1)
            globalDict = updateDict(globalDict, new_dic)
        return globalDict
    else:
        # receive a message from thread 0 with tag of 0
        localList = comm.recv(source=0, tag=0)
        for key_word in words:
            globalDict[key_word] = counts(key_word, localList)
            comm.send(globalDict, dest = 0, tag = 1)

#main
def main():
    words = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet", "you", "my", "blood", "poison", "macbeth", "king", "heart", "honest"]
    for i in [1,2,4,8]:
        start = time.time()
        mapReduce = countWords(i, words, files)
        end = time.time()
        total = end - start
        print('Threads:',i,'Time:',total)
    for word in mapReduce:
        print(f'Word count {word} : {mapReduce[word]}')
  

if __name__ == "__main__":
    main()
