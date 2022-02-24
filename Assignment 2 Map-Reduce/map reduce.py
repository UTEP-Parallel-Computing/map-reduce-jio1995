#Jiovani Hernandez
import time
import pymp

words = ["hate", "love", "death", "night", "sleep", "time", "henry", "hamlet", "you", "my", "blood", "poison", "macbeth", "king", "heart", "honest"]

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
  #global dictionary
  globalDict = pymp.shared.dict()
  
  #merge all files into one 
  merged = combineFiles(files)

  #count the times word appears, and put it in a dictionary
  with pymp.Parallel(threads) as i:
    for x in i.iterate(words):
      globalDict[x] = counts(x, merged)
      #return dictionary that has the count of words
  return globalDict

#main
def main():
  for i in [1,2,4,8]:
    start = time.time()
    mapReduce = countWords(i, words, files)
    end = time.time()
    total = end - start
    print('Threads:',i,'Time:',total)
  for word,times in mapReduce.items():
    print(f'Word count {word} : {times}')

if __name__ == "__main__":
    main()
