#include "mpi.h"
#include <iostream>
#include <math.h>
#include <time.h>
#include <stdio.h>
#include <bits/stdc++.h>

//Opening and reading directory
#include "sys/types.h"
#include "dirent.h"

//Storing Index
#include <unordered_map>
#include <map>

#include <string>
#include <iterator>

//For sprintf
#include <string.h>

//File IO
#include <sstream>
#include <fstream>

//Can be used later for sort
#include <algorithm>

// cereal libraries
#include "cereal/types/map.hpp"
#include "cereal/types/vector.hpp"
#include "cereal/types/string.hpp"
#include "cereal/types/utility.hpp"
#include "cereal/archives/binary.hpp"


using namespace std;

/************************************************************************************************************************************

1. Assume each process is mapped to one node.
2. Assuming the query can only be of the form -- give the list of documents containing the word in decreasing ordre of frequency.
   If the query is of the form -- whether the document contains this word or not? - we have travese whole list of documnet ids 
   associated with a word.
3. IMPLEMENT : EFFICIENT IO
4. To compile: mpiCC -Iinclude filename -std=c++11
5. DOCID 
6. partition
7. document size

************************************************************************************************************************************/

#define MAX_FILE_NAME_SIZE 30
#define LOWEST_ALPHABET_ASCII 97
#define LONG_DIGIT_NO 10

bool sortinrev(const pair<long int,long int> &a, const pair<long int,long int> &b)
{
       return (a.first > b.first);
}

vector<pair<long int, long int>> mergeVectors(vector<pair<long int, long int>> vec1,vector<pair<long int, long int>> vec2)
{
	vector<pair<long int, long int>> mergedVec(vec1.size()+vec2.size());
	merge(vec1.begin(),vec1.end(),vec2.begin(),vec2.end(),mergedVec.begin(),sortinrev);
	vec1.clear();
	vec2.clear();

	return mergedVec;
}

/********************************************UTILITY FUNCTION FOR PRINTING LOCAL INDEX INTO FILES**************************************************************/

//Function to print vector of maps
void printMaps(vector<unordered_map<string, vector<pair<long int,long int>>>> receivedMaps, int noOfProcesses, int processId, string globalIndexFolder)
{
	unordered_map<string, vector<pair<long int,long int>>> final_map;
	unordered_map<string, vector<pair<long int,long int>>>::iterator mapItr;

	//Postings list for a word
	vector<pair<long int,long int>> documentsWithWord,mergedVector;

	//An entry for a map ---- word and its postings list
	string currentWord;
	vector<pair<long int , long int>> postingsForWord;

	char filename[MAX_FILE_NAME_SIZE];
	long int documentID, wordFreq;

	sprintf(filename,"%s/%d.txt",globalIndexFolder.c_str(),processId);

	FILE* fp = fopen(filename,"w");

	vector<pair<long int, long int>>::iterator itr;

	fprintf(fp,"\n--------------Index Begin--------------\n\n");

	for(int i=0;i<noOfProcesses;i++)
	{
		final_map = receivedMaps[i];

		fprintf(fp, "Partition %d ===> \n\n",i);

		//Iterate over FINAL MAP
		for(mapItr = final_map.begin(); mapItr!= final_map.end();mapItr++)
		{	

			//Get one entry from a map
			currentWord = mapItr->first;
			postingsForWord = mapItr->second;

			fprintf(fp,"%s:\n",currentWord.c_str());

			for(itr = postingsForWord.begin(); itr!=postingsForWord.end(); itr++)
			{
				documentID = itr->second;
				wordFreq = itr->first;
				fprintf(fp, "\t%ld : %ld \n", documentID, wordFreq);
			}	
		
		}
	}

	fprintf(fp,"\n\n--------------Index Over--------------\n\n");

	fclose(fp);
}



/***********************************************************************************************************************/



int main(int argc, char** argv)
{
	int err;
	int processId,noOfProcesses;
	string currentFolder = ".";
	string parentFolder = "..";

	//Hard Coded Variable for the time being
	long int noOfFiles = 3;

	//Word Count Map for each document
   	unordered_map<string, long int> wordCountMap;

	//Parallelization starts
	err = MPI_Init(&argc, &argv);

	//Get Rank and Total no of processes
	err = MPI_Comm_rank(MPI_COMM_WORLD, &processId);
	err = MPI_Comm_size(MPI_COMM_WORLD, &noOfProcesses);

   	//Inverted Index Map for whole documents in the local node
   	vector<unordered_map<string, vector<pair<long int,long int>>>> invertedIndexMap(noOfProcesses);

	///////////////////////////////////////////////////////////////////////////////////////////////////////////////

	//Load Stopwords into the set
	//https://algs4.cs.princeton.edu/35applications/stopwords.txt//
	unordered_set<string> stopwords;
	ifstream stopwordsStream;
	stopwordsStream.open("stopwords.txt");
	string stopWord;		

	while(!stopwordsStream.eof())
	{
	   	getline(stopwordsStream, stopWord);

	   	if(stopWord.empty())
	   	{
	   		continue;
	   	}	 

	   	if (stopWord[stopWord.size()-1]=='\n')
	   	{
	   		stopWord.erase(stopWord.size()-1, 1);
	   	}					        
	   	
	   	stopwords.insert(stopWord);
	}	
	
	/////////////////////////////////////////////////////////////////////////////////////////////////////////////// 		

	//Mappers
	char filename[MAX_FILE_NAME_SIZE];

	//There is a directory for each node
	char directoryName[MAX_FILE_NAME_SIZE];
	sprintf(directoryName,"%d/",processId);

	DIR *dp; 
	struct dirent *ep;    
	long int i=0;		 
	dp = opendir(directoryName);

	int partitionSize = 26/(noOfProcesses);
	int remainder = 26%(noOfProcesses);
	vector<pair<long int,long int>>::iterator vecItr;
	
	//array containing the partition index (0...(noOfProcesses)) for each alphabet
	int partitionIndex[26];

	int low,high;

	for(int k=0;k<(noOfProcesses);k++)
	{
		low = partitionSize*k;
		high = partitionSize*(k+1) - 1;
		if(k == (noOfProcesses-1))
			high = high + remainder;

		for(int j=low;j<=high;j++)
		{
			partitionIndex[j] = k;
		}
	}

	//partition to which a given word belongs
	int correspondingPartition;
	long int documentID, wordFreq;
	ifstream inputFile;
	string readLine;
	int len;	
 	string currentWord;
    unordered_map<string, long int>::iterator mapItr;
    vector<pair<long int,long int>> documentsWithWord,mergedVector;

	if (dp != NULL)
	{
		//Start creating the local index in the node -- read each file one by one
		while ((ep = readdir(dp))!=NULL)
		{

			//Ignore the current and parent folders
			if(((string(ep->d_name)).compare(currentFolder))==0 || ((string(ep->d_name)).compare(parentFolder))==0)
			{
				continue;
			}

			//REVISIT IT
			documentID = (processId-1)*noOfFiles+i;

			//Current file read

			sprintf(filename,"%d/%s",processId,ep->d_name);
			inputFile.open(filename);

   			//Reading the document
   			while(!inputFile.eof())
   			{
   				//Reading document line by line 
   			 	getline(inputFile, readLine);

   			 	if(readLine.empty())
   			 	{	 
   			 		continue;
   			 	}

   			 	istringstream iss(readLine);///////////////////////////////////////////////////////////

   			 	while(iss >> currentWord)
   			 	{
   			 		//Remove special characters and check if its a stopword
				    for (int j = 0, len = currentWord.size(); j < len; j++)
				    {
				        // check whether parsing character is punctuation or not
				        //if (ispunct(currentWord[j]))
				        if(!((currentWord[j]>='a' && currentWord[j]<='z')||(currentWord[j]>='A' && currentWord[j]<='Z')))
				        {
				            currentWord.erase(j--, 1);
				            len = currentWord.size();
				        }
				        else
				        {
				        	currentWord[j] = tolower(currentWord[j]);
				        }
				    }

				    if(currentWord.empty())
				    	continue;
				    
				    //Check for stopword
					if(stopwords.find(currentWord)!=stopwords.end())
					{
						//Ignore the stopword
						continue;
					}

   			 		//Update its frequency
		            wordCountMap[currentWord]++;
   			 	}
   			}

   			//Closing the file
   			inputFile.close();

   			//When the document is processed and the file is closed

		    //Start adding the words from the word count map to the inverted index map
		    for (mapItr= wordCountMap.begin(); mapItr != wordCountMap.end(); mapItr++)
		    {
	        	currentWord = mapItr->first;
	        	wordFreq = mapItr->second;

	        	correspondingPartition = partitionIndex[currentWord[0]-LOWEST_ALPHABET_ASCII];
				//printf("%d %d %c\n",correspondingPartition,processId,currentWord[0]);
	        	
	        	//If the currentWord doesn't exist in invertedIndexMap
	        	if(invertedIndexMap[correspondingPartition].find(currentWord) == invertedIndexMap[correspondingPartition].end())
	        	{
	        		vector<pair<long int,long int>> newVector;
	        		newVector.push_back(make_pair(wordFreq,documentID)); 
	        		invertedIndexMap[correspondingPartition][currentWord] = newVector;
	        	}
	        	//Otherwise make the document ID entry for the current word in its already existing map
	        	else
	        	{
	        		documentsWithWord = invertedIndexMap[correspondingPartition][currentWord];
	        		documentsWithWord.push_back(make_pair(wordFreq,documentID));
	        		invertedIndexMap[correspondingPartition][currentWord] = documentsWithWord;
	        	}

	        	//Only for printing
	        	documentsWithWord = invertedIndexMap[correspondingPartition][currentWord];
			    
			    for (vecItr = documentsWithWord.begin(); vecItr != documentsWithWord.end(); vecItr++)
			    {
			    	//cout << " DOC ID: " << vecItr->second << "  ==>  FREQ: " << vecItr->first << endl;
			    }
	        }

	        cout << "One Document over from Process: "<<filename<<" : " <<processId << endl;
	        
	        //Clearing the wordCountMap for processing new document
	        wordCountMap.clear();

	        //Now go to the next document in next iteration
	        i++;
		}
	    
	    //Close the directory
      	closedir (dp);
    }
	else
	{
	    perror("Couldn't open the directory");
	}

	unordered_map<string, vector<pair<long int,long int>>>::iterator mapItr1;
	for(int k=0;k<(noOfProcesses);k++)
	{
		for(mapItr1=(invertedIndexMap[k]).begin();mapItr1!=(invertedIndexMap[k]).end();mapItr1++)
		{
			//sort the vectors containing word frequency along with document ID according to frequency for each word in invertedIndexMap
			sort((mapItr1->second).begin(), (mapItr1->second).end(), sortinrev);
		}
	}

	//Only for debugging - i.e printing local index into the files
	printMaps(invertedIndexMap, noOfProcesses, processId, "LocalIndex");

	//*******************************************ALL DOCUMENTS IN NODE ARE PROCESSESED****************************************//
	
	//send/receive the invertedIndexMap vector to/from other process

	//***************** if documents size less ************************//
	if(0)
	{
		stringstream ss;
		cereal::BinaryOutputArchive outArchive(ss);

		string serializedMap[noOfProcesses];
		int serializedStringSizes[noOfProcesses];
		string combinedSerializedMapString;
		int sendDisp[noOfProcesses];
		int receiveDisp[noOfProcesses];
		int receiveSizes[noOfProcesses];
		int totalReceiveSize = 0;
		int previousStringSize = 0;

		for(int k=0;k<noOfProcesses;k++)
		{
			outArchive(invertedIndexMap[k]);
			serializedMap[k] = ss.str();
			serializedStringSizes[k] = serializedMap[k].size();	
			combinedSerializedMapString += serializedMap[k];
		
			sendDisp[k] = previousStringSize;
			previousStringSize += serializedStringSizes[k];
			ss.str("");
			ss.clear();
		}
		
		MPI_Alltoall(&(serializedStringSizes[0]), 1, MPI_INT, &(receiveSizes[0]), 1, MPI_INT, MPI_COMM_WORLD);

		previousStringSize = 0;
		for(int k=0;k<noOfProcesses;k++)
		{
			totalReceiveSize += receiveSizes[k];
			receiveDisp[k] = previousStringSize;
			previousStringSize += receiveSizes[k];	
		}
		
		string receivedSerializedMapString;
		receivedSerializedMapString.resize(totalReceiveSize);

		MPI_Alltoallv(&(combinedSerializedMapString[0]), &(serializedStringSizes[0]), &(sendDisp[0]), MPI_CHAR, &(receivedSerializedMapString[0]), &(receiveSizes[0]), &(receiveDisp[0]), MPI_CHAR, MPI_COMM_WORLD);

		previousStringSize = 0;
		stringstream newSS;
		cereal::BinaryInputArchive inArchive(newSS);	
		for(int k=0;k<noOfProcesses;k++)
		{
			newSS << receivedSerializedMapString.substr(previousStringSize,receiveSizes[k]);
			cereal::BinaryInputArchive inArchive(newSS);
			inArchive(invertedIndexMap[k]);
			previousStringSize += receiveSizes[k];
			newSS.str("");
			newSS.clear();
		}	
	}

	//**************** end ************************//

	//**************** large sized documents ************************//
	else
	{
		ofstream serializeFile;
		cereal::BinaryOutputArchive outArchive(serializeFile);
		
		for(int k=0;k<noOfProcesses;k++)
		{
			serializeFile.open(to_string(k)+to_string(processId), std::ofstream::out | std::ofstream::trunc);
			outArchive(invertedIndexMap[k]);
			serializeFile.close();
		}
		
		MPI_Barrier(MPI_COMM_WORLD);

		ifstream inputSerializeFile;
		cereal::BinaryInputArchive inArchive(inputSerializeFile);
		for(int k=0;k<noOfProcesses;k++)
		{
			inputSerializeFile.open(to_string(processId)+to_string(k));
			inArchive(invertedIndexMap[k]);
			inputSerializeFile.close();
		}
	}

	printMaps(invertedIndexMap, noOfProcesses, processId, "Merged");

	//**************** end ************************//
	
	//*****************************************************REDUCE PHASE BEGINS******************************************************//
	
	//New Implementation
	unordered_map<string, vector<pair<long int,long int>>> final_map, localMap;
	
	//Received Maps from AllGatherV
	//vector<unordered_map<string, vector<pair<long int,long int>>>> receivedMaps (noOfProcesses);

	vector<unordered_map<string, vector<pair<long int,long int>>>> receivedMaps = invertedIndexMap;
	
	//An entry for a map ---- word and its postings list
	vector<pair<long int , long int>> postingsForWord;


	//Iterate over all maps
	for(int k = 0; k<noOfProcesses;k++)
	{

		localMap = receivedMaps[k];

		//Iterate over one map
		for(mapItr1 = localMap.begin(); mapItr1!= localMap.end(); mapItr1++)
		{
			
			//Get one entry from a map
			currentWord = mapItr1->first;
			postingsForWord = mapItr1->second;

			//If it doesn't exists in the final map, make a new entry
			if(final_map.find(currentWord) == final_map.end())
			{
				final_map[currentWord] = postingsForWord;
			}

	       	//Otherwise merge the existing posting list with this one
	       	else
	       	{
	       		documentsWithWord = final_map[currentWord];
	       		mergedVector = mergeVectors(documentsWithWord, postingsForWord);
	       		final_map[currentWord] = mergedVector;
	       	}

		}	

	}

	/*****************************************************Final Map is Ready********************************************/

	//Now write it into file

	//Assuming the distributed global index files are in a folder and file name will be the process id

	string globalIndexFolder = "GlobalIndex";
	
	sprintf(filename,"%s/%d.txt",globalIndexFolder.c_str(),processId);

	FILE* fp = fopen(filename,"w");

	vector<pair<long int, long int>>::iterator itr;

	fprintf(fp,"\n--------------Index Begin--------------\n\n");

	//Iterate over FINAL MAP
	for(mapItr1 = final_map.begin(); mapItr1!= final_map.end();mapItr1++)
	{	
		//Get one entry from a map
		currentWord = mapItr1->first;
		postingsForWord = mapItr1->second;

		fprintf(fp,"%s:\n",currentWord.c_str());

		for(itr = postingsForWord.begin(); itr!=postingsForWord.end(); itr++)
		{
			documentID = itr->second;
			wordFreq = itr->first;
			fprintf(fp, "\t%ld : %ld \n", documentID, wordFreq);
		}	
	
	}

	fprintf(fp,"\n\n--------------Index Over--------------\n\n");

	fclose(fp);

	err = MPI_Finalize();
	return 0;
		
}

