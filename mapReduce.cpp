// #include "mpi.h"
// #include <iostream>
// #include <math.h>
// #include <time.h>
// #include <stdio.h>
// #include <bits/stdc++.h>

// //Opening and reading directory
// #include "sys/types.h"
// #include "dirent.h"

// //Storing Index
// #include <unordered_map>
// #include <map>

// #include <string>
// #include <iterator>

// //For sprintf
// #include <string.h>

// //File IO
// #include <sstream>
// #include <fstream>

// //Can be used later for sort
// #include <algorithm>

// using namespace std;

// /************************************************************************************************************************************

// 1. Assume each process is mapped to one node.
// 2. Assuming the query can only be of the form -- give the list of documents containing the word in decreasing ordre of frequency.
//    If the query is of the form -- whether the document contains this word or not? - we have travese whole list of documnet ids 
//    associated with a word.
// 3. Map doc - docId

// 4 IMPLEMENT : EFFICIENT IO
 
// ************************************************************************************************************************************/


// #define MAX_FILE_NAME_SIZE 30
// #define LOWEST_ALPHABET_ASCII 97

// int main(int argc, char** argv)
// {

// 	int err;
// 	int processId,noOfProcesses;
// 	string currentFolder = ".";
// 	string parentFolder = "..";

// 	//Hard Coded Variable for the time being
// 	long int noOfFiles = 3;


// 	//Word Count Map for each document
//    	unordered_map<string, long int> wordCountMap;

//    	//Inverted Index Map for whole documents in the local node
//    	vector<unordered_map<string, multimap<long int, long int, greater <long int>>>> invertedIndexMap(20);

// 	//Parallelization starts
// 	err = MPI_Init(&argc, &argv);

// 	//Get Rank and Total no of processes
// 	err = MPI_Comm_rank(MPI_COMM_WORLD, &processId);
// 	err = MPI_Comm_size(MPI_COMM_WORLD, &noOfProcesses);

// 	//Mappers
// 	if(processId != 0)
// 	{
// 		char filename[MAX_FILE_NAME_SIZE];

// 		//There is a directory for each node
// 		char directoryName[MAX_FILE_NAME_SIZE];
// 		sprintf(directoryName,"%d/",processId);

// 		DIR *dp; 
// 		struct dirent *ep;    
// 		long int i=0;		 
// 		dp = opendir(directoryName);

// 		int partitionSize = 26/(noOfProcesses);
// 		int remainder = 26%(noOfProcesses);
		

// 		//array containing the partition index (0...(noOfProcesses)) for each alphabet
// 		int partitionIndex[26];

// 		int low,high;		
// 		for(int k=0;k<(noOfProcesses);k++)
// 		{
// 			low = partitionSize*k;
// 			high = partitionSize*(k+1) - 1;
// 			if(k == (noOfProcesses-2))
// 				high = high + remainder;

// 			for(int j=low;j<=high;j++)
// 			{
// 				partitionIndex[j] = k;
// 			}
// 		}

// 		//partition to which a given word belongs
// 		int correspondingPartition;

// 		if (dp != NULL)
// 		{
// 			//Start creating the local index in the node -- read each file one by one
// 			while ((ep = readdir(dp))!=NULL)
// 			{

// 				//Ignore the current and parent folders
// 				if(((string(ep->d_name)).compare(currentFolder))==0 || ((string(ep->d_name)).compare(parentFolder))==0)
// 				{
// 					continue;
// 				}

// 				//REVISIT IT
// 				long int documentID = (processId-1)*noOfFiles+i;

// 				//Start processing one document
// 				ifstream fp;

// 				//Current file read

// 				cout<< ep->d_name <<endl;
// 				sprintf(filename,"%d/%s",processId,ep->d_name);
// 				fp.open(filename);

// 	   			string readLine;


// 	   			//Reading the document
// 	   			while(!fp.eof())
// 	   			{
// 	   				//Reading document line by line 
// 	   			 	getline(fp, readLine);

// 	   			 	if(readLine.empty())
// 	   			 	{	 
// 	   			 		continue;
// 	   			 	}

// 	   			 	istringstream iss(readLine);
// 	   			 	string currentWord;

// 	   			 	while(iss >> currentWord)
// 	   			 	{

// 	   			 		//Remove special characters and check if its a stopword
// 	   			 		int len;
	   			 		
// 					    for (int j = 0, len = currentWord.size(); j < len; j++)
// 					    {
// 					        // check whether parsing character is punctuation or not
// 					        if (ispunct(currentWord[j]))
// 					        {
// 					            currentWord.erase(j--, 1);
// 					            len = currentWord.size();
// 					        }
// 					        else
// 					        {
// 					        	currentWord[j] = tolower(currentWord[j]);
// 					        }
// 					    }

// 					    if(currentWord.empty())
// 					    	continue;


	
// 					    //Check for stopword
					    



	   			 		
// 	   			 		//Update its frequency
// 			            wordCountMap[currentWord]++;
// 	   			 	}
// 	   			}

// 	   			//Closing the file
// 	   			fp.close();

// 	   			//When the document is processed and the file is closed

// 			    //Start adding the words from the word count map to the inverted index map
// 			    unordered_map<string, long int>::iterator itr;
// 			    for (itr= wordCountMap.begin(); itr != wordCountMap.end(); itr++)
// 			    {
// 		        	string currentWord = itr->first;
// 		        	int wordFreq = itr->second;
		        	
		        	
// 		        	correspondingPartition = partitionIndex[currentWord[0]-LOWEST_ALPHABET_ASCII];
// 		        	cout<<currentWord<<" : "<<wordFreq << endl;

// 		        	//If the currentWord doesn't exist in invertedIndexMap
// 		        	if(invertedIndexMap[correspondingPartition].find(currentWord) == invertedIndexMap[correspondingPartition].end())
// 		        	{

// 		        		multimap<long int, long int, greater <long int> > newMap;
// 		        		newMap.insert(pair<long int, long int>(wordFreq,documentID)); 

// 		        		invertedIndexMap[correspondingPartition][currentWord] = newMap;
// 		        	}

// 		        	//Otherwise make the document ID entry for the current word in its already existing map
// 		        	else
// 		        	{
// 		        		multimap<long int, long int, greater <long int> > documentsWithWord = invertedIndexMap[correspondingPartition][currentWord];
// 		        		documentsWithWord.insert(pair<long int, long int>(wordFreq,documentID));
// 		        		invertedIndexMap[correspondingPartition][currentWord] = documentsWithWord;
// 		        	}

		        	

// 		        	//Only for printing
// 		        	multimap<long int, long int, greater <long int> > documentsWithWord = invertedIndexMap[correspondingPartition][currentWord];
// 		        	multimap<long int, long int>::iterator itr;
				    
// 				    for (itr= documentsWithWord.begin(); itr != documentsWithWord.end(); itr++)
// 				    {
// 				    	cout<<" DOC ID: "<<itr->second<<"  ==>  FREQ: "<<itr->first<<endl;
// 				    }
	  				
// 		        }

// 		        cout<<"One Document over"<<endl;
		        
// 		        //Clearing the wordCountMap for processing new document
// 		        wordCountMap.clear();


// 		        //Now go to the next document in next iteration
// 		        i++;
// 			}
		    
// 		    //Close the directory
// 	      	closedir (dp);
//         }
    

// 		else
// 		{
// 		    perror("Couldn't open the directory");
// 		}



		
// 		//*******************************************ALL DOCUMENTS IN NODE ARE PROCESSESED****************************************//
		
		

// 		//Create Segement Files


// 	}

// 	//Let every process finish off the mapping phase
// 	err = MPI_Barrier(MPI_COMM_WORLD);

// 	//*****************************************************REDUCE PHASE BEGINS******************************************************//

// 	return 0;
// }

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

using namespace std;

/************************************************************************************************************************************

1. Assume each process is mapped to one node.
2. Assuming the query can only be of the form -- give the list of documents containing the word in decreasing ordre of frequency.
   If the query is of the form -- whether the document contains this word or not? - we have travese whole list of documnet ids 
   associated with a word.
3. Map doc - docId

4 IMPLEMENT : EFFICIENT IO
 
************************************************************************************************************************************/


#define MAX_FILE_NAME_SIZE 30
#define LOWEST_ALPHABET_ASCII 97

bool sortinrev(const pair<long int,long int> &a, const pair<long int,long int> &b)
{
       return (a.first > b.first);
}

vector<pair<long int, long int>> mergeVectors(vector<pair<long int, long int>> vec1,vector<pair<long int, long int>> vec2)
{
	vector<pair<long int, long int>> mergedVec(vec1.size()+vec2.size());
	merge(vec1.begin(),vec1.end(),vec2.begin(),vec2.end(),mergedVec.begin());
	vec1.clear();
	vec2.clear();

	return mergedVec;
}

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

   	//Inverted Index Map for whole documents in the local node
   	vector<unordered_map<string, vector<pair<long int,long int>>>> invertedIndexMap(20);

	//Parallelization starts
	err = MPI_Init(&argc, &argv);

	//Get Rank and Total no of processes
	err = MPI_Comm_rank(MPI_COMM_WORLD, &processId);
	err = MPI_Comm_size(MPI_COMM_WORLD, &noOfProcesses);

	//Mappers
	if(1)
	{
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
				long int documentID = (processId-1)*noOfFiles+i;

				//Start processing one document
				ifstream fp;

				//Current file read

				// cout<< ep->d_name <<endl;
				sprintf(filename,"%d/%s",processId,ep->d_name);
				fp.open(filename);

	   			string readLine;


	   			//Reading the document
	   			while(!fp.eof())
	   			{
	   				//Reading document line by line 
	   			 	getline(fp, readLine);

	   			 	if(readLine.empty())
	   			 	{	 
	   			 		continue;
	   			 	}

	   			 	istringstream iss(readLine);
	   			 	string currentWord;

	   			 	while(iss >> currentWord)
	   			 	{

	   			 		//Remove special characters and check if its a stopword
	   			 		int len;
	   			 		
					    for (int j = 0, len = currentWord.size(); j < len; j++)
					    {
					        // check whether parsing character is punctuation or not
					        if (ispunct(currentWord[j]))
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
					    



	   			 		
	   			 		//Update its frequency
			            wordCountMap[currentWord]++;
	   			 	}
	   			}

	   			//Closing the file
	   			fp.close();

	   			//When the document is processed and the file is closed

			    //Start adding the words from the word count map to the inverted index map
			    unordered_map<string, long int>::iterator itr;
			    for (itr= wordCountMap.begin(); itr != wordCountMap.end(); itr++)
			    {
		        	string currentWord = itr->first;
		        	long int wordFreq = itr->second;
		        	
		        	
		        	correspondingPartition = partitionIndex[currentWord[0]-LOWEST_ALPHABET_ASCII];
		        	// cout<<currentWord<<" : "<<wordFreq << endl;
		        	 
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
		        		vector<pair<long int,long int>> documentsWithWord = invertedIndexMap[correspondingPartition][currentWord];
		        		documentsWithWord.push_back(make_pair(wordFreq,documentID));
		        		invertedIndexMap[correspondingPartition][currentWord] = documentsWithWord;
		        	}

		        	

		        	//Only for printing
		        	vector<pair<long int,long int>> documentsWithWord = invertedIndexMap[correspondingPartition][currentWord];
		        	vector<pair<long int,long int>>::iterator itr;
				    
				    for (itr= documentsWithWord.begin(); itr != documentsWithWord.end(); itr++)
				    {
				    	cout << " DOC ID: " << itr->second << "  ==>  FREQ: " << itr->first << endl;
				    }
	  				
		        }

		        cout << "One Document over" << endl;
		        
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

		//sort the vectors containing word frequency along with document ID according to frequency for each word in invertedIndexMap
		unordered_map<string, vector<pair<long int,long int>>>::iterator mapItr;
		string wordString[noOfProcesses];
		vector<pair<long int,long int>> freqIDVec[noOfProcesses];
		vector<long int> indices[noOfProcesses];
		long int index = 0; 
		for(int k=0;k<(noOfProcesses);k++)
		{
			for(mapItr=invertedIndexMap[k].begin();mapItr!=invertedIndexMap[k].end();mapItr++)
			{
				sort((mapItr->second).begin(), (mapItr->second).end(), sortinrev);
				wordString[k].append(mapItr->first);
				// cout << processId << ":" << mapItr->first << endl;
				wordString[k].append(":");
				freqIDVec[k].insert(freqIDVec[k].begin(),(mapItr->second).begin(),(mapItr->second).end());
				indices[k].push_back(index);
				index += (mapItr->second).size();
			}
		}

		// cout << "0 " << processId << " " << wordString[0] << endl;
		// cout << "1 " << processId << " " << wordString[1] << endl;
		// cout << "2 " << processId << " " << wordString[2] << endl;
		
		//*******************************************ALL DOCUMENTS IN NODE ARE PROCESSESED****************************************//
		
		//send the invertedMapIndex vector to root process

	}

	//Let every process finish off the mapping phase
	err = MPI_Barrier(MPI_COMM_WORLD);

	//*****************************************************REDUCE PHASE BEGINS******************************************************//

	err = MPI_Finalize();
	return 0;
}