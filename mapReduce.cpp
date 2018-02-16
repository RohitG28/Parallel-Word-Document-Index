#include "mpi.h"
#include <iostream>
#include <math.h>
#include <time.h>
#include <stdio.h>

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
   	unordered_map<string, multimap<long int, long int, greater <long int>>> invertedIndexMap;



	//Parallelization starts
	err = MPI_Init(&argc, &argv);


	//Get Rank and Total no of processes
	err = MPI_Comm_rank(MPI_COMM_WORLD, &processId);
	err = MPI_Comm_size(MPI_COMM_WORLD, &noOfProcesses);

	//Mappers
	if(processId != 0)
	{
		char filename[MAX_FILE_NAME_SIZE];

		//There is a directory for each node
		char directoryName[MAX_FILE_NAME_SIZE];
		sprintf(directoryName,"%d/",processId);

		DIR *dp; 
		struct dirent *ep;    
		long int i=0;		 
		dp = opendir(directoryName);


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

				cout<< ep->d_name <<endl;
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
		        	int wordFreq = itr->second;

		        	
		        	cout<<currentWord<<" : "<<wordFreq<<endl;
		        	

		        	//If the currentWord doesn't exist in invertedIndexMap
		        	if(invertedIndexMap.find(currentWord) == invertedIndexMap.end())
		        	{

		        		multimap<long int, long int, greater <long int> > newMap;
		        		newMap.insert(pair<long int, long int>(wordFreq,documentID)); 

		        		invertedIndexMap[currentWord] = newMap;
		        	}

		        	//Otherwise make the document ID entry for the current word in its already existing map
		        	else
		        	{
		        		multimap<long int, long int, greater <long int> > documentsWithWord = invertedIndexMap[currentWord];
		        		documentsWithWord.insert(pair<long int, long int>(wordFreq,documentID));
		        		invertedIndexMap[currentWord] = documentsWithWord;
		        	}

		        	

		        	//Only for printing
		        	multimap<long int, long int, greater <long int> > documentsWithWord = invertedIndexMap[currentWord];
		        	multimap<long int, long int>::iterator itr;
				    
				    for (itr= documentsWithWord.begin(); itr != documentsWithWord.end(); itr++)
				    {
				    	cout<<" DOC ID: "<<itr->second<<"  ==>  FREQ: "<<itr->first<<endl;
				    }
	  				
		        }

		        cout<<"One Document over"<<endl;
		        
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



		
		//*******************************************ALL DOCUMENTS IN NODE ARE PROCESSESED****************************************//
		
		

		//Create Segement Files


	}

	//Let every process finish off the mapping phase
	err = MPI_Barrier(MPI_COMM_WORLD);

	//*****************************************************REDUCE PHASE BEGINS******************************************************//

	return 0;
}