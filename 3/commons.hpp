#ifndef COMMONS_HPP
#define COMMONS_HPP

#include<bits/stdc++.h>
#include <mpi.h>
#include <pthread.h>
#include <chrono>
#include <mutex>
#include <unistd.h>
#include <atomic>

using namespace std;

#define CHUNK_SIZE 32
#define MD_SERVER_RANK 0
#define FAILOVER_INTERVAL 2000
#define HEART_BEAT_MESSAGE_TAG 72
#define NUM_REPLICATION 3

enum QueryType{
    UPLOAD,
    RETRIEVE,
    SEARCH,
    LIST_FILE,
    FAILOVER,
    RECOVER,
    EXIT,
    INVALID,
};

std::unordered_map<std::string, int> QueryMap{
    {"upload", UPLOAD},
    {"retrieve", RETRIEVE},
    {"search", SEARCH},
    {"list_file", LIST_FILE},
    {"failover", FAILOVER},
    {"recover", RECOVER},
    {"exit", EXIT}
};

void printFailure(){
    cout << -1 << endl;
}
void printSuccess(){
    cout << 1 << endl;
}

int getQueryType(string cmd){
    auto it = QueryMap.find(cmd);
    if (it != QueryMap.end()) {
        return it->second;
    } else {
        return INVALID;
    }
}

void printChunk(vector<char> &chunk){
    for(auto ch : chunk){
        cout << ch;
    }
}

void printChunkList(vector<vector<char>> &chunk_list){
    for(auto chunk : chunk_list){
        printChunk(chunk);
        cout << endl;
    }
}

int sendChunkId(int chunk_id, int DstRank){
    if(MPI_Send(&chunk_id, 1, MPI_INT, DstRank, DstRank, MPI_COMM_WORLD) != MPI_SUCCESS){
        return -1;
    }
    return 0;
}

int receiveChunkId(int &chunk_id, int SelfRank, int srcRank){
    MPI_Status status;
    if(MPI_Recv(&chunk_id, 1, MPI_INT, srcRank, SelfRank, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
        return -1;
    }
    return 0;
}

int sendQueryType(int QueryType, int DstRank){
    if(MPI_Send(&QueryType, 1, MPI_INT, DstRank, DstRank, MPI_COMM_WORLD) != MPI_SUCCESS){
        return -1;
    }
    return 0;
}

int receiveQueryType(int &QueryType, int SelfRank, int srcRank){
    MPI_Status status;
    if(MPI_Recv(&QueryType, 1, MPI_INT, srcRank, SelfRank, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
        return -1;
    }
    return 0;
}

int sendChunk(vector<char> &data, int DstRank){
    if(MPI_Send(data.data(), CHUNK_SIZE, MPI_CHAR, DstRank, DstRank, MPI_COMM_WORLD) != MPI_SUCCESS){
        return -1;
    }
    return 0;
}

int receiveChunk(vector<char> &data, int SelfRank, int srcRank){
    MPI_Status status;
    if(MPI_Recv(data.data(), CHUNK_SIZE, MPI_CHAR, srcRank, SelfRank, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
        return -1;
    }
    return 0;
}


#endif