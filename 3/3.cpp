#include<bits/stdc++.h>
#include<mpi.h>

using namespace std;

#define CHUNK_SIZE 32

enum QueryType{
    UPLOAD,
    RETRIVE,
    SEARCH,
    LIST_FILE,
    FAILOVER,
    RECOVERY,
    EXIT,
};

int sendQueryType(int QueryType, int DstRank){
    if(MPI_Send(&QueryType, 1, MPI_INT, DstRank, DstRank, MPI_COMM_WORLD) != MPI_SUCCESS){
        return -1;
    }
    return 0;
}

int receiveQueryType(int &QueryType, int SelfRank){
    MPI_Status status;
    if(MPI_Recv(&QueryType, 1, MPI_INT, 0, SelfRank, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
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

int receiveChunk(vector<char> &data, int SelfRank){
    MPI_Status status;
    if(MPI_Recv(data.data(), CHUNK_SIZE, MPI_CHAR, 0, SelfRank, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
        return -1;
    }
    return 0;
}

int sendQuery(){
    return 0;
}

int receiveQuery(){
    return 0;
}

void divideFileIntoChunks(string &data, vector<vector<char>> &chunks_list){
    int total_size = data.size();
    for(int i = 0 ; i < total_size ; i += CHUNK_SIZE){
        int len = min(CHUNK_SIZE, total_size - i);
        vector<char> chunk(data.begin() + i, data.begin() + i + len);
        chunks_list.push_back(chunk);
    }
}

int readFile(string path, vector<vector<char>> &chunks_list){
    ifstream file(path);
    string line;
    if(!file.is_open()){
        return -1;
    }
    ostringstream buffer;
    buffer << file.rdbuf();
    if(file.fail() && !file.eof()){
        return -1;
    }
    string content = buffer.str();
    divideFileIntoChunks(content, chunks_list);
    return 0;
}

void printChunk(vector<char> &chunk){
    for(auto ch : chunk){
        cout << ch;
    }
}


int main(int argc, char** argv){
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if(rank == 0){
        while(true){
            int rank; cin >> rank;
            // int query; cin >> query;
            // int ret = sendQueryType(query, rank);
            // cout << ret << endl;
            // vector<char> chunk(32,'a');
            // sendChunk(chunk, rank);
            string path;
            cin >> path;
            vector<vector<char>> chunks_list;
            cout << readFile(path, chunks_list) << endl;
            // for(int i = 0 ; i < chunks_list.size() ; i++){
            //     cout << "CHUNK-" << i << endl;
            //     printChunk(chunks_list[i]);
            //     cout << endl;
            // }
            break;
        }
    }
    else{
        vector<vector<char>> chunks;
        while(true){
            // int query;
            // int ret = receiveQueryType(query, rank);
            // cout << ret << endl;
            // cout << "query received : " << query << endl;
            vector<char> chunk(32);
            receiveChunk(chunk, rank);
        }
    }

    MPI_Finalize();
    return 0;
}

/*
TO DO-
1. write allocate chunks to servers algorithm
2. write code to receive data from storage servers

-- MetaData server
    - stores list of struct 'Storage server'
    - struct storage server stores if that server is down or not, count of chunks it stores etc.
    - stores list of file Struct 
    - file Struct contains mapping of chunk_no -> (parent_server, idx in that servers chunk list)
*/