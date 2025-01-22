#include<bits/stdc++.h>
#include<mpi.h>

using namespace std;

#define CHUNK_SIZE 32
#define MD_SERVER_RANK 0

void divideFileIntoChunks(string &data, vector<vector<char>> &chunks_list);
int sendChunk(vector<char> &data, int DstRank);
int receiveChunk(vector<char> &data, int SelfRank, int srcRank);
int sendChunkId(int chunk_id, int DstRank);
int receiveChunkId(int &chunk_id, int SelfRank, int srcRank);

int sendQueryType(int QueryType, int DstRank);
int receiveQueryType(int &QueryType, int SelfRank, int srcRank);

enum QueryType{
    UPLOAD,
    RETRIEVE,
    SEARCH,
    LIST_FILE,
    FAILOVER,
    RECOVERY,
    EXIT,
};


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

class StorageServer{
    public:
        int id;
        int chunks_cnt;
        bool is_down;
    StorageServer(int id){
        this->id = id; // id is rank - 1
        this->chunks_cnt = 0;
        this->is_down = false;
    }
};

class File{
public:
    string name;
    string path;
    int chunks_cnt;
    vector<pair<int,int>> storage_location;
    File(string name, string path){
        this->name = name;
        this->path = path;
        this->chunks_cnt = 0;
    }
    int readFile(vector<vector<char>> &chunks_list){
        ifstream file(this->path);
        string line;
        if(!file.is_open()){
            cerr << "Can't open file" << endl;
            return -1;
        }
        ostringstream buffer;
        buffer << file.rdbuf();
        if(file.fail() && !file.eof()){
            cerr << "fail to read" << endl;
            return -1;
        }
        string content = buffer.str();
        divideFileIntoChunks(content, chunks_list);
        this->chunks_cnt = chunks_list.size();
        return 0;
    }
    ~File();
};

class MetaDataServer{
    public:
        int num_storage_servers;
        vector<StorageServer*> storage_servers;
        vector<File*> files_list;
    
    MetaDataServer(int num_servers){
        this->num_storage_servers = num_servers;
        this->storage_servers.resize(num_servers);
        for(int i = 0 ; i < num_servers ; i++){
            this->storage_servers[i] = new StorageServer(i);
        }
    }
    void allocateStorageServers(File *file){
        file->storage_location.resize(file->chunks_cnt);
        int server_id = 0;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            int chunk_id = this->storage_servers[server_id]->chunks_cnt;
            file->storage_location[i] = make_pair(server_id, chunk_id);

            cout << "chunk-" << i << " allocated to rank-" << server_id + 1 << endl;

            server_id = (server_id + 1) % this->num_storage_servers;
        }
    }
    int sendFile(File* file, vector<vector<char>> &chunks_list){
        // Need to modify for failover of storage servers
        int server_id, chunk_id;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            tie(server_id, chunk_id) = file->storage_location[i];
            if(sendQueryType(0, server_id + 1) == -1){
                return -1;
            }
            if(sendChunk(chunks_list[i], server_id + 1) == -1){
                return -1;
            }
            cout << "sent chunk-" << i << " to rank-" << server_id + 1 << endl;
        }
        files_list.push_back(file);
        return 0;
    }
    File* searchFile(string filename){
        for(auto file : this->files_list){
            if(filename == file->name){
                return file;
            }
        }
        return NULL;
    }
    int retrieveFile(File* file, string &content){
        // need to modify for failover cases
        int qtype = 1, server_id, chunk_id;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            vector<char> chunk(32,'\0');
            tie(server_id, chunk_id) = file->storage_location[i];
            if(sendQueryType(qtype, server_id + 1) == -1){
                return -1;
            }
            if(sendChunkId(chunk_id, server_id + 1) == -1){
                return -1;
            }
            if(receiveChunk(chunk, MD_SERVER_RANK, server_id + 1) == -1){
                return -1;
            }
            cout << "recv chunk-" << i << " from rank-" << server_id + 1 << endl;
            content.append(chunk.begin(), chunk.end());
        }
        return 0;
    }
};

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

// int sendQuery(){
//     return 0;
// }

// int receiveQuery(){
//     return 0;
// }

void divideFileIntoChunks(string &data, vector<vector<char>> &chunks_list){
    int total_size = data.size();
    for(int i = 0 ; i < total_size ; i += CHUNK_SIZE){
        int len = min(CHUNK_SIZE, total_size - i);
        vector<char> chunk(data.begin() + i, data.begin() + i + len);
        chunks_list.push_back(chunk);
    }
}

void readInput(vector<string> &commands){
    string line, arg;
    getline(cin, line);
    stringstream strstream(line);
    while(strstream >> arg){
        commands.push_back(arg);
    }
}
void sendExitToAll(int size){
    for(int i = 1 ; i < size ; i++){
        int qtype = 6;
        sendQueryType(qtype, i);
    }
}

int main(int argc, char** argv){
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if(rank == MD_SERVER_RANK){
        MetaDataServer* md_server = new MetaDataServer(size - 1);
        // Note : can't run with size = 1, handle this case accordingly
        // - To Do ; Error handling for wrong input format
        while(true){
            // int rank; cin >> rank;
            // // int query; cin >> query;
            // // int ret = sendQueryType(query, rank);
            // // cout << ret << endl;
            // // vector<char> chunk(32,'a');
            // // sendChunk(chunk, rank);
            // string path;
            // cin >> path;
            vector<string> commands;
            readInput(commands);
            if(commands[0] == "upload"){
                File* file = new File(commands[1], commands[2]);
                vector<vector<char>> chunks_list;
                file->readFile(chunks_list);
                // printChunkList(chunks_list);
                md_server->allocateStorageServers(file);
                md_server->sendFile(file, chunks_list);
            }
            else if(commands[0] == "retrieve"){
                File* file;
                if((file = md_server->searchFile(commands[1])) == NULL){
                    cout << -1 << endl;
                }
                else{
                    string content="";
                    md_server->retrieveFile(file, content);
                    cout << content << endl;
                }
            }
            else if(commands[0] == "exit"){
                sendExitToAll(size);
                break;
            }
        }
    }
    else{
        vector<vector<char>> stored_data;
        // To Do - Error handling on this side(storage server)
        int chunk_cnt = 0;
        while(true){
            int qtype = -1;
            bool exitflag = false;
            int ret = receiveQueryType(qtype, rank, MD_SERVER_RANK);
            switch(qtype){
                case UPLOAD:{
                    vector<char> chunk(32);
                    receiveChunk(chunk, rank, MD_SERVER_RANK);
                    cout << "Rank - " << rank << " : chunk receieved" << endl;
                    stored_data.push_back(chunk);
                    chunk_cnt++;
                    break;
                }
                case RETRIEVE:{
                    int chunkId;
                    receiveChunkId(chunkId, rank, MD_SERVER_RANK);
                    sendChunk(stored_data[chunkId], MD_SERVER_RANK);
                    cout << "chunk-" << chunkId << " sent from rank-" << rank << endl;
                    break;
                }
                case EXIT:{
                    exitflag = true;
                    break;
                }
                default:
                    break;
            }
            if(exitflag)
                break;
        }
    }

    MPI_Finalize();
    return 0;
}

 // print chunks
// for(int i = 0 ; i < chunks_list.size() ; i++){
//     cout << "CHUNK-" << i << endl;
//     printChunk(chunks_list[i]);
//     cout << endl;
// }

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
