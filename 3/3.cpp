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
#define FAILOVER_INTERVAL 3
#define HEART_BEAT_MESSAGE_TAG 72
#define NUM_REPLICATION 3

void printFailure(){
    cout << -1 << endl;
}
void printSuccess(){
    cout << 1 << endl;
}

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

class StorageServerInfo{
public:
    int id;
    int chunks_cnt;
    bool isDown;
    chrono::time_point<chrono::steady_clock> last_heartbeat;

    StorageServerInfo(int id){
        this->id = id; // id is rank - 1
        this->chunks_cnt = 0;
        this->isDown = false;
        this->last_heartbeat = chrono::steady_clock::now();
    }
    ~StorageServerInfo();
};

class StorageServer{
public:
    int server_id;
    atomic<bool> active_flag;
    atomic<bool> exit_flag;
    StorageServer(int server_id){
        this->server_id = server_id;
        this->active_flag.store(true);
        this->exit_flag.store(false);
    }
    void confirmExit(){
        int qtype = EXIT;
        sendQueryType(qtype, MD_SERVER_RANK);
    }
    bool isActive(){
        return this->active_flag.load();
    }
    bool shouldExit(){
        return this->exit_flag.load();
    }
    void setExit(){
        this->exit_flag.store(true);
    }
    void deactivateServer(){
        this->active_flag.store(false);
    }
    void activateServer(){
        this->active_flag.store(true);
    }
};

class File{
public:
    string name;
    string path;
    int chunks_cnt;
    vector<vector<pair<int,int>>> storage_location;

    File(string name, string path){
        this->name = name;
        this->path = path;
        this->chunks_cnt = 0;
    }
    int readFile(vector<vector<char>> &chunks_list){
        ifstream file(this->path);
        string line;
        if(!file.is_open()){
            // cerr << "Can't open file" << endl;
            return -1;
        }
        ostringstream buffer;
        buffer << file.rdbuf();
        if(file.fail() && !file.eof()){
            // cerr << "fail to read" << endl;
            return -1;
        }
        string content = buffer.str();
        divideFileIntoChunks(content, chunks_list);
        this->chunks_cnt = chunks_list.size();
        return 0;
    }
    void allocateChunkInfo(){
        this->storage_location.resize(this->chunks_cnt);
        for(int i = 0 ; i < this->chunks_cnt ; i++){
            this->storage_location[i].resize(NUM_REPLICATION);
            for(int j = 0 ; j < NUM_REPLICATION ; j++){
                this->storage_location[i][j] = make_pair(-1,-1);
            }
        }
    }
    ~File();
};

class MetaDataServer{
    public:
        int num_storage_servers;
        vector<StorageServerInfo*> storage_servers;
        vector<File*> files_list;
        mutex heartBeatMutex;
        atomic<bool> exit_flag;

    MetaDataServer(int num_servers){
        this->exit_flag.store(false);
        this->num_storage_servers = num_servers;
        this->storage_servers.resize(num_servers);
        for(int i = 0 ; i < num_servers ; i++){
            this->storage_servers[i] = new StorageServerInfo(i);
        }
    }
    void setExit(){
        this->exit_flag.store(true);
    }
    bool shouldExit(){
        return this->exit_flag.load();
    }
    void closeStorageServers(){
        for(int i = 0 ; i < num_storage_servers ; i++){
            int qtype = EXIT;
            sendQueryType(qtype, i + 1);
        }
        // for(int i = 0 ; i < num_storage_servers ; i++){
        //     int qtype;
        //     receiveQueryType(qtype, MD_SERVER_RANK, i + 1);
        // }
    }
    void allocateStorageServers(File *file){
        // Need to modify for failover of storage servers
        file->allocateChunkInfo();
        int server_id = 0;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            file->storage_location[i].resize(NUM_REPLICATION);
            for(int j = 0 ; j < NUM_REPLICATION ; j++){
                if(!this->storage_servers[server_id]->isDown){
                    int chunk_id = this->storage_servers[server_id]->chunks_cnt;
                    this->storage_servers[server_id]->chunks_cnt += 1;
                    file->storage_location[i][j] = make_pair(server_id, chunk_id);
                    // cout << "chunk-" << i << " allocated to rank-" << server_id + 1 << endl;
                    server_id = (server_id + 1) % this->num_storage_servers;
                }
            }
        }
    }
    pair<int,int> getAvailableLocation(File* file, int chunk_idx){
        int server_id, chunk_id;
        for(int i = 0 ; i < NUM_REPLICATION ; i++){
            tie(server_id, chunk_id) = file->storage_location[chunk_idx][i];
            if(server_id != -1 && !this->storage_servers[server_id]->isDown){
                return file->storage_location[chunk_idx][i];
            }
        }
        return make_pair(-1,-1);
    }
    vector<int> getAllAvalLocation(File* file, int chunk_idx){
        priority_queue<int, vector<int>, greater<int>> locations;
        int server_id, chunk_id;
        for(int i = 0 ; i < NUM_REPLICATION ; i++){
            tie(server_id, chunk_id) = file->storage_location[chunk_idx][i];
            if(server_id != -1 && !this->storage_servers[server_id]->isDown){
                locations.push(server_id);
            }
        }
        vector<int> locations_sorted;
        while(!locations.empty()){
            locations_sorted.push_back(locations.top());
            locations.pop();
        }
        assert(locations_sorted.size() != 0);
        return locations_sorted;
    }
    bool checkAvailability(File* file){
        int server_id, chunk_id;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            tie(server_id, chunk_id) = getAvailableLocation(file, i);
            if(server_id == -1){
                return false;
            }
        }
        return true;
    }
    int sendFile(File* file, vector<vector<char>> &chunks_list){
        // Need to modify for failover of storage servers
        int server_id, chunk_id;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            for(int j = 0 ; j < NUM_REPLICATION ; j++){
                tie(server_id, chunk_id) = file->storage_location[i][j];
                if(server_id != -1){
                    if(sendQueryType(0, server_id + 1) == -1){
                        return -1;
                    }
                    if(sendChunk(chunks_list[i], server_id + 1) == -1){
                        return -1;
                    }
                    // cout << "sent chunk- " << i << " to rank-" << server_id + 1 << endl;
                }
            }
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
    void listFileLocations(File* file){
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            vector<int> locations = this->getAllAvalLocation(file, i);
            cout << i << " " << locations.size() << " ";
            for(auto server_id : locations){
                cout << server_id + 1 << " ";
            }
            cout << endl;
        }
    }
    int retrieveFile(File* file, string &content){
        // need to modify for failover cases
        int qtype = 1, server_id, chunk_id, ret;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            vector<char> chunk(32,'\0');
            tie(server_id, chunk_id) = this->getAvailableLocation(file, i);
            assert(server_id != -1);
            if(sendQueryType(qtype, server_id + 1) == -1){
                return -1;
            }
            if(sendChunkId(chunk_id, server_id + 1) == -1){
                return -1;
            }
            if(receiveChunk(chunk, MD_SERVER_RANK, server_id + 1) == -1){
                return -1;
            }
            // cout << "recv chunk-" << i << " from rank-" << server_id + 1 << endl;
            content.append(chunk.begin(), chunk.end());
        }
        return 0;
    }

    void* receiveHeartBeat(void*) {
        while (!this->shouldExit()) {
            int server_id;
            MPI_Request request;
            MPI_Irecv(&server_id, 1, MPI_INT, MPI_ANY_SOURCE, HEART_BEAT_MESSAGE_TAG, MPI_COMM_WORLD, &request);

            int flag = 0;
            while (!this->shouldExit()) {
                MPI_Test(&request, &flag, MPI_STATUS_IGNORE);
                if (flag) {
                    lock_guard<mutex> lock(this->heartBeatMutex);
                    this->storage_servers[server_id]->last_heartbeat = chrono::steady_clock::now();
                    if (this->storage_servers[server_id]->isDown) {
                        cout << "ss-" << server_id + 1 << " is up" << endl;
                        this->storage_servers[server_id]->isDown = false;
                    }
                    break; 
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            if (this->shouldExit()) {
                MPI_Cancel(&request);
                MPI_Request_free(&request);
            }
        }

        return nullptr;
    }

    void* monitorHeartBeat(void*) {
        while (!this->shouldExit()) {
            for (int i = 0; i < 50 ; ++i) {
                if (this->shouldExit()) {
                    return nullptr;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            lock_guard<mutex> lock(this->heartBeatMutex);
            for (int i = 0 ; i < this->num_storage_servers ; i++) {
                int time_passed = chrono::duration_cast<chrono::seconds>(chrono::steady_clock::now() - this->storage_servers[i]->last_heartbeat).count();
                if (!this->storage_servers[i]->isDown && time_passed > FAILOVER_INTERVAL) {
                    cout << "rank " << i + 1 << " is down." << endl;
                    this->storage_servers[i]->isDown = true;
                }
            }
        }
        return nullptr;
    }

    static void* startReceiver(void* instance) {
        return ((MetaDataServer*)instance)->receiveHeartBeat(nullptr);
    }
    static void* startMonitor(void* instance) {
        return ((MetaDataServer*)instance)->monitorHeartBeat(nullptr);
    }
    int simulateRecover(int rank){
        if(this->storage_servers[rank - 1]->isDown){
            int qtype = RECOVER;
            MPI_Send(&qtype, 1, MPI_INT, rank, rank, MPI_COMM_WORLD);
        }
        return 0;
    }
    int simulateFailover(int rank){
        if(!this->storage_servers[rank - 1]->isDown){
            int qtype = FAILOVER;
            MPI_Send(&qtype, 1, MPI_INT, rank, rank, MPI_COMM_WORLD);
        }
        return 0;
    }
};

void* sendHeartBeat(void* params) {
    StorageServer* status = (StorageServer*)params;
    while (!status->shouldExit()) {
        if (status->isActive()) {
            MPI_Send(&status->server_id, 1, MPI_INT, MD_SERVER_RANK, HEART_BEAT_MESSAGE_TAG, MPI_COMM_WORLD);
        }
        for (int i = 0; i < 50 ; ++i) {
            if (status->shouldExit()) {
                return nullptr;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }
    return nullptr;
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

void divideFileIntoChunks(string &data, vector<vector<char>> &chunks_list){
    int total_size = data.size();
    for(int i = 0 ; i < total_size ; i += CHUNK_SIZE){
        vector<char> chunk(CHUNK_SIZE,'\0');
        int len = min(CHUNK_SIZE, total_size - i);
        copy(data.c_str() + i, data.c_str() + i + len, chunk.begin());
        // chunk(data.begin() + i, data.begin() + i + len);
        chunks_list.push_back(chunk);
    }
}

void readInput(vector<string> &args){
    string line, arg;
    getline(cin, line);
    stringstream strstream(line);
    while(strstream >> arg){
        args.push_back(arg);
    }
}

int getQueryType(string cmd){
    auto it = QueryMap.find(cmd);
    if (it != QueryMap.end()) {
        return it->second;
    } else {
        return INVALID;
    }
}
int InitMPI(int &size, int &rank, int argc, char** argv){
    int provided;
    if(MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided) != MPI_SUCCESS){
        return -1;
    }
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    return 0;
}

int main(int argc, char** argv){

    int size, rank;
    if(InitMPI(size, rank, argc, argv) == -1){
        cout << "Error During MPI Initialization" << endl;
        return 0;
    }
    if(rank == MD_SERVER_RANK){
        MetaDataServer* md_server = new MetaDataServer(size - 1);
        pthread_t heartBeatReceiver, heartBeatMonitor;
        pthread_create(&heartBeatReceiver, nullptr, md_server->startReceiver, md_server);
        pthread_create(&heartBeatMonitor, nullptr, md_server->startMonitor, md_server);

        // Note : can't run with size = 1, handle this case accordingly
        // - To Do ; Error handling for wrong input format

        while(!md_server->shouldExit()){
            vector<string> args;
            readInput(args);
            switch(getQueryType(args[0])){
                case UPLOAD:{
                    File* file = new File(args[1], args[2]);
                    vector<vector<char>> chunks_list;
                    if(file->readFile(chunks_list) == -1){
                        printFailure();
                        break;
                    }
                    md_server->allocateStorageServers(file);
                    if(md_server->sendFile(file, chunks_list) == -1){
                        printFailure();
                        break;
                    }
                    printSuccess();
                    break;
                }
                case RETRIEVE:{
                    File* file;
                    if((file = md_server->searchFile(args[1])) == NULL){
                        printFailure();
                        break;
                    }
                    else if(!md_server->checkAvailability(file)){
                        printFailure();
                        break;
                    }
                    else{
                        string content="";
                        if(md_server->retrieveFile(file, content) == -1){
                            printFailure();
                            break;
                        }
                        // ofstream output("temp.txt");
                        // if (!output.is_open()) {
                        //     cerr << "Error: Unable to open file for writing." << endl;
                        //     return -1;
                        // }
                        // output << content;
                        // output.close();
                        cout << content << endl;
                    }
                    break;
                }
                case SEARCH:{
                    File* file;
                    if((file = md_server->searchFile(args[1])) == NULL){
                        printFailure();
                        break;
                    }
                    else if(!md_server->checkAvailability(file)){
                        printFailure();
                        break;
                    }
                    else{
                        // search
                    }
                    break;
                }
                case LIST_FILE:{
                    File* file;
                    if((file = md_server->searchFile(args[1])) == NULL){
                        printFailure();
                    }
                    else if(!md_server->checkAvailability(file)){
                        printFailure(); 
                    }
                    else{
                        md_server->listFileLocations(file);
                    }
                    break;
                }
                case FAILOVER:{
                    int ss_rank = stoi(args[1]);
                    if(ss_rank > 0 && ss_rank < size){
                        if(md_server->simulateFailover(ss_rank) == -1){
                            printFailure();
                            break;
                        }
                        printSuccess();
                    }
                    else{
                        printFailure();
                    }
                    break;
                }
                case RECOVER:{
                    int rank = stoi(args[1]);
                    if(rank > 0 && rank < size){
                        if(md_server->simulateRecover(rank) == -1){
                            printFailure();
                            break;
                        }
                        printSuccess();
                    }
                    else{
                        printFailure();
                    }
                    break;
                }
                case EXIT:{
                    md_server->closeStorageServers();
                    md_server->setExit();
                    break;
                }
                default:{
                    printFailure();
                    break;
                }
            }
        }
        pthread_join(heartBeatMonitor, nullptr);
        pthread_join(heartBeatReceiver, nullptr);
    }
    else{
        pthread_t heartBeatSender;
        StorageServer* storageServer = new StorageServer(rank - 1);
        pthread_create(&heartBeatSender, nullptr, sendHeartBeat, (void*)storageServer);
        vector<vector<char>> stored_data;
        // To Do - Error handling on this side(storage server)
        
        int chunk_cnt = 0;
        while(!storageServer->shouldExit()){
            int qtype = -1;
            bool exitflag = false;
            int ret = receiveQueryType(qtype, rank, MD_SERVER_RANK);
            switch(qtype){
                case UPLOAD:{
                    vector<char> chunk(32);
                    receiveChunk(chunk, rank, MD_SERVER_RANK);
                    // cout << "Rank - " << rank << " : chunk receieved" << endl;
                    stored_data.push_back(chunk);
                    chunk_cnt++;
                    break;
                }
                case RETRIEVE:{
                    int chunkId;
                    receiveChunkId(chunkId, rank, MD_SERVER_RANK);
                    sendChunk(stored_data[chunkId], MD_SERVER_RANK);
                    // cout << "chunk-" << chunkId << " sent from rank-" << rank << endl;
                    break;
                }
                case EXIT:{
                    storageServer->setExit();
                    break;
                }
                case FAILOVER:{
                    storageServer->deactivateServer();
                    break;
                }
                case RECOVER:{
                    storageServer->activateServer();
                    break;
                }
                default:
                    break;
            }
        }
        pthread_join(heartBeatSender, nullptr);
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

/*
Fork can be used to execute the instructions
mpiexec -np 12 --use-hwthread-cpus --oversubscribe ./a.out
 */
