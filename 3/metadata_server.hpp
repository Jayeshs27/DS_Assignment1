#ifndef METADATA_SERVER_HPP
#define METADATA_SERVER_HPP

#include "commons.hpp"

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
        this->divideFileIntoChunks(content, chunks_list);
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

class StorageServerInfo{
public:
    int id;
    int chunks_cnt;
    atomic<bool> isDown;
    chrono::time_point<chrono::steady_clock> last_heartbeat;

    StorageServerInfo(int id){
        this->id = id; // id is rank - 1
        this->chunks_cnt = 0;
        this->isDown.store(false);
        this->last_heartbeat = chrono::steady_clock::now();
    }
    bool isServerDown(){
        return this->isDown.load();
    }
    void setDown(){
        this->isDown.store(true);
        return;
    }
    void setUp(){
        this->isDown.store(false);
        return;
    }
    ~StorageServerInfo();
};

class MetaDataServer{
    public:
        int num_storage_servers;
        int last_used_server;
        vector<StorageServerInfo*> storage_servers;
        vector<File*> files_list;
        mutex heartBeatMutex;
        atomic<bool> exit_flag;

        chrono::time_point<chrono::steady_clock> tmp_sent_time;

    MetaDataServer(int num_servers){
        this->exit_flag.store(false);
        this->num_storage_servers = num_servers;
        this->last_used_server = num_servers - 1;
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
        int server_id = (this->last_used_server + 1) % this->num_storage_servers;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            file->storage_location[i].resize(NUM_REPLICATION);
            for(int j = 0 ; j < NUM_REPLICATION ; j++){
                if(!this->storage_servers[server_id]->isServerDown()){
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
            if(server_id != -1 && !this->storage_servers[server_id]->isServerDown()){
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
            if(server_id != -1 && !this->storage_servers[server_id]->isServerDown()){
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
                    int qtype = UPLOAD;
                    if(sendQueryType(qtype, server_id + 1) == -1){
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
        int qtype = RETRIEVE, server_id, chunk_id, ret;
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

            if (i == file->chunks_cnt - 1) {
                content.append(chunk.begin(), find(chunk.begin(), chunk.end(), '\0'));
            } else {
                content.append(chunk.begin(), chunk.end());
            }
        }
        return 0;
    }
    int searchIntoFile(File* file, string target){
        vector<int> serversSeqs;
        int qtype = SEARCH, server_id, chunk_id, ret;
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            tie(server_id, chunk_id) = this->getAvailableLocation(file, i);
            assert(server_id != -1);
            if(sendQueryType(qtype, server_id + 1) == -1){
                return -1;
            }
            if(sendChunkId(chunk_id, server_id + 1) == -1){
                return -1;
            }
            if(sendSearchQuery(target, server_id + 1) == -1){
                return -1;
            }
            serversSeqs.push_back(server_id + 1);
        }

        vector<int> offsets, prevs(2, -1);
        for(int i = 0 ; i < file->chunks_cnt ; i++){
            vector<int> positions, partialMatches;
            if(this->receiveSearchResponse(serversSeqs[i], positions, partialMatches) == -1){
                return -1;
            }
            if(prevs[1] + partialMatches[0] == target.size()){
                offsets.push_back(i * CHUNK_SIZE - prevs[1]);
            }
            for(auto p : positions){
                // cout << p << " ";
                offsets.push_back(i * CHUNK_SIZE + p);
            }
            // cout << endl;
            prevs = partialMatches;
        }

        cout << offsets.size() << endl;
        for(auto p : offsets){
            cout << p << " ";
        }
        cout << endl;

        // ofstream output("out.txt");
        // if (!output.is_open()) {
        //     cerr << "Error: Unable to open file for writing." << endl;
        //     return -1;
        // }
        // output << offsets.size() << endl;
        // for(auto p : offsets){
        //     output << p << " ";
        // }
        // output << endl;
        // output.close();

        return 0;
    }
    int sendSearchQuery(string target, int DstRank){
        int target_size = target.size();
        if(MPI_Send(&target_size, 1, MPI_INT, DstRank, DstRank, MPI_COMM_WORLD) != MPI_SUCCESS){
            return -1;
        }
        if(MPI_Send(target.c_str(), target_size, MPI_CHAR, DstRank, DstRank, MPI_COMM_WORLD) != MPI_SUCCESS){
            return -1;
        }
        return 0;
    }
    int receiveSearchResponse(int DstRank, vector<int> &positions, vector<int> &partialMatches){
        int data_size;
        MPI_Status status; 
        if(MPI_Recv(&data_size, 1, MPI_INT, DstRank, MD_SERVER_RANK, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
            return -1;
        }
        positions.resize(data_size);
        if(MPI_Recv(positions.data(), data_size, MPI_INT, DstRank, MD_SERVER_RANK, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
            return -1;
        }

        partialMatches.resize(2);
        if(MPI_Recv(partialMatches.data(), 2, MPI_INT, DstRank, MD_SERVER_RANK, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
            return -1;
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
                    // lock_guard<mutex> lock(this->heartBeatMutex);
                    this->storage_servers[server_id]->last_heartbeat = chrono::steady_clock::now();
                    if (this->storage_servers[server_id]->isServerDown()) {
                        // cout << "ss-" << server_id + 1 << " is up" << endl;
                        this->storage_servers[server_id]->setUp();
                    }
                    break; 
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
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
            // int iter = 50 / this->num_storage_servers;
            // for (int i = 0; i < 5 ; ++i) {
            //     if (this->shouldExit()) {
            //         return nullptr;
            //     }
            //     std::this_thread::sleep_for(std::chrono::milliseconds(10));
            // }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));

            // lock_guard<mutex> lock(this->heartBeatMutex);
            for (int i = 0 ; i < this->num_storage_servers ; i++) {
                int time_passed = chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - this->storage_servers[i]->last_heartbeat).count();
                if (!this->storage_servers[i]->isServerDown() && time_passed > FAILOVER_INTERVAL) {
                    // cout << "rank " << i + 1 << " is down." << time_passed << endl;
                    // cout << chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - this->tmp_sent_time).count() << endl;
                    this->storage_servers[i]->setDown();
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
        if(this->storage_servers[rank - 1]->isServerDown()){
            int qtype = RECOVER;
            MPI_Send(&qtype, 1, MPI_INT, rank, rank, MPI_COMM_WORLD);
            // this->storage_servers[rank - 1]->setUp();
        }
        return 0;
    }
    int simulateFailover(int rank){
        if(!this->storage_servers[rank - 1]->isServerDown()){
            int qtype = FAILOVER;
            MPI_Send(&qtype, 1, MPI_INT, rank, rank, MPI_COMM_WORLD);
            // this->storage_servers[rank - 1]->setDown();
        }
        return 0;
    }
};

#endif