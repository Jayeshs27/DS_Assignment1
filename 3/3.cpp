#include "metadata_server.hpp"
#include "storager_server.hpp"

void readInput(vector<string> &args){
    string line, arg;
    getline(cin, line);
    stringstream strstream(line);
    while(strstream >> arg){
        args.push_back(arg);
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

        while(!md_server->shouldExit()){
            vector<string> args;
            readInput(args);
            switch(getQueryType(args[0])){
                case UPLOAD:{
                    if(args.size() != 3){
                        printFailure();
                        break;
                    }
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
                    md_server->listFileLocations(file);
                    break;
                }
                case RETRIEVE:{
                    if(args.size() != 2){
                        printFailure();
                        break;
                    }
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
                        // ofstream output("out.txt");
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
                    if(args.size() != 3){
                        printFailure();
                        break;
                    }
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
                        md_server->searchIntoFile(file, args[2]);
                    }
                    break;
                }
                case LIST_FILE:{
                    if(args.size() != 2){
                        printFailure();
                        break;
                    }
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
                    if(args.size() != 2){
                        printFailure();
                        break;
                    }
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
                    if(args.size() != 2){
                        printFailure();
                        break;
                    }
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
        pthread_create(&heartBeatSender, nullptr, storageServer->startSender, (void*)storageServer);

        while(!storageServer->shouldExit()){

            int qtype = -1;
            receiveQueryType(qtype, rank, MD_SERVER_RANK);
            switch(qtype){
                case UPLOAD:{
                    vector<char> chunk(32,'\0');
                    receiveChunk(chunk, rank, MD_SERVER_RANK);
                    // cout << "Rank - " << rank << " : chunk receieved" << endl;
                    storageServer->stored_data.push_back(chunk);
                    storageServer->chunk_cnt += 1;
                    break;
                }
                case RETRIEVE:{
                    int chunkId;
                    receiveChunkId(chunkId, rank, MD_SERVER_RANK);
                    sendChunk(storageServer->stored_data[chunkId], MD_SERVER_RANK);
                    // cout << "chunk-" << chunkId << " sent from rank-" << rank << endl;
                    break;
                }
                case SEARCH:{
                    storageServer->handleSearchRequest();
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
