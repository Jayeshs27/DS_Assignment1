#ifndef STORAGE_SERVER_HPP
#define STORAGE_SERVER_HPP
#include "commons.hpp"

class StorageServer{
public:
    int server_id;
    atomic<bool> active_flag;
    atomic<bool> exit_flag;
    int chunk_cnt;
    vector<vector<char>> stored_data;
    StorageServer(int server_id){
        this->server_id = server_id;
        this->active_flag.store(true);
        this->exit_flag.store(false);
        this->chunk_cnt = 0;
    }
    // void confirmExit(){
    //     int qtype = EXIT;
    //     sendQueryType(qtype, MD_SERVER_RANK);
    // }
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
    void searchExactWord(int chunk_idx, string& target, vector<int> &positions, vector<int> &partialMatches) {
        string line(this->stored_data[chunk_idx].begin(), this->stored_data[chunk_idx].end());
        int pos = 0;
        int wordStart = 0;

        string firstWord(""), lastWord("");

        for (int i = 0; i < line.size(); ) {
            // Skip any separators (spaces, commas, periods, etc.)
            while (i < line.size() && !isalnum(line[i])) {
                ++i;
            }

            // Mark the start of the word
            int start = i;
            // Find the end of the current word
            while (i < line.size() && isalnum(line[i])) {
                ++i;
            }

            // Extract the word
            if (start < i) {
                string word = line.substr(start, i - start);

                // Save the first word encountered
                if (firstWord.empty()) {
                    firstWord = word;
                }

                // Check if the word matches the target
                if (word == target) {
                    positions.push_back(start);
                }

                // Update the last word
                lastWord = word;
            }
        }
        
        // cout << firstWord << " " << lastWord << endl;
        partialMatches.resize(2, -1);
        if(target.size() > firstWord.size() && isalnum(line[0])){
            // cout << string(target.end() - firstWord.size(), target.end()) << endl;
            if(equal(target.end() - firstWord.size(), target.end(), line.begin())){
                partialMatches[0] = firstWord.size();
            }
        }
        if(target.size() > lastWord.size() && isalnum(line[CHUNK_SIZE - 1])){
            // cout << string(target.begin(), target.begin() + lastWord.size()) << endl;
            if(equal(target.begin() , target.begin() + lastWord.size(), line.end() - lastWord.size())){
                partialMatches[1] = lastWord.size();
            }
        }
    }
    int handleSearchRequest(){
        vector<char> query;
        int target_size, chunk_id;
        MPI_Status status;
        if(receiveChunkId(chunk_id, this->server_id + 1, MD_SERVER_RANK) == -1){
            return -1;
        }
        if(MPI_Recv(&target_size, 1, MPI_INT, MD_SERVER_RANK, this->server_id + 1, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
            return -1;
        }
        query.resize(target_size);
        if(MPI_Recv(query.data(), target_size, MPI_CHAR, MD_SERVER_RANK, this->server_id + 1, MPI_COMM_WORLD, &status) != MPI_SUCCESS){
            return -1;
        }

        string target(query.begin(), query.end());
        vector<int> offsets, partialMatches;
        this->searchExactWord(chunk_id, target, offsets, partialMatches);

        int vsize = offsets.size();
        if(MPI_Send(&vsize, 1, MPI_INT, MD_SERVER_RANK, MD_SERVER_RANK, MPI_COMM_WORLD) != MPI_SUCCESS){
            return -1;
        }
        if(MPI_Send(offsets.data(), vsize, MPI_INT, MD_SERVER_RANK, MD_SERVER_RANK, MPI_COMM_WORLD) != MPI_SUCCESS){
            return -1;
        }
        if(MPI_Send(partialMatches.data(), 2, MPI_INT, MD_SERVER_RANK, MD_SERVER_RANK, MPI_COMM_WORLD) != MPI_SUCCESS){
            return -1;
        }
        return 0;
    }
    void findHalfMatches(int chunk_id, const string &target, vector<int> &front, vector<int> &back) {
        const string &line = string(this->stored_data[chunk_id].begin(), this->stored_data[chunk_id].end());
        int target_len = target.size();
        int line_len = line.size(); 

        for (int i = 1; i < target_len; ++i) {
            if (std::equal(target.begin() + i, target.end(), line.begin())) {
                front.push_back(target_len - i);
            }
        }

        for (int i = 1; i < target_len; ++i) {
            int line_start = line_len - (target_len - i);
            if (line_start < 0) break; // Prevent negative index

            if (std::equal(target.begin(), target.begin() + target_len - i, line.begin() + line_start)) {
                back.push_back(target_len - i);
            }
        }
    }
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
    static void* startSender(void* instance) {
        return ((StorageServer*)instance)->sendHeartBeat(instance);
    }
    // void waitIfDown(){
    //     while(!this->isActive() && !this->shouldExit()){
    //         std::this_thread::sleep_for(std::chrono::milliseconds(10));
    //     }
    // }
};


#endif