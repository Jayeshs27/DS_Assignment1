#include <mpi.h>
#include <pthread.h>
#include <iostream>
#include <chrono>
#include <map>
#include <mutex>
#include <unistd.h> // For usleep
#include <bits/stdc++.h>
using namespace std;

// Global variables for heartbeat tracking
map<int, chrono::time_point<chrono::steady_clock>> last_heartbeat;
vector<bool> isdown;
mutex heartbeat_mutex;
const int failover_interval = 5; // seconds

bool heartbeat_active = true; // Control variable for heartbeat

// Function for sending heartbeat
void* send_heartbeat(void* rank_ptr) {
    int rank = *((int*)rank_ptr);
    while (true) {
        if (heartbeat_active) {
            MPI_Send(&rank, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        }
        usleep(1000000); // Sleep for 1 second
    }
    return nullptr;
}

// Function for receiving heartbeat
void* receive_heartbeat(void*) {
    int rank;
    while (true) {
        MPI_Recv(&rank, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        lock_guard<mutex> lock(heartbeat_mutex);
        last_heartbeat[rank] = chrono::steady_clock::now();

        if(isdown[rank - 1]){
            cout << rank << " is up" << endl;
            isdown[rank - 1] = false;
        }
    }
    return nullptr;
}

// Function for monitoring heartbeats
void* monitor_heartbeats(void*) {
    while (true) {
        usleep(1000000); // Sleep for 1 second
        lock_guard<mutex> lock(heartbeat_mutex);
        for (auto it = last_heartbeat.begin(); it != last_heartbeat.end(); ++it) {
            int index = it->first - 1;
            if (!isdown[index] && chrono::duration_cast<chrono::seconds>(chrono::steady_clock::now() - it->second).count() > failover_interval) {
                cout << "Node " << it->first << " is down." << endl;
                isdown[index] = true;
                // Handle reallocation of chunks
            }
        }
    }
    return nullptr;
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);


    pthread_t heartbeat_thread, heartBeatMonitor;

    if (rank == 0) {
        // Metadata server: receive heartbeats
        isdown.resize(size, false);
        pthread_create(&heartbeat_thread, nullptr, receive_heartbeat, nullptr);
        pthread_create(&heartBeatMonitor, nullptr, monitor_heartbeats, nullptr);

        while(true){
            int rank; cin >> rank;
            int query; cin >> query;
            cout << "Rank-" << rank << ", query sent:" << query << endl;
            MPI_Send(&query, 1, MPI_INT, rank, rank, MPI_COMM_WORLD);
        }

    } else {
        // Other nodes: send heartbeats
        pthread_create(&heartbeat_thread, nullptr, send_heartbeat, &rank);
        while(true){
            int query;
            MPI_Status status;
            MPI_Recv(&query, 1, MPI_INT, 0, rank, MPI_COMM_WORLD, &status);
            cout << "Rank:" << rank << ", query received:" << query << endl;
            if(query == 0){
                heartbeat_active = true;
            }
            else if(query == 1){
                heartbeat_active = false;
            }
        }
    }
    if(rank == 0){
        pthread_join(heartBeatMonitor, nullptr);
    }

    pthread_join(heartbeat_thread, nullptr);

    MPI_Finalize();
    return 0;
}
