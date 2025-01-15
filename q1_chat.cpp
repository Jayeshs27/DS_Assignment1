#include <iostream>
#include <vector>
#include <queue>
#include <mpi.h>
#include <bits/stdc++.h>
using namespace std;
#define INF INT_MAX

void bfs_1d_partitioning(const std::vector<std::vector<int>>& adj, int n, int s) {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int vertices_per_process = n / size;
    int start = rank * vertices_per_process;
    int end = (rank == size - 1) ? n : start + vertices_per_process;

    std::vector<int> distance(n, INF);
    std::vector<int> local_distance(vertices_per_process, INF);
    std::vector<std::queue<int>> send_buffers(size);

    if (rank == s / vertices_per_process) {
        local_distance[s % vertices_per_process] = 0;
        send_buffers[s / vertices_per_process].push(s);
    }

    while (true) {
        std::vector<int> received_vertices;
        for (int i = 0; i < size; ++i) {
            if (i != rank) {
                int recv_size;
                MPI_Recv(&recv_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                for (int j = 0; j < recv_size; ++j) {
                    int vertex;
                    MPI_Recv(&vertex, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    received_vertices.push_back(vertex);
                }
            }
        }

        for (int u : received_vertices) {
            for (int v : adj[u]) {
                int owner = v / vertices_per_process;
                if (rank == owner) {
                    int local_v = v % vertices_per_process;
                    if (local_distance[local_v] == INF) {
                        local_distance[local_v] = local_distance[u % vertices_per_process] + 1;
                        send_buffers[owner].push(v);
                    }
                }
            }
        }

        bool no_more_work = true;
        for (int i = 0; i < size; ++i) {
            if (!send_buffers[i].empty()) {
                no_more_work = false;
                int send_size = send_buffers[i].size();
                MPI_Send(&send_size, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                while (!send_buffers[i].empty()) {
                    int vertex = send_buffers[i].front();
                    send_buffers[i].pop();
                    MPI_Send(&vertex, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                }
            } else {
                int zero = 0;
                MPI_Send(&zero, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
            }
        }

        bool global_no_more_work;
        MPI_Allreduce(&no_more_work, &global_no_more_work, 1, MPI_CXX_BOOL, MPI_LAND, MPI_COMM_WORLD);
        if (global_no_more_work) break;
    }

    MPI_Gather(local_distance.data(), vertices_per_process, MPI_INT, distance.data(), vertices_per_process, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        for (int i = 0; i < n; ++i) {
            std::cout << "Distance from " << s << " to " << i << " is " << distance[i] << std::endl;
        }
    }
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    int n = 10;  // Total number of vertices
    std::vector<std::vector<int>> adj(n);
    // Add edges to the adjacency list (example graph)
    adj[0] = {1, 2};
    adj[1] = {0, 3, 4};
    adj[2] = {0, 5, 6};
    adj[3] = {1};
    adj[4] = {1};
    adj[5] = {2};
    adj[6] = {2};
    // Add more edges as needed...

    int source_vertex = 0;
    bfs_1d_partitioning(adj, n, source_vertex);

    MPI_Finalize();
    return 0;
}
