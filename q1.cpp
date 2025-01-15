#include<bits/stdc++.h>
#include<mpi.h>

#define INF 1e9
using namespace std;

void sendEdges(int rank, int size, int V, vector<vector<int>> &graph){
    for(int i = 0 ; i < V ; i++){
        int proc = i % size;
        if(proc != rank){
            int num_neighbours = graph[i].size();
            MPI_Status status;
            MPI_Send(&num_neighbours, 1, MPI_INT, proc, i,  MPI_COMM_WORLD);
            MPI_Send(graph[i].data(), num_neighbours, MPI_INT, proc, 2*i, MPI_COMM_WORLD);
        }
    }
}
void receiveEdges(int rank, int size, vector<vector<int>> &neighbours){
    int cnt = neighbours.size();
    int tag = rank;
    for(int i = 0 ; i < cnt ; i++){
        MPI_Status status;
        int num_neighbours;
        MPI_Recv(&num_neighbours, 1, MPI_INT, 0, tag,  MPI_COMM_WORLD, &status);
        neighbours[i].resize(num_neighbours);
        MPI_Recv(neighbours[i].data(), num_neighbours, MPI_INT, 0, tag * 2, MPI_COMM_WORLD, &status);
        tag += size;
    }
}

int num_nodes_assigned(int size, int rank, int V){
    int res = V / size;
    if(rank < V % size){
        res += 1;
    }
    return res;
}
int main(int argc, char* argv[]){
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int V, E;
    vector<vector<int>> graph;
    if(rank == 0){
        cin >> V >> E;
        graph.resize(V);
        for(int i = 0 ; i < E ; i++){
            int u, v, d;
            cin >> u >> v >> d;
            graph[v].push_back(u);
            if(d == 1) 
                graph[u].push_back(v);
        }
    }
    MPI_Bcast(&V, 1, MPI_INT, 0, MPI_COMM_WORLD);
    int nodes_cnt = num_nodes_assigned(size, rank, V);
    vector<vector<int>> neighbours;
    vector<int> dist(nodes_cnt, INF);
    if(rank == 0){
        neighbours.resize(nodes_cnt);
        int node_v = 0;
        for(int i = 0 ; i < nodes_cnt ; i++){
            neighbours[i].insert(neighbours[i].end(), graph[node_v].begin(), graph[node_v].end());
            // for(auto x : neighbours[i]){
            //     cout << x << " ";
            // }
            // cout << endl;
            node_v += size;
        }
        sendEdges(rank, size, V, graph);
    }
    else{
        neighbours.resize(nodes_cnt);
        receiveEdges(rank, size, neighbours);
    }
    queue<int> que;
    if(rank == 0){
        que.push(0);
        dist[0] = 0;
    }
    while(true){
        vector<vector<int>> next_layer(size);
        map<int,bool> inQue;
        // if(rank == 0){
        //     cout << rank << " : ";
        //     while(!que.empty()){
        //         int u = que.front();
        //         que.pop();
        //         cout << u << "-";
        //         int u_ind = (u - rank) / size;
        //         for(int i = 0 ; i < neighbours[u_ind].size() ; i++){
        //             int v = neighbours[u_ind][i];
        //             cout << v << " ";
        //             if(!inQue[v]){
        //                 next_layer[v % size].push_back(v);
        //                 next_layer[v % size].push_back(dist[u_ind] + 1);
        //                 inQue[v] = true;
        //             }
        //         }
        //     }
        // }
        // else{
        cout << rank << " : ";
        while(!que.empty()){
            int u = que.front();
            que.pop();
            cout << u << "-";
            int u_ind = (u - rank) / size;
            for(int i = 0 ; i < neighbours[u_ind].size() ; i++){
                int v = neighbours[u_ind][i];
                cout << v << " ";
                if(!inQue[v]){
                    next_layer[v % size].push_back(v);
                    next_layer[v % size].push_back(dist[u_ind] + 1);
                    inQue[v] = true;
                }
            }
        }
        // }
        cout << endl;
        // print loop
        // for(int i = 0 ; i < size ; i++){
        //     for(auto x : next_layer[i]){
        //         cout << x << " ";
        //     }
        //     cout << endl;
        // }

        vector<int> sendBuf, sendCnts(size), sendDispls(size);
        int displs = 0;
        for(int i = 0 ; i < size ; i++){
            sendBuf.insert(sendBuf.end(), next_layer[i].begin(), next_layer[i].end());
            sendCnts[i] = next_layer[i].size();
            sendDispls[i] = displs;
            displs += next_layer[i].size();
        }
        vector<int> recvBuf, recvCnts(size), recvDispls(size);
        MPI_Alltoall(sendCnts.data(), 1, MPI_INT, recvCnts.data(), 1, MPI_INT, MPI_COMM_WORLD);
        int sum = 0;
        for(int i = 0 ; i < size ; i++){
            recvDispls[i] = sum;
            sum += recvCnts[i];
        }
        recvBuf.resize(sum);
        MPI_Alltoallv(sendBuf.data(), sendCnts.data(), sendDispls.data(), MPI_INT, recvBuf.data(), \
         recvCnts.data(), recvDispls.data(), MPI_INT, MPI_COMM_WORLD);

        // working till here
        // cout << rank << " : " << endl;
        int isEmpty = 1;
        for(int i = 0 ; i < recvBuf.size() ; i += 2){
            int v_ind = (recvBuf[i] - rank) / size;
            if(dist[v_ind] == INF){
                dist[v_ind] = recvBuf[i + 1];
                que.push(recvBuf[i]);
                // cout << recvBuf[i] << " " << recvBuf[i + 1] << " | ";
                isEmpty = 0;
            }
        }
        // cout << endl;
        // cout << rank << endl;
        // for(int i = 0 ; i < size ; i++){
        //     cout << sendCnts[i] << " ";
        // }
        // cout << endl;
        // for(int i = 0 ; i < size ; i++){
        //     cout << recvCnts[i] << " ";
        // }
        // cout << endl;
        // for(auto t : recvBuf){
        //     cout << t << " ";
        // }
        // cout << endl;

        int isAllEmpty;
        MPI_Allreduce(&isEmpty, &isAllEmpty, 1, MPI_INT, MPI_BAND, MPI_COMM_WORLD);
        if(isAllEmpty){
            break;
        }

        // break;
    }


    // PRINT /////////
    // cout << "Rank-" << rank  << " " << nodes_cnt << endl;
    // int node_v = rank;
    // for(int i = 0 ; i < nodes_cnt ; i++){
    //     cout << node_v << " : ";
    //     for(auto node : neighbours[i]){
    //         cout << node << " ";
    //     }
    //     cout << endl;
    //     node_v += size;
    // }

    MPI_Finalize();
    return 0;
}