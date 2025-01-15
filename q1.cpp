#include<bits/stdc++.h>
#include<mpi.h>

using namespace std;

int main(int argc, char* argv[]){
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int V;
    vector<int> arr;
    if(rank == 0){
        cin >> V;
        for(int i = 0 ; i < V ; i++){
            arr.push_back(i);
        }
    }
    MPI_Bcast(&V, 1, MPI_INT, 0, MPI_COMM_WORLD);
    
    int local_size = V / size;
    vector<int> sendCnt, displs, local_data;
    int disp = 0;
    for(int i = 0 ; i < size ; i++){
        displs.push_back(disp);
        disp += local_size;
        if(i == size - 1){
            sendCnt.push_back(local_size + V % size);
        }
        else{
            sendCnt.push_back(local_size);
        }
    }

    // int* local_data = (int*)malloc(sizeof(int) * sendCnt[rank]);
    // int* local_data = new int(sendCnt[rank]);
    local_data.resize(sendCnt[rank]);
    MPI_Scatterv(arr.data(), sendCnt.data(), displs.data(), MPI_INT, local_data.data(), sendCnt[rank], MPI_INT, 0, MPI_COMM_WORLD);

    cout << "Rank : " << rank << endl;
    for(int i = 0 ; i < sendCnt[rank] ; i++){
        cout << local_data[i] << " ";
    }
    cout << endl;
    
    MPI_Finalize();
    return 0;
}