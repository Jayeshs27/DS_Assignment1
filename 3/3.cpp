#include<bits/stdc++.h>
#include<mpi.h>

using namespace std;


int main(int argc, char** argv){
    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if(rank == 0){
        while(true){
            int query; cin >> query;
            MPI_Send(&query, 1, MPI_INT, 1, 1, MPI_COMM_WORLD);
        }
    }
    else{
        while(true){
            int query;
            MPI_Status status;
            MPI_Recv(&query, 1, MPI_INT, 0, rank, MPI_COMM_WORLD, &status);
            cout << "query received : " << query << endl;
        }
    }
    return 0;
}