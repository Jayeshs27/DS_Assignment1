#include<bits/stdc++.h>
#include<map>
#include<mpi.h>
using namespace std;

map<char, int> ENCODE_DIR = {
    {'R', 1},
    {'L', 2},
    {'U', 3},
    {'D', 4}
};

map<int, char> DECODE_DIR = {
    {1, 'R'},
    {2, 'L'},
    {3, 'U'},
    {4, 'D'}
};
map<char, char> REVERSE = {
    {'L', 'R'},
    {'R', 'L'},
    {'U', 'D'},
    {'D', 'U'}
};
map<char, char> CLOCKWISE = {
    {'L', 'U'},
    {'R', 'D'},
    {'U', 'R'},
    {'D', 'L'}
};

bool compare1(pair<vector<int>,char> a, pair<vector<int>,char> b){
     return a.first[1] < b.first[1];
}

bool compare2(pair<vector<int>,char> a, pair<vector<int>,char> b){
     return a.first[2] < b.first[2];
}

bool compare3(vector<int> a, vector<int> b){
     return a[0] < b[0];
}

vector<int> calculatePartitions(int rank, int size, int N, int M){
    vector<int> location(2);
    int len = N;
    if(M > N) len = M;
    int sz = len / size;
    if(rank == size - 1){
        sz += len % size;
    }
    location[0] = rank * (len / size);
    location[1] = location[0] + sz - 1;
    return location;
}

void updatePosition(vector<int> &balls_info, int i, int N, int M){
    int direct = balls_info[i + 3];
    if(direct == 1)
        balls_info[i + 2] = (balls_info[i + 2] + 1 + M) % M;
    else if(direct == 2)
        balls_info[i + 2] = (balls_info[i + 2] - 1 + M) % M;
    else if(direct == 3)
        balls_info[i + 1] = (balls_info[i + 1] - 1 + N) % N;
    else
        balls_info[i + 1] = (balls_info[i + 1] + 1 + N) % N;
}
bool exceedingBoundary(vector<int> &balls_info, int i, vector<int> &x_st_end, vector<int> &y_st_end){
    if(x_st_end[0] > balls_info[i + 1] || x_st_end[1] < balls_info[i + 1]){
        return true;
    }
    if(y_st_end[0] > balls_info[i + 2] || y_st_end[1] < balls_info[i + 2]){
        return true;
    }
    return false;
}
bool goingLeftORUp(vector<int> &balls_info, int i){
    if(balls_info[i + 3] == 2 || balls_info[i + 3] == 3){
        return true;
    }
    return false;
}
void updateGrid(vector<vector<int>> &grid, vector<int> &balls_info, int i, int xoffset, int yoffset){
    int x = balls_info[i + 1] - xoffset;
    int y = balls_info[i + 2] - yoffset;
    grid[x][y]++;
}

void updateDirection(vector<vector<int>> &grid, vector<int> &arr, int i, int xoffset, int yoffset){
    int x = arr[i + 1] - xoffset;
    int y = arr[i + 2] - yoffset;

    if(grid[x][y] == 2){
        arr[i + 3] = ENCODE_DIR[CLOCKWISE[DECODE_DIR[arr[i + 3]]]];
    }
    else if(grid[x][y] == 4){
        arr[i + 3] = ENCODE_DIR[REVERSE[DECODE_DIR[arr[i + 3]]]];
    }
}

void sendInitialPositions(int size, vector<vector<int>> &init_pos){
    for(int proc = 1 ; proc < size ; proc++){
        int tag = proc;
        int mesg_sz = init_pos[proc].size();
        MPI_Send(&mesg_sz, 1, MPI_INT, proc, tag, MPI_COMM_WORLD);
        MPI_Send(init_pos[proc].data(), mesg_sz, MPI_INT, proc, 2*tag, MPI_COMM_WORLD);
    }
}

void receiveInitialPositions(int rank, int size, vector<int> &balls_pos){
    MPI_Status status;
    int mesg_sz;
    MPI_Recv(&mesg_sz, 1, MPI_INT, 0, rank, MPI_COMM_WORLD, &status);
    balls_pos.resize(mesg_sz);
    MPI_Recv(balls_pos.data(), mesg_sz, MPI_INT, 0, rank*2, MPI_COMM_WORLD, &status);
}

int main(int argc, char* argv[]){
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    int N, M, K, T;
    vector<vector<int>> init_pos;
    if(rank == 0){
        cin >> N >> M >> K >> T;
        vector<pair<vector<int>,char>> balls(K,{vector<int>(3),' '});
        for(int i = 0 ; i < K ; i++){
            balls[i].first[0] = i + 1;
            cin >> balls[i].first[1] >> balls[i].first[2] >> balls[i].second;
        }
        int sort_dim = 1;
        auto comparator = compare1;
        if(M > N){
            sort_dim = 2;
            comparator = compare2;
        }
        init_pos.resize(size);
        sort(balls.begin(), balls.end(), comparator);
        int ptr = 0;
        for(int i = 0 ; i < size ; i++){
            auto dims = calculatePartitions(i, size, N, M);
            while(dims[0] <= dims[1] && ptr < K && balls[ptr].first[sort_dim] <= dims[1]){
                init_pos[i].push_back(balls[ptr].first[0]);
                init_pos[i].push_back(balls[ptr].first[1]);
                init_pos[i].push_back(balls[ptr].first[2]);
                init_pos[i].push_back(ENCODE_DIR[balls[ptr].second]);
                ptr++;
            }
        }
    }
    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&M, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&T, 1, MPI_INT, 0, MPI_COMM_WORLD);

    vector<int> balls_info;
    vector<int> x_st_end(2), y_st_end(2);
    if(rank == 0){
        sendInitialPositions(size, init_pos);
        balls_info.insert(balls_info.end(), init_pos[0].begin(), init_pos[0].end());
    }
    else{
        receiveInitialPositions(rank, size, balls_info);
    }
    if(N >= M){
        y_st_end[0] = 0;
        y_st_end[1] = M - 1;
        x_st_end = calculatePartitions(rank, size, N, M);
    }
    else{
        x_st_end[0] = 0;
        x_st_end[1] = N - 1;
        y_st_end = calculatePartitions(rank, size, N, M);
    }
    int xlen = x_st_end[1] - x_st_end[0] + 1;
    int ylen = y_st_end[1] - y_st_end[0] + 1;
    while(T > 0){
        vector<vector<int>> local_grid(xlen, vector<int>(ylen, 0));
        int len = balls_info.size();
        vector<bool> toKeep(len/4, false);
        vector<int> send_to_previous, send_to_next;
        for(int i = 0 ; i < len ; i += 4){
            updatePosition(balls_info, i, N, M);
            if(exceedingBoundary(balls_info, i, x_st_end, y_st_end)){
                if(goingLeftORUp(balls_info, i)){
                    send_to_previous.push_back(balls_info[i]);
                    send_to_previous.push_back(balls_info[i + 1]);
                    send_to_previous.push_back(balls_info[i + 2]);
                    send_to_previous.push_back(balls_info[i + 3]);
                }
                else{
                    send_to_next.push_back(balls_info[i]);
                    send_to_next.push_back(balls_info[i + 1]);
                    send_to_next.push_back(balls_info[i + 2]);
                    send_to_next.push_back(balls_info[i + 3]);
                }
            }
            else{
                updateGrid(local_grid, balls_info, i, x_st_end[0], y_st_end[0]);
                toKeep[i / 4] = true;
            }
        }
        vector<int> from_prev, from_next;
        int prev_proc = (rank - 1 + size) % size;
        int next_proc = (rank + 1 + size) % size;
        int recvCnt;
        MPI_Status status;
        if(rank % 2 == 1){
            int sendCnt = send_to_next.size();
            int tag = rank + next_proc;
            MPI_Sendrecv(&sendCnt, 1, MPI_INT, next_proc, tag, \
                &recvCnt, 1, MPI_INT, next_proc, tag, MPI_COMM_WORLD, &status);

            from_next.resize(recvCnt);
            MPI_Sendrecv(send_to_next.data(), sendCnt, MPI_INT, next_proc, 2 * tag, \
                from_next.data(), recvCnt, MPI_INT, next_proc, 2 * tag, MPI_COMM_WORLD, &status);

            sendCnt = send_to_previous.size();
            tag = rank + prev_proc;
            MPI_Sendrecv(&sendCnt, 1, MPI_INT, prev_proc, tag, \
                &recvCnt, 1, MPI_INT, prev_proc, tag, MPI_COMM_WORLD, &status);

            from_prev.resize(recvCnt);
            MPI_Sendrecv(send_to_previous.data(), sendCnt, MPI_INT, prev_proc, 2 * tag, \
                from_prev.data(), recvCnt, MPI_INT, prev_proc, 2 * tag, MPI_COMM_WORLD, &status);

        }
        else{
            int sendCnt = send_to_previous.size();
            int tag = rank + prev_proc;
            MPI_Sendrecv(&sendCnt, 1, MPI_INT, prev_proc, tag, \
                &recvCnt, 1, MPI_INT, prev_proc, tag, MPI_COMM_WORLD, &status);

            from_prev.resize(recvCnt);
            MPI_Sendrecv(send_to_previous.data(), sendCnt, MPI_INT, prev_proc, 2 * tag, \
                from_prev.data(), recvCnt, MPI_INT, prev_proc, 2 * tag, MPI_COMM_WORLD, &status);

            sendCnt = send_to_next.size();
            tag = rank + next_proc;
            MPI_Sendrecv(&sendCnt, 1, MPI_INT, next_proc, tag, \
                &recvCnt, 1, MPI_INT, next_proc, tag, MPI_COMM_WORLD, &status);

            from_next.resize(recvCnt);
            MPI_Sendrecv(send_to_next.data(), sendCnt, MPI_INT, next_proc, 2 * tag, \
                from_next.data(), recvCnt, MPI_INT, next_proc, 2 * tag, MPI_COMM_WORLD, &status);
        }
        for(int i = 0 ; i < from_prev.size() ; i += 4){
            updateGrid(local_grid, from_prev, i, x_st_end[0], y_st_end[0]);
        }
        for(int i = 0 ; i < from_next.size() ; i += 4){
            updateGrid(local_grid, from_next, i, x_st_end[0], y_st_end[0]);
        }
        for(int i = 0 ; i < from_prev.size() ; i += 4){
            updateDirection(local_grid, from_prev, i, x_st_end[0], y_st_end[0]);
        }
        for(int i = 0 ; i < from_next.size() ; i += 4){
            updateDirection(local_grid, from_next, i, x_st_end[0], y_st_end[0]);
        }
        vector<int> temp;
        temp.insert(temp.end(), balls_info.begin(), balls_info.end());
        balls_info.clear();
        for(int i = 0 ; i < temp.size() ; i += 4){
            if(toKeep[i/4]){
                updateDirection(local_grid, temp, i, x_st_end[0], y_st_end[0]);
                balls_info.push_back(temp[i]);
                balls_info.push_back(temp[i + 1]);
                balls_info.push_back(temp[i + 2]);
                balls_info.push_back(temp[i + 3]);
            }
        }
        balls_info.insert(balls_info.end(), from_prev.begin(), from_prev.end());
        balls_info.insert(balls_info.end(), from_next.begin(), from_next.end());
        T--;
    }
    int balls_cnt = balls_info.size();
    vector<int> recvCnts(size), recvDispls(size);
    MPI_Gather(&balls_cnt, 1, MPI_INT, recvCnts.data(), 1, MPI_INT, 0, MPI_COMM_WORLD);
    int totalsz = 0;
    for(int i = 0 ; i < size ; i++){
        recvDispls[i] = totalsz;
        totalsz += recvCnts[i];
    }
    vector<int> recvInfo(totalsz);
    MPI_Gatherv(balls_info.data(), balls_cnt, MPI_INT, recvInfo.data(), recvCnts.data(), recvDispls.data(), MPI_INT, 0, MPI_COMM_WORLD);
    if(rank == 0){
        vector<vector<int>> final_pos(K, vector<int>(4));
        for(int i = 0 ; i < K ; i++){
           for(int j = 0 ; j < 4 ; j++){
                final_pos[i][j] = recvInfo[i*4 + j];
           }
        }
        sort(final_pos.begin(), final_pos.end(), compare3);
        for(int i = 0 ; i < K ; i++){
            cout << final_pos[i][1] << " " << final_pos[i][2] << " " << DECODE_DIR[final_pos[i][3]] << endl;
        }
    }
    MPI_Finalize();
    return 0;
}
