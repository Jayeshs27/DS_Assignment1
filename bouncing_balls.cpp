#include<bits/stdc++.h>
using namespace std;

vector<vector<int>> grid;

class Ball{
    public:
        pair<int,int> pos;
        char direction;

        Ball(pair<int,int> pos, char direction){
            this->pos = pos;
            this->direction = direction;
        }
        void updatePosition(int gridN, int gridM){
            if(direction == 'R'){
                this->pos.first = (this->pos.first + 1) % gridN;
                this->pos.second = (this->pos.second + 1) % gridM;
            }
            else if(direction == 'L'){
                this->pos.first = (this->pos.first + 1) % gridN;
                this->pos.second = (this->pos.second + 1) % gridM;
            }
        }
        bool isCollide();
        void changeDirection();
};

int main(){
    int N, M, K, T;
    cin >> N >> M >> K >> T; 
    vector<Ball*> Balls(K);
    for(int i = 0 ; i < K ; i++){
        pair<int,int> ball_pos; char direction;
        cin >> ball_pos.first >> ball_pos.second >> direction;
        Balls[i] = new Ball(ball_pos, direction);
    }
    return 0;
}