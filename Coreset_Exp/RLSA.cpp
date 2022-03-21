#include<libpq-fe.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <math.h>
using namespace std;

class RLSA {
private:
	vector<vector<string>> joinseq; // 连接顺序，默认只有一个分叉表，但是可以有多路连接，如果有分叉表joinseq[0]上的是完整路径，其余都是从分叉表开始的路径
	// 例: joinseq[0] = a-b-c-d-e
	// 	   joinseq[1] = c-f-g
	// 	   joinseq[2] = c-h-i-j-k	
	
	vector<double> cur_ans; // 当前解
	unordered_map<pair<string, string>, string> join_var; // 表之间的连接变量
	unordered_map<string, double> ht; // 每个表的ht
	double H; // 整个数据库的H
	PGconn* conn; // 与数据库的连接
	int stop; // 分叉的表在seq[0]上的位置，即joinseq[0][stop] = 分叉表
	vector<string> alltables; // 所有表的名称
	unordered_map<string, vector<string>> tablecolumns; // 每个表有哪些字段
	unordered_map<string, int> dic; // 每个字段所对应的是哪一维特征

public:
	RLSA(vector<vector<string>>& seq, vector<double>& ans, unordered_map<pair<string, string>, string>& var,vector<string>& all, string dbname) {
		joinseq = seq;
		cur_ans = ans;
		join_var = var;
		alltables = all;

		conn = PQconnectdb("host=127.0.0.1 dbname=" + dbname + " user=postgres password=chen4");
		if (PQstatus(conn) == CONNECTION_BAD) {
			fprintf(stderr, "Connection to database failed: %s\n",
			PQerrorMessage(conn));
			PQfinish(conn);
			return 0;
		}
	}

	//更新当前解
	void set_ans(vector<double>& new_ans) {
		cur_ans = new_ans;
		cal_h();
	}

	// 在数据库中按照joinseq逆序统计residual值
	void cal_residual() {
		// 找到分叉的表
		if(joinseq.size()>1){
			int i=0;
			while(true){
				if(joinseq[0][i]==joinseq[1][0]){
					stop = i;
					break;
				}
			}
		}

		// 先计算到分叉表的，如果没有分叉表就直接算完了
		int n = joinseq.size();
		for(int i=0; i<n; i++){
			if(i==0)
				cal_chain_residual(joinseq[i].size()-1, stop, i);
			else
				cal_chain_residual(joinseq[i].size()-1, 0, i);
		}
		// 如果有分叉表，继续算分叉表到开头的
		if(stoptable!=null)
			cal_chain_residual(stop, 0, 0);
	}

	void cal_chain_residual(int start, int end, int chainnum) {
		/*   not completed   */
		int i = start;
		do{
			string sql = "";
			auto res = PQexec(conn, sql.c_str());
			i--;
		}while(i!=end);
	}

	// 从第一个表开始按照joinseq顺序统计full值
	void cal_full() {
		cal_chain_full(0, stop, 0);
		
		// 如果有分叉表，继续算分叉表到各个结尾的
		if(stoptable!=null){
			int n = joinseq.size();
			for(int i=0; i<n; i++){
				if(i==0)
					cal_chain_full(stop, joinseq[i].size()-1, i);
				else
					cal_chain_full(0, joinseq[i].size()-1, i);
			}
		}
	}

	void cal_chain_full(int start, int end, int chainnum) {
		/*   not completed   */
		auto i = start;
		do{
			string sql = "";
			auto res = PQexec(conn, sql.c_str());
			i++;
		}while(i!=end);
	}

	// 计算每个表的ht和H
	void cal_h() {
		H = 0;
		for(auto& table : alltables){
			string sql = "";
			auto res = PQexec(conn, sql.c_str());
			vector<int> dim(tablecolumns[table].size());
			for(auto col : tablecolumns[table]){
				dim.eplace(dic[col]);
			}
			ht[table] = loss_function(res, dim);
			H += ht[table];
		}
	}

	//返回特定维度上的loss_function值
	double loss_function(PGresult * tuples ,vector<int>& dim) {
		/*   weight    */
		double res = 0;

		int tuple_num = PQntuples(tuples);
		int field_num = PQnfields(tuples);
		
		for (int i = 0; i < tuple_num; ++i){
			for (int j = 0; j < field_num; ++j)
				res += pow((PQgetvalue(res, i, j) - cur_ans[dim[j]]), 2) ;//* weight
		}
		return res;
	}

	void cal_label(){

	}

	vector<vector<double>> rlsample(int samplenum) {
		vector<vector<double>> res;
		string sql = "";
		long long n = (long long)PQexec(conn, sql.c_str());
		int N = math.ceil(log(n)) + math.ceil(log(alltables.size()));
		for(int j=N; j>0; j--){
			//S1: 对每个表，先筛选表，再利用ht和j算标签
			cal_residual();
			cal_full();
			cal_h();
			cal_label();
			//S2: 利用residual信息进行sample

			
		}


		return res;
	}

};