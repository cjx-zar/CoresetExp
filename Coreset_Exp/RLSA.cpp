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
	unordered_map<string, string> totail, tohead; // 每个表往尾和往头联接时所使用的联接变量
	unordered_map<string, vector<string>> join_table; // 所有表的邻接表
	unordered_map<string, double> ht; // 每个表的ht
	double H; // 整个数据库的H
	PGconn* conn; // 与数据库的连接
	int stop = -1; // 分叉的表在seq[0]上的位置，即joinseq[0][stop] = 分叉表
	vector<string> alltables; // 所有表的名称
	unordered_map<string, vector<string>> tablecolumns; // 每个表有哪些字段
	unordered_map<string, int> dic; // 每个字段所对应的是哪一维特征

public:
	RLSA(vector<vector<string>>& seq, vector<double>& ans, vector<string>& all, string dbname, string password, string host, unordered_map<string, vector<string>>& jtable, unordered_map<string, string>& tail, unordered_map<string, string>& head) {
		joinseq = seq;
		cur_ans = ans;
		alltables = all;
		join_table = jtable;
		totail = tail;
		tohead = head;

		conn = PQsetdbLogin("127.0.0.1", "5432", NULL, NULL, "credit", "postgres", "chen4");
		// conn = PQsetdbLogin(host, "5432", NULL, NULL, dbname, "postgres", password);
		if (PQstatus(conn) == CONNECTION_BAD) {
			fprintf(stderr, "Connection to database failed: %s\n",
				PQerrorMessage(conn));
			PQfinish(conn);
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
		if (joinseq.size() > 1) {
			int i = 0;
			while (true) {
				if (joinseq[0][i] == joinseq[1][0]) {
					stop = i;
					break;
				}
			}
		}

		// 先计算到分叉表的，如果没有分叉表就直接算完了
		int n = joinseq.size();
		for (int i = 0; i < n; i++) {
			if (i == 0)
				cal_chain_residual(joinseq[i].size() - 1, stop + 1, i);
			else
				cal_chain_residual(joinseq[i].size() - 1, 1, i);
		}
		// 如果有分叉表，继续算分叉表到开头的
		if (stop != -1)
			cal_chain_residual(stop, 0, 0);
	}

	void cal_chain_residual(int start, int end, int chainnum) {
		/*   not completed   */
		int i = start;
		string sql;
		for (int i = start; i >= end; i--) {
			// 计算res_view, 是表中每个tuple都要有的一个记录，等价于res_view中必须包含主键



			// 计算full_view，是res_view的汇总（累乘sum）






			//生成一个view，里面是group by count()的结果，然后对于后面的表直接去查view就好了
			if (tohead[joinseq[chainnum][i]].empty() || tohead[joinseq[chainnum][i]] == totail[joinseq[chainnum][i]]) {
				sql = "create view " + joinseq[chainnum][i] + "_view as select "\
					+ totail[joinseq[chainnum][i]] + ", count(" + totail[joinseq[chainnum][i]] + ")from "\
					+ joinseq[chainnum][i] + " group by " + totail[joinseq[chainnum][i]] + "; ";
			}
			else if (totail[joinseq[chainnum][i]].empty()) {
				sql = "create view " + joinseq[chainnum][i] + "_view as select "\
					+ tohead[joinseq[chainnum][i]] + ", count(" + tohead[joinseq[chainnum][i]] + ")from "\
					+ joinseq[chainnum][i] + " group by " + tohead[joinseq[chainnum][i]] + "; ";
			}
			else {
				sql = "create view " + joinseq[chainnum][i] + "_view as select "\
					+ tohead[joinseq[chainnum][i]] + "," + totail[joinseq[chainnum][i]] + ", count(*)from "\
					+ joinseq[chainnum][i] + " group by " + tohead[joinseq[chainnum][i]] + "," + totail[joinseq[chainnum][i]] + "; ";
			}
			auto res = PQexec(conn, sql.c_str());


			if (i == start && i != stop) {// 最末尾的表，
				sql = "select * from " + joinseq[chainnum][i] + "_view";
				auto res = PQexec(conn, sql.c_str());
				int tuple_num = PQntuples(res);
				for (int i = 0; i < tuple_num; ++i) {
					//对于view里的每一项，要乘以在其接下来的表的view上的count
					for (auto next : join_table[joinseq[chainnum][i]]) {
						sql = "select count from " + next + "_view where " + next + "_view." + tohead[next] + "=" + joinseq[chainnum][i] + "_view." + totail[joinseq[chainnum][i]] + ";";
						auto res = PQexec(conn, sql.c_str());

					}
				}
			}
		}

	}

	// 从第一个表开始按照joinseq顺序统计full值
	void cal_full() {
		int n = joinseq.size();
		for (int i = 0; i < n; i++) {
			cal_chain_full(0, joinseq[i].size() - 1, i);
		}
	}

	void cal_chain_full(int start, int end, int chainnum) {
		/*   not completed   */
		auto i = start;
		do {
			string sql = "";
			auto res = PQexec(conn, sql.c_str());
			i++;
		} while (i != end);
	}

	// 计算每个表的ht和H
	void cal_h() {
		H = 0;
		for (auto& table : alltables) {
			string sql = "";
			auto res = PQexec(conn, sql.c_str());
			vector<int> dim;
			dim.reserve(tablecolumns[table].size());
			for (auto col : tablecolumns[table]) {
				dim.emplace_back(dic[col]);
			}
			ht[table] = loss_function(res, dim);
			H += ht[table];
		}
	}

	//返回特定维度上的loss_function值
	double loss_function(PGresult* tuples, vector<int>& dim) {
		/*   weight    */
		double res = 0;

		int tuple_num = PQntuples(tuples);
		int field_num = PQnfields(tuples);

		for (int i = 0; i < tuple_num; ++i) {
			for (int j = 0; j < field_num; ++j)
				res += pow((atoi(PQgetvalue(tuples, i, j)) - cur_ans[dim[j]]), 2);//* weight
		}
		return res;
	}

	void cal_label() {

	}

	vector<vector<double>> rlsample(int samplenum) {
		vector<vector<double>> res;
		string sql = "";
		long long n = (long long)PQexec(conn, sql.c_str());
		int N = ceil(log(n)) + ceil(log(alltables.size()));
		for (int j = N; j > 0; j--) {
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