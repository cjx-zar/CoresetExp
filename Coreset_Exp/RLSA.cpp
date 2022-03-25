#include<libpq-fe.h>
#include <iostream>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <math.h>
using namespace std;

class RLSA {
private:
	vector<vector<string>> joinseq; // ����˳��Ĭ��ֻ��һ���ֲ�����ǿ����ж�·���ӣ�����зֲ��joinseq[0]�ϵ�������·�������඼�Ǵӷֲ��ʼ��·��
	// ��: joinseq[0] = a-b-c-d-e
	// 	   joinseq[1] = c-f-g
	// 	   joinseq[2] = c-h-i-j-k	

	vector<double> cur_ans; // ��ǰ��
	unordered_map<string, string> totail, tohead; // ÿ������β����ͷ����ʱ��ʹ�õ����ӱ���
	unordered_map<string, vector<string>> join_table; // ���б���ڽӱ�
	unordered_map<string, double> ht; // ÿ�����ht
	double H; // �������ݿ��H
	PGconn* conn; // �����ݿ������
	int stop = -1; // �ֲ�ı���seq[0]�ϵ�λ�ã���joinseq[0][stop] = �ֲ��
	vector<string> alltables; // ���б������
	unordered_map<string, vector<string>> tablecolumns; // ÿ��������Щ�ֶ�
	unordered_map<string, int> dic; // ÿ���ֶ�����Ӧ������һά����

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

	//���µ�ǰ��
	void set_ans(vector<double>& new_ans) {
		cur_ans = new_ans;
		cal_h();
	}

	// �����ݿ��а���joinseq����ͳ��residualֵ
	void cal_residual() {
		// �ҵ��ֲ�ı�
		if (joinseq.size() > 1) {
			int i = 0;
			while (true) {
				if (joinseq[0][i] == joinseq[1][0]) {
					stop = i;
					break;
				}
			}
		}

		// �ȼ��㵽�ֲ��ģ����û�зֲ���ֱ��������
		int n = joinseq.size();
		for (int i = 0; i < n; i++) {
			if (i == 0)
				cal_chain_residual(joinseq[i].size() - 1, stop + 1, i);
			else
				cal_chain_residual(joinseq[i].size() - 1, 1, i);
		}
		// ����зֲ��������ֲ����ͷ��
		if (stop != -1)
			cal_chain_residual(stop, 0, 0);
	}

	void cal_chain_residual(int start, int end, int chainnum) {
		/*   not completed   */
		int i = start;
		string sql;
		for (int i = start; i >= end; i--) {
			// ����res_view, �Ǳ���ÿ��tuple��Ҫ�е�һ����¼���ȼ���res_view�б����������



			// ����full_view����res_view�Ļ��ܣ��۳�sum��






			//����һ��view��������group by count()�Ľ����Ȼ����ں���ı�ֱ��ȥ��view�ͺ���
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


			if (i == start && i != stop) {// ��ĩβ�ı�
				sql = "select * from " + joinseq[chainnum][i] + "_view";
				auto res = PQexec(conn, sql.c_str());
				int tuple_num = PQntuples(res);
				for (int i = 0; i < tuple_num; ++i) {
					//����view���ÿһ�Ҫ��������������ı��view�ϵ�count
					for (auto next : join_table[joinseq[chainnum][i]]) {
						sql = "select count from " + next + "_view where " + next + "_view." + tohead[next] + "=" + joinseq[chainnum][i] + "_view." + totail[joinseq[chainnum][i]] + ";";
						auto res = PQexec(conn, sql.c_str());

					}
				}
			}
		}

	}

	// �ӵ�һ����ʼ����joinseq˳��ͳ��fullֵ
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

	// ����ÿ�����ht��H
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

	//�����ض�ά���ϵ�loss_functionֵ
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
			//S1: ��ÿ������ɸѡ��������ht��j���ǩ
			cal_residual();
			cal_full();
			cal_h();
			cal_label();
			//S2: ����residual��Ϣ����sample


		}


		return res;
	}

};