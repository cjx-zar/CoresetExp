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
	unordered_map<pair<string, string>, string> join_var; // ��֮������ӱ���
	unordered_map<string, double> ht; // ÿ�����ht
	double H; // �������ݿ��H
	PGconn* conn; // �����ݿ������
	int stop; // �ֲ�ı���seq[0]�ϵ�λ�ã���joinseq[0][stop] = �ֲ��
	vector<string> alltables; // ���б������
	unordered_map<string, vector<string>> tablecolumns; // ÿ��������Щ�ֶ�
	unordered_map<string, int> dic; // ÿ���ֶ�����Ӧ������һά����

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

	//���µ�ǰ��
	void set_ans(vector<double>& new_ans) {
		cur_ans = new_ans;
		cal_h();
	}

	// �����ݿ��а���joinseq����ͳ��residualֵ
	void cal_residual() {
		// �ҵ��ֲ�ı�
		if(joinseq.size()>1){
			int i=0;
			while(true){
				if(joinseq[0][i]==joinseq[1][0]){
					stop = i;
					break;
				}
			}
		}

		// �ȼ��㵽�ֲ��ģ����û�зֲ���ֱ��������
		int n = joinseq.size();
		for(int i=0; i<n; i++){
			if(i==0)
				cal_chain_residual(joinseq[i].size()-1, stop, i);
			else
				cal_chain_residual(joinseq[i].size()-1, 0, i);
		}
		// ����зֲ��������ֲ����ͷ��
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

	// �ӵ�һ����ʼ����joinseq˳��ͳ��fullֵ
	void cal_full() {
		cal_chain_full(0, stop, 0);
		
		// ����зֲ��������ֲ��������β��
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

	// ����ÿ�����ht��H
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

	//�����ض�ά���ϵ�loss_functionֵ
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