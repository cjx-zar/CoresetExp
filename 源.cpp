#include<libpq-fe.h>
#include <iostream>
using namespace std;


int main() {
	int lib_ver = PQlibVersion();
	/*printf("Version of libpq: %d\n", lib_ver);*/

	PGconn* conn = PQconnectdb("host=127.0.0.1 dbname=credit user=postgres password=chen4");
	if (PQstatus(conn) == CONNECTION_BAD) {
		fprintf(stderr, "Connection to database failed: %s\n",
			PQerrorMessage(conn));
		PQfinish(conn);
		return 0;
	}
	int ver = PQserverVersion(conn);
	/*printf("Server version: %d\n", ver);*/

	string sql = "select * from train_data, bureau, bureau_balance where train_data.SK_ID_CURR = bureau.SK_ID_CURR and bureau.SK_ID_BUREAU = bureau_balance.SK_ID_BUREAU limit 1000000;";
	auto res = PQexec(conn, sql.c_str());
 
	cout << sizeof(res) << endl;

	int tuple_num = PQntuples(res);
	int field_num = PQnfields(res);
	for (int i = 0; i < tuple_num; ++i){
		for (int j = 0; j < field_num; ++j)
			cout << PQgetvalue(res, i, j) << " ";
		cout << endl;
	}

	cout << "Total cnt = " << tuple_num << endl;
	
	
	PQclear(res);
	PQfinish(conn);
	return 0;
}