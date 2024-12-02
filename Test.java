package sample;

import java.sql.Connection;
import java.sql.DriverManager;

public class Test 
{
	private static Connection con = null;

	private Test() {
	}

	static {
		try {

			Class.forName(DBInfo.driver);

			con = DriverManager.getConnection(DBInfo.DBurl, DBInfo.DBname, DBInfo.Dbpass);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Connection getconn() {
		return con;
	}
}
