import java.sql.*;
import java.util.HashMap;

public class Schema {
	private static final String usr = "postgres";
	private static final String password = "password";
	private static final String url = "jdbc:postgresql://localhost:5432/postgres";
	private static HashMap<String, String> dataType = new HashMap<>();

	public static HashMap<String, String> getSchema() {
		connect();
		retrieve();
		return dataType;
	}

	public static void connect(){
		try {
			Class.forName("org.postgresql.Driver");     //Loads the required driver
			System.out.println("Success loading Driver!");
		} catch(Exception exception) {
			System.out.println("Fail loading Driver!");
			exception.printStackTrace();
		}
	}
	public static void retrieve() {
		try {
			Connection con = DriverManager.getConnection(url, usr, password);
			System.out.println("Success connecting server!");

			ResultSet rs;
			Statement st = con.createStatement();
			String query =
					"select data_type, column_name " +
							"from information_schema.columns " +
							"where table_name= 'sales'";
			rs = st.executeQuery(query);
			boolean more;
			more = rs.next();
			while (more) {
				if (rs.getString("data_type").contains("character")) {
					dataType.put(rs.getString("column_name"), "String");
				} else {
					dataType.put(rs.getString("column_name"), "int");

				}
				more = rs.next();
			}

		} catch (SQLException exception) {
			System.out.println("Connection URL or username or password errors!");
			exception.printStackTrace();
		}
	}
}
