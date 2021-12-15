import java.sql.*;
import java.util.*;
public class MF {

	private static final String usr = "postgres";
	private static final String pwd = "password";
	private static final String url = "jdbc:postgresql://localhost:5432/postgres";

	List<Result> resultTable = new ArrayList<>();
	List<MFStruct> mfStruct = new ArrayList<>();

	static class Sales {
		String prod;
		int month;
		int year;
		String state;
		int quant;
		int day;
		String cust;
	}

	static class MFStruct{
		String cust;
		int sum_quant_1;
		int count_quant_1;
		int avg_quant_1;
		int sum_quant_2;
		int count_quant_2;
		int avg_quant_2;
		int sum_quant_3;
		int count_quant_3;
		int avg_quant_3;
	}

	static class Result{
		String cust;
		int avg_quant_1;
		int avg_quant_2;
		int avg_quant_3;
	}

	public static void main(String [] args){
		MF mf = new MF();
		mf.connect();
		mf.retrieve();
		mf.addToResultTable();
		mf.resultTable();
	}

	public void connect() { 
		try { 
			Class.forName("org.postgresql.Driver");
			System.out.println("Success loading Driver!");
		}catch(Exception exception) {
			System.out.println("Fail loading Driver!");
			exception.printStackTrace();
		}
	}
	public void retrieve(){
		try {
			Connection con = DriverManager.getConnection(url, usr, pwd);
			ResultSet rs;
			boolean more;
			Statement st = con.createStatement();
			String query = "select * from sales";


			 /** 
			 * Generating while loops for each grouping variable. 
			 */ 

			//While loop for grouping variable 1.
			rs = st.executeQuery(query);
			more = rs.next();
			while(more){
				Sales currRow = new Sales();
				currRow.prod = rs.getString("prod");
				currRow.month = rs.getInt("month");
				currRow.year = rs.getInt("year");
				currRow.state = rs.getString("state");
				currRow.quant = rs.getInt("quant");
				currRow.day = rs.getInt("day");
				currRow.cust = rs.getString("cust");
				if(currRow.year==2016 && currRow.prod.equals("Coke")){
					if (currRow.state.equals("NY")){
						boolean found = false;
						for(MFStruct mfsRow: mfStruct){
							if(compare(mfsRow.cust, currRow.cust)) {
								found = true;
								mfsRow.sum_quant_1 += currRow.quant;
								mfsRow.count_quant_1 ++;
								if(mfsRow.count_quant_1 !=0){
									mfsRow.avg_quant_1 = mfsRow.sum_quant_1/mfsRow.count_quant_1;
								}
							}
						}
						if(found == false){
							MFStruct mfsRow = new MFStruct();
							mfsRow.cust = currRow.cust;
							mfsRow.sum_quant_1 = currRow.quant;
							mfsRow.count_quant_1++;
							if(mfsRow.count_quant_1 !=0){
								mfsRow.avg_quant_1 = mfsRow.sum_quant_1/mfsRow.count_quant_1;
							}
							mfStruct.add(mfsRow);
						}
					}
				}
				more = rs.next();
			}

			//While loop for grouping variable 2.
			rs = st.executeQuery(query);
			more = rs.next();
			while(more){
				Sales currRow = new Sales();
				currRow.prod = rs.getString("prod");
				currRow.month = rs.getInt("month");
				currRow.year = rs.getInt("year");
				currRow.state = rs.getString("state");
				currRow.quant = rs.getInt("quant");
				currRow.day = rs.getInt("day");
				currRow.cust = rs.getString("cust");
				if(currRow.year==2016 && currRow.prod.equals("Coke")){
					if (currRow.state.equals("NJ")){
						boolean found = false;
						for(MFStruct mfsRow: mfStruct){
							if(compare(mfsRow.cust, currRow.cust)) {
								found = true;
								mfsRow.sum_quant_2 += currRow.quant;
								mfsRow.count_quant_2 ++;
								if(mfsRow.count_quant_2 !=0){
									mfsRow.avg_quant_2 = mfsRow.sum_quant_2/mfsRow.count_quant_2;
								}
							}
						}
						if(found == false){
							MFStruct mfsRow = new MFStruct();
							mfsRow.cust = currRow.cust;
							mfsRow.sum_quant_2 = currRow.quant;
							mfsRow.count_quant_2++;
							if(mfsRow.count_quant_2 !=0){
								mfsRow.avg_quant_2 = mfsRow.sum_quant_2/mfsRow.count_quant_2;
							}
							mfStruct.add(mfsRow);
						}
					}
				}
				more = rs.next();
			}

			//While loop for grouping variable 3.
			rs = st.executeQuery(query);
			more = rs.next();
			while(more){
				Sales currRow = new Sales();
				currRow.prod = rs.getString("prod");
				currRow.month = rs.getInt("month");
				currRow.year = rs.getInt("year");
				currRow.state = rs.getString("state");
				currRow.quant = rs.getInt("quant");
				currRow.day = rs.getInt("day");
				currRow.cust = rs.getString("cust");
				if(currRow.year==2016 && currRow.prod.equals("Coke")){
					if (currRow.state.equals("CT")){
						boolean found = false;
						for(MFStruct mfsRow: mfStruct){
							if(compare(mfsRow.cust, currRow.cust)) {
								found = true;
								mfsRow.sum_quant_3 += currRow.quant;
								mfsRow.count_quant_3 ++;
								if(mfsRow.count_quant_3 !=0){
									mfsRow.avg_quant_3 = mfsRow.sum_quant_3/mfsRow.count_quant_3;
								}
							}
						}
						if(found == false){
							MFStruct mfsRow = new MFStruct();
							mfsRow.cust = currRow.cust;
							mfsRow.sum_quant_3 = currRow.quant;
							mfsRow.count_quant_3++;
							if(mfsRow.count_quant_3 !=0){
								mfsRow.avg_quant_3 = mfsRow.sum_quant_3/mfsRow.count_quant_3;
							}
							mfStruct.add(mfsRow);
						}
					}
				}
				more = rs.next();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
	public void addToResultTable(){
		for(MFStruct mfs: mfStruct){
			Result result = new Result();
			result.cust = mfs.cust;
			if(mfs.avg_quant_1 > mfs.avg_quant_2 && mfs.avg_quant_1 > mfs.avg_quant_3){
				result.avg_quant_1 = mfs.avg_quant_1;
				result.avg_quant_2 = mfs.avg_quant_2;
				result.avg_quant_3 = mfs.avg_quant_3;
				resultTable.add(result);
			}
		}
	}
	public void resultTable(){
		System.out.printf("%-4s","cust\t");
		System.out.printf("%-11s","avg_quant_1\t");
		System.out.printf("%-11s","avg_quant_2\t");
		System.out.printf("%-11s","avg_quant_3\t");
		System.out.println();
		System.out.printf("====\t===========\t===========\t===========\t ");
		for(Result result: resultTable){
			System.out.printf("\n");
			System.out.printf("%-4s\t", result.cust);
			System.out.printf("%11s\t", result.avg_quant_1);
			System.out.printf("%11s\t", result.avg_quant_2);
			System.out.printf("%11s\t", result.avg_quant_3);
		}
	}
	public boolean compare(String str1, String str2){
		return str1.equals(str2);
	}
	public boolean compare(int num1, int num2){
		return (num1 == num2);
	}
}
