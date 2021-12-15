import java.io.*;
import java.util.*;

/**
 * GenerateMF class to generate MF.java file
 */
public class GenerateMF {

	public static void generateMF(HashMap<String, String> dataType) {
		try {
			File output = new File("src/MF.java");
			PrintWriter writer = new PrintWriter(output);
			writer.print("import java.sql.*;\n");
			writer.print("import java.util.*;\n");

			// creating MF class
			writer.print("public class MF {\n\n"); // opening bracket of MF class
			writer.print("\tprivate static final String usr = \"postgres\";\n");
			writer.print("\tprivate static final String pwd = \"password\";\n");
			writer.print("\tprivate static final String url = \"jdbc:postgresql://localhost:5432/postgres\";\n\n");

			// declare resultTable and mfStruct
			writer.print("\tList<Result> resultTable = new ArrayList<>();\n");
			writer.print("\tList<MFStruct> mfStruct = new ArrayList<>();\n\n");

			MainClass mainClass = new MainClass();

			// generate data structure used in the program: Sales schema, MFStruct, and Result class
			generateStruct(dataType, writer, mainClass);

			// generate main() method, a method to start the program
			generateMain(writer);

			// generate connect() method, a method to connect to the database
			generateConnect(writer); 

			// generate retrieve() method, a method to retrieve data from the database
			generateRetrieve(writer, mainClass, dataType);

			// generate addToResultTable method, a method to add data to the result table
			generateAddToResultTable(writer, mainClass);

			// generate resultTable() method, a method to output data
			generateResultTable(writer, mainClass);

			// utility methods used in MF.java
			generateUtilityMethods(writer);

			writer.print("}\n"); // closing bracket of MF class
			writer.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * This function create several data structure(Sales/Result/MFStruct)we use to generate the MF query
	 * Sales class: stores the schema of the sales table
	 * Result class: stores all the select attributes. A single Result class is a row of the output table.
	 * @param dataType sales schema
	 * @param writer write mf code to a file
	 * @param mainClass a class to get input from a file or terminal
	 */
	public static void generateStruct(HashMap<String, String> dataType, PrintWriter writer, MainClass mainClass) {

		// To generate the sales schema:
		// key is the column name, value is dataType of that column.
		writer.print("\tstatic class Sales {\n"); // opening bracket of sales schema
		for (Map.Entry<String, String> entry : dataType.entrySet())
			writer.print("\t\t" + entry.getValue() + " " + entry.getKey() + ";\n");
		writer.print("\t}\n\n"); // closing bracket of sales schema

		// generate the MFStruct: using fvect and grouping attributes
		List<String> addedVariable = new ArrayList<>();
		writer.print("\tstatic class MFStruct{\n"); // opening bracket of MFStruct
		for (String str : mainClass.getGroupAttributes()) { // for loop to print out group attributes
			for (Map.Entry<String, String> entry : dataType.entrySet())
				if (str.equals(entry.getKey())) {
					writer.print("\t\t" + entry.getValue() + " " + entry.getKey() + ";\n");
				}
		}

		for(VariableBuilder vb : mainClass.getFVect()) {
			if(vb.aggregate.contains("avg")) {
				String sumVariable = "sum_" + vb.attribute + "_" + vb.index;
				String countVariable = "count_" + vb.attribute + "_" + vb.index;
				if(!addedVariable.contains(sumVariable)) {
					writer.print("\t\tint " + sumVariable + ";\n");
					addedVariable.add(sumVariable);
				}
				if(!addedVariable.contains(countVariable)) {
					writer.print("\t\tint " + countVariable + ";\n");
					addedVariable.add(countVariable);
				}
			}
			if(!addedVariable.contains(vb.getString())) {
				writer.print("\t\tint " + vb.getString() + ";\n");
				addedVariable.add(vb.getString());
			}
		}

		writer.print("\t}\n\n"); // closing bracket of MFStruct

		// generate the result class
		writer.print("\tstatic class Result{\n"); // opening bracket of result class
		for (String str : mainClass.getSelectAttributes()) {
			if (dataType.get(str) != null) {
				writer.print("\t\t" + dataType.get(str) + " " + str + ";\n");
			} else {
				writer.print("\t\tint " + str + ";\n");
			}
		}
		writer.print("\t}\n\n"); // closing bracket of result class
	}

	public static void generateMain(PrintWriter writer) {
		writer.print("\tpublic static void main(String [] args){\n");
		writer.print("\t\tMF mf = new MF();\n");
		writer.print("\t\tmf.connect();\n");
		writer.print("\t\tmf.retrieve();\n");
		writer.print("\t\tmf.addToResultTable();\n");
		writer.print("\t\tmf.resultTable();\n");
		writer.print("\t}\n\n");
	}

	public static void generateConnect(PrintWriter writer) {
		writer.print("\tpublic void connect() { \n");
		writer.print("\t\ttry { \n");
		writer.print("\t\t\tClass.forName(\"org.postgresql.Driver\");\n");
		writer.print("\t\t\tSystem.out.println(\"Success loading Driver!\");\n");
		writer.print("\t\t}catch(Exception exception) {\n");
		writer.print("\t\t\tSystem.out.println(\"Fail loading Driver!\");\n");
		writer.print("\t\t\texception.printStackTrace();\n");
		writer.print("\t\t}\n");
		writer.print("\t}\n");
	}

	public static void generateRetrieve(PrintWriter writer, MainClass mainClass, HashMap<String, String> dataType) {
		writer.print("\tpublic void retrieve(){\n");
		writer.print("\t\ttry {\n");
		writer.print("\t\t\tConnection con = DriverManager.getConnection(url, usr, pwd);\n");

		// Declaring variables
		writer.print("\t\t\tResultSet rs;\n");
		writer.print("\t\t\tboolean more;\n");
		writer.print("\t\t\tStatement st = con.createStatement();\n");
		writer.print("\t\t\tString query = \"select * from sales\";\n");
		writer.print("\n");

		generateWhileLoop(writer, mainClass, dataType);

		writer.print("\t\t}catch(Exception e) {\n");
		writer.print("\t\t\te.printStackTrace();\n");
		writer.print("\t\t}\n");
		writer.print("\t}\n");
	}

	public static void generateWhileLoop(PrintWriter writer, MainClass mainClass, HashMap<String, String> dataType) {
		List<String> addedVariable = new ArrayList<>();
		List<String> updatedVariable = new ArrayList<>();

		// Generating number of while loops equal to number of Grouping variables.
		writer.print("\n\t\t\t /** \n\t\t\t * Generating while loops for each grouping variable. \n\t\t\t */ \n");
		for (int i = 0; i < mainClass.getNoGV(); i++) {
			writer.print("\n\t\t\t//While loop for grouping variable " + (i + 1) + ".\n");
			writer.print("\t\t\trs = st.executeQuery(query);\n");
			writer.print("\t\t\tmore = rs.next();\n");
			writer.print("\t\t\twhile(more){\n");
			writer.print("\t\t\t\tSales currRow = new Sales();\n");

			// get a row of data from the database and store it to the currRow using Sales class
			for (Map.Entry<String, String> entry : dataType.entrySet()) {
				if (entry.getValue().equals("String")) {
					writer.print("\t\t\t\tcurrRow." + entry.getKey() + " = rs.getString(\"" + entry.getKey()
							+ "\");\n");
				} else if (entry.getValue().equals("int")) {
					writer.print("\t\t\t\tcurrRow." + entry.getKey() + " = rs.getInt(\"" + entry.getKey()
							+ "\");\n");
				}
			}

			writer.print("\t\t\t\tif(" + mainClass.getWhere() + "){\n"); // where condition
			writer.print("\t\t\t\t\tif (" ); // select condition
			if(mainClass.getSuchThat().size() != 0) {
				writer.print(mainClass.getSuchThat().get(i));
			} else {
				writer.print("true");
			}
			writer.print("){\n");

			writer.print("\t\t\t\t\t\tboolean found = false;\n");
			writer.print("\t\t\t\t\t\tfor(MFStruct mfsRow: mfStruct){\n");

			writer.print("\t\t\t\t\t\t\tif(");
			if(mainClass.getGroupAttributes().size() != 0) {
				StringJoiner joiner = new StringJoiner(" && ");
				for(String str : mainClass.getGroupAttributes()) {
					str = "compare(mfsRow." + str + ", currRow." + str +")";
					joiner.add(str);
				}
				writer.print(joiner.toString());
			} else {
				writer.print("true");
			}
			writer.print(") {\n");

			writer.print("\t\t\t\t\t\t\t\tfound = true;\n");

			// Outputting the aggregate functions if record is added already.
			for (VariableBuilder vb : mainClass.getFVect()) {
				if (Integer.parseInt(vb.index) == i + 1) {
					if (vb.aggregate.equals("avg")) {
						String sum = "sum_" + vb.attribute + "_" + vb.index;
						String count = "count_" + vb.attribute + "_" + vb.index;
						if (!updatedVariable.contains(sum)) {
							updatedVariable.add(sum);
							writer.print("\t\t\t\t\t\t\t\tmfsRow." + sum
									+ " += currRow." + vb.attribute + ";\n");
						}
						if (!updatedVariable.contains(count)) {
							updatedVariable.add(count);
							writer.print("\t\t\t\t\t\t\t\tmfsRow." + count + " ++;\n");
						}
						if (!updatedVariable.contains(vb.getString())) {
							updatedVariable.add(vb.getString());
							writer.print("\t\t\t\t\t\t\t\tif(mfsRow." + count + " !=0){\n");
							writer.print("\t\t\t\t\t\t\t\t\tmfsRow." + vb.getString() +
									" = mfsRow." + sum + "/mfsRow." + count + ";\n");
							writer.print("\t\t\t\t\t\t\t\t}\n");
						}
					}
					if (!updatedVariable.contains(vb.getString()) && vb.aggregate.equals("sum")) {
						writer.print("\t\t\t\t\t\t\t\tmfsRow." + vb.getString()
								+ " += currRow." + vb.attribute + ";\n");
						updatedVariable.add(vb.getString());
					}
					if (!updatedVariable.contains(vb.getString()) && vb.aggregate.equals("max")) {
						writer.print("\t\t\t\t\t\t\t\tmfsRow." + vb.getString() + " = (mfsRow." + vb.getString()
								+ "< currRow." + vb.attribute + ") ? currRow." + vb.attribute + " : mfsRow."
								+ vb.getString() + ";\n");
						updatedVariable.add(vb.getString());
					}
					if (!updatedVariable.contains(vb.getString()) && vb.aggregate.equals("min")) {
						writer.print("\t\t\t\t\t\t\t\tmfsRow." + vb.getString() + " = (mfsRow." + vb.getString()
								+ "> currRow." + vb.attribute + ") ? currRow." + vb.attribute + " : mfsRow."
								+ vb.getString() + ";\n");
						updatedVariable.add(vb.getString());
					}
					if (!updatedVariable.contains(vb.getString()) && vb.aggregate.equals("count")) {
						writer.print("\t\t\t\t\t\t\t\tmfsRow." + vb.getString() + "++;\n");
						updatedVariable.add(vb.getString());
					}
				}
			}
			writer.print("\t\t\t\t\t\t\t}\n");
			writer.print("\t\t\t\t\t\t}\n");

			writer.print("\t\t\t\t\t\tif(found == false){\n");
			writer.print("\t\t\t\t\t\t\tMFStruct mfsRow = new MFStruct();\n");
			for (String str : mainClass.getGroupAttributes()) {
				writer.print("\t\t\t\t\t\t\tmfsRow." + str + " = currRow." + str + ";\n");
			}
			for (VariableBuilder vb : mainClass.getFVect()) {
				if (Integer.parseInt(vb.index) == i + 1) {
					if (vb.aggregate.equals("avg")) {
						String sum = "sum_" + vb.attribute + "_" + vb.index;
						String count = "count_" + vb.attribute + "_" + vb.index;
						if (!addedVariable.contains(sum)) {
							addedVariable.add(sum);
							writer.print("\t\t\t\t\t\t\tmfsRow." + "sum_" + vb.attribute + "_"
									+ vb.index + " = currRow." + vb.attribute + ";\n");
						}
						if (!addedVariable.contains(count)) {
							addedVariable.add(count);
							writer.print("\t\t\t\t\t\t\tmfsRow." + "count_" + vb.attribute
									+ "_" + vb.index + "++;\n");
						}
						if (!addedVariable.contains(vb.getString())) {
							addedVariable.add(vb.getString());
							writer.print("\t\t\t\t\t\t\tif(mfsRow." + count + " !=0){\n");
							writer.print("\t\t\t\t\t\t\t\tmfsRow." + vb.getString() + " = mfsRow."
									+ sum + "/mfsRow." + count + ";\n");
							writer.print("\t\t\t\t\t\t\t}\n");
						}

					} else {
						if (!addedVariable.contains(vb.getString())) {
							if (vb.aggregate.equals("count")) {
								writer.print("\t\t\t\t\t\t\tmfsRow." + "count_" + vb.attribute
										+ "_" + vb.index + "++;\n");
							} else {
								writer.print("\t\t\t\t\t\t\tmfsRow." + vb.getString()
										+ " = currRow." + vb.attribute + ";\n");
							}
							addedVariable.add(vb.getString());
						}
					}
				}
			}
			writer.print("\t\t\t\t\t\t\tmfStruct.add(mfsRow);\n");
			writer.print("\t\t\t\t\t\t}\n");
			writer.print("\t\t\t\t\t}\n");
			writer.print("\t\t\t\t}\n");
			writer.print("\t\t\t\tmore = rs.next();\n");
			writer.print("\t\t\t}\n");
		}
	}

	public static void generateAddToResultTable(PrintWriter writer, MainClass mainClass) {

		writer.print("\tpublic void addToResultTable(){\n"); // opening bracket of addToResultTable
		writer.print("\t\tfor(MFStruct mfs: mfStruct){\n"); // opening bracket of iterating
		writer.print("\t\t\tResult result = new Result();\n");

		for (String str : mainClass.getGroupAttributes()) {
			writer.print("\t\t\tresult." + str + " = mfs." + str + ";\n");
		}

		writer.print("\t\t\tif(");
		if (!mainClass.getHaving().equals("")) { // having condition
			String having = mainClass.getHaving();
			if (having.contains("sum")) {
				having = having.replace("sum", "mfs.sum");
			}
			if(having.contains("avg")) {
				having = having.replace("avg", "mfs.avg");
			}
			if(having.contains("max")) {
				having = having.replace("max", "mfs.max");
			}
			if(having.contains("min")) {
				having = having.replace("min", "mfs.min");
			}
			if(having.contains("count")) {
				having = having.replace("count", "mfs.count");
			}
			writer.print(having);
		} else { // if there is no having condition, just write true
			writer.print("true");
		}
		writer.print("){\n");// opening bracket of having condition
		for (String str : mainClass.getSelectAttributes()) {
			for (VariableBuilder vb : mainClass.getFVect()) {
				if (str.equals(vb.getString())) {
					writer.print("\t\t\t\tresult." + vb.getString() + " = mfs." + vb.getString() + ";\n");
				}
			}
		}
		writer.print("\t\t\t\tresultTable.add(result);\n");
		writer.print("\t\t\t}\n"); // closing bracket of if block of having condition
		writer.print("\t\t}\n"); // closing bracket of iterating mfStruct
		writer.print("\t}\n"); // closing bracket of addToResultTable.
	}
	public static void generateResultTable(PrintWriter writer, MainClass mainClass) {
		int len = 0;
		writer.print("\tpublic void resultTable(){\n");

		for (String str : mainClass.getSelectAttributes()) {// code to print out header of the table
			len = str.length();
			writer.print("\t\tSystem.out.printf(\"%-" + len + "s\",\"" + str + "\\t\");\n");
		}
		writer.print("\t\tSystem.out.println();\n");
		writer.print("\t\tSystem.out.printf(\"");
		for (String str : mainClass.getSelectAttributes()) {
			len = str.length();
			for (int i = 0; i < len; i++) {
				writer.print("=");
			}
			writer.print("\\t");
		}
		writer.print(" \");\n");
		writer.print("\t\tfor(Result result: resultTable){\n");
		writer.print("\t\t\tSystem.out.printf(\"\\n\");\n");
		for (String str : mainClass.getSelectAttributes()) {
			for (String str1 : mainClass.getGroupAttributes()) {
				if (str.equals(str1)) {
					len = str.length();
					if (str.equals("month") || str.equals("year") || str.equals("days") || str.equals("quant")) {
						writer.print("\t\t\tSystem.out.printf(\"%" + len + "s\\t\", result." + str + ");\n");
					} else {
						writer.print("\t\t\tSystem.out.printf(\"%-" + len + "s\\t\", result." + str + ");\n");
					}
				}
			}
			for (VariableBuilder fVect : mainClass.getFVect()) {
				if (str.equals(fVect.getString())) {
					len = str.length();
					writer.print("\t\t\tSystem.out.printf(\"%" + len + "s\\t\", result." + str + ");\n");
				}
			}
		}
		writer.print("\t\t}\n");
		writer.print("\t}\n");
	}

	public static void generateUtilityMethods(PrintWriter writer) {
		writer.print("\tpublic boolean compare(String str1, String str2){\n");
		writer.print("\t\treturn str1.equals(str2);\n\t}\n");
		writer.print("\tpublic boolean compare(int num1, int num2){\n");
		writer.print("\t\treturn (num1 == num2);\n\t}\n");
	}
}
