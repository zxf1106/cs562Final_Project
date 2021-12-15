
import java.io.*;
import java.util.*;

public class MainClass {
	// Key: column name, value: dataType of that column
	public static HashMap<String, String> dataType = new HashMap<>();

	// Declaring the DataStructures for six arguments in MFQuery
	private static List<String> selectAttributes = new ArrayList<>();
	private static int noGV;
	private static List<String> groupAttributes = new ArrayList<>();
	private static List<VariableBuilder> fVect = new ArrayList<>();
	private static List<String> suchThat = new ArrayList<>();
	private static String having = "";
	private static String whereCondition = "true";

	public static void main(String args[]) {
		MainClass mainClass = new MainClass();
		mainClass.connect();
		dataType = Schema.getSchema();
		System.out.println("The datatype of the given sales table: " + dataType);
		System.out.print("Please enter input option:\n" +
				"0 for terminal input \n" +
				"1 for file input\n");
		Scanner sc = new Scanner(System.in);
		String inputLine = sc.nextLine();

		if(Integer.parseInt(inputLine) == 0) {
			getTerminalInput(sc);
		} else {
			System.out.print("Please enter the path of your file: \n" +
					"sample input choice(copy one of the sample path or your own file path:)\n" +
					"input/MFQuery1.txt\n" +
					"input/MFQuery2.txt\n" +
					"input/MFQuery3.txt\n");
			String fileName = sc.nextLine().trim();
			File inputFile = new File(fileName);
			getFileInput(inputFile, sc);
		}
		GenerateMF.generateMF(dataType);
		System.out.println("Generation Successful");
	}

	// connect to the database
	private void connect() {
		try {
			Class.forName("org.postgresql.Driver");
			System.out.println("Driver has been connected successfully!");
		} catch (Exception e) {
			System.out.println("Failed to load the driver !");
			e.printStackTrace();
		}
	}

	public static void getTerminalInput(Scanner sc) {
		try{
			String input = "";
			System.out.println("select_attribute:");
			if(sc.hasNextLine()) {
				input = sc.nextLine();
				if(!input.equals("")) {
					buildSelectAttributes(input);
				}
			}
			System.out.println("no_gv:");
			if(sc.hasNextLine()) {
				input = sc.nextLine();
				noGV = (input.equals("") ? 0 : Integer.parseInt(input));
			}
			System.out.println("grouping_attributes:");
			if(sc.hasNextLine()) {
				input = sc.nextLine().trim();
				if(!input.equals(""))
					groupAttributes = Arrays.asList(input.split(", "));
			}
			System.out.println("where condition:");
			if(sc.hasNextLine()) {
				input = sc.nextLine().trim();
				buildWhereCondition(input);
			}
			System.out.println("fvect:");
			if(sc.hasNextLine()) {
				input = sc.nextLine().trim();
				if(!input.equals("")) {
					buildFVect(input);
				}
			}
			System.out.println("select_condition:");
			if(sc.hasNextLine()) {
				input = sc.nextLine().trim();
				if(!input.equals("")) {
					buildSuchThat(input, noGV);
				}
			}
			System.out.println("having_condition:");
			if(sc.hasNextLine()) {
				input = sc.nextLine().trim();
				if(!input.equals("")) {
					buildHaving(input);
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

	public static void getFileInput(File file, Scanner sc) {
		try {
			sc = new Scanner(file);
			String input = "";
			while (sc.hasNextLine()) {
				input = sc.nextLine();
				if (input.contains("select_attribute") && sc.hasNextLine()) {
					input = sc.nextLine().trim();
					buildSelectAttributes(input);
				} else if (input.contains("no_gv") && sc.hasNextLine()) {
					input = sc.nextLine().trim();
					noGV = (input.equals("") ? 0 : Integer.parseInt(input));
				} else if (input.contains("grouping_attributes") && sc.hasNextLine()) {
					input = sc.nextLine().trim();
					groupAttributes.addAll(Arrays.asList(input.split(", ")));
				} else if (input.contains("where") && sc.hasNextLine()) {
					input = sc.nextLine().trim();
					buildWhereCondition(input);
				} else if (input.contains("fvect") && sc.hasNextLine()) {
					input = sc.nextLine().trim();
					buildFVect(input);
				} else if (input.contains("select_condition:") && sc.hasNextLine()) {
					input = sc.nextLine().trim();
					buildSuchThat(input, noGV);
				} else if (input.contains("having_condition") && sc.hasNextLine()) {
					input = sc.nextLine().trim();
					buildHaving(input);
				}
			}
		} catch(Exception exception) {
			exception.printStackTrace();
		}
	}

	public static void buildSelectAttributes(String select_attributes) {
		String[] attributes = select_attributes.split(", ");
		for(String attribute : attributes) {
			if(attribute.contains("_")) {
				String[] values = attribute.split("_");
				VariableBuilder vb = new VariableBuilder(values[0], values[1], values[2]);
				attribute = vb.getString();
			}
			selectAttributes.add(attribute);
		}
	}

	public static void buildFVect(String f_vect) {
		String[] vectors = f_vect.split(", ");
		for(String vector : vectors) {
			String[] values = vector.split("_");
			VariableBuilder vb = new VariableBuilder(values[0], values[1], values[2]);
			fVect.add(vb);
		}
	}

	public static void buildSuchThat(String select_conditions, int no_gv) {
		if(no_gv == 0) {
			return;
		}
		String[] conditions = select_conditions.split(", ");
		for(int i = 1; i <= no_gv; i++) {
			List<String> such_that = new ArrayList<>();
			for(String condition : conditions) {
				String[] parts = condition.split("_");
				SuchThat vb = new SuchThat(Integer.parseInt(parts[0]), parts[1]);
				if(vb.getIndex() == i) {
					such_that.add(condition);
				}
			}
			StringJoiner joiner = new StringJoiner(" && ");
			for(String str : such_that) {
				String[] temp = str.split("_");
				str = "currRow." + temp[1];
				if(str.contains("==") || str.contains(">=") || str.contains("<=") || str.contains("!=")) {
					str = str;
				} else if(str.contains("=")) {
					str = str.replace("=", "==");
				}

				if (str.contains("prod==") || str.contains("state==") || str.contains("cust==")) {
					String[] value = str.split("==");
					str = value[0] + ".equals(" + value[1] + ")";
				}else if (str.contains("prod!=") || str.contains("state!=") || str.contains("cust!=")) {
					String[] value = str.split("!=");
					str = "!" + value[0] + ".equals(" + value[1] + ")";
				}
				joiner.add(str);
			}
			suchThat.add(joiner.toString());
		}
	}


	public static void buildHaving(String having_condition) {
		String[] havings = having_condition.split(" ");
		StringJoiner joiner = new StringJoiner(" ");
		for(String str : havings) {
			if(str.contains("_")) {
				String[] components = str.split("_");
				VariableBuilder vb = new VariableBuilder(components[0], components[1], components[2]);
				str = vb.getString();
			}
			if(str.equals("and")) {
				joiner.add("&&");
			} else if(str.equals("or")) {
				joiner.add("||");
			} else if(str.equals("==") || str.equals(">=") || str.equals("<=") || str.equals("!=")) {
				joiner.add(str);
			} else if(str.equals("=")) {
				joiner.add("==");
			} else{
				joiner.add(str);
			}
		}
		having = joiner.toString();
	}

	public static void buildWhereCondition(String where_condition) {
		if(where_condition.equals("")) {
			return;
		}
		String[]wheres = where_condition.split(", ");
		StringJoiner joiner = new StringJoiner(" && ");
		for(String where : wheres) {
			where = where.replaceAll(" ", "");
			if(where.contains("==") || where.contains(">=") || where.contains("<=") || where.contains("!=")) {
				where = where;
			} else if(where.contains("=")) {
				where = where.replace("=", "==");
			}

			if (where.contains("prod==") || where.contains("state==") || where.contains("cust==")) {
				String[] value = where.split("==");
				where = "currRow." + value[0] + ".equals(" + value[1] + ")";
			}else if (where.contains("prod!=") || where.contains("state!=") || where.contains("cust!=")) {
				String[] value = where.split("!=");
				where = "!currRow." + value[0] + ".equals(" + value[1] + "\")";
			} else {
				where = "currRow." + where;
			}
			joiner.add(where);
		}
		whereCondition = joiner.toString();
	}

	public List<String> getSelectAttributes() {
		return selectAttributes;
	}

	public int getNoGV() {
		return noGV;
	}

	public List<String> getGroupAttributes() {
		return groupAttributes;
	}

	public List<VariableBuilder> getFVect() {
		return fVect;
	}

	public List<String> getSuchThat() {
		return suchThat;
	}

	public String getHaving() {
		return having;
	}

	public String getWhere() {
		return whereCondition;
	}
}

class VariableBuilder {
	public String index;
	public String aggregate;
	public String attribute;

	VariableBuilder(String index, String aggregate, String attribute) {
		this.index = index;
		this.aggregate = aggregate;
		this.attribute = attribute;
	}
	public String getString() {
		return aggregate + "_" + attribute + "_" + index;
	}
}

class SuchThat {
	public int index;
	public String attribute;
	public SuchThat(int index, String attribute) {
		this.index = index;
		this.attribute = attribute;
	}
	public int getIndex() {
		return index;
	}
	public String getAttribute() {
		return attribute;
	}
}
