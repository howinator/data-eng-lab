package datalab;
import java.sql.*;
import java.util.ArrayList;

/**
 * Created by howie on 2/21/17.
 */

public class LabOne {

    public static void main(String[] argv){
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        Connection connection = null;

        try {
            // I know to not normally code db credentials into my code
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:32768/postgres", "postgres", "postgres");
            Integer numRows = 5000000;
            resetDatabase(connection);
            createTable(connection);

            VariationOne firstVariationData = new VariationOne(numRows);
            VariationTwo secondVariationData = new VariationTwo(numRows);

            connection.setAutoCommit(false);

            loadRows(firstVariationData, connection, numRows);

            resetDatabase(connection);
            createTable(connection);

            loadRows(secondVariationData, connection, numRows);


        } catch (SQLException e){
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException se) {
            }
        }

    }

    private static void printLatexRow(ArrayList<Integer> results, Integer runNumber){
        String prefix, eleList, fullRow;
        eleList = "";
        prefix = "    \\multicolumn{2}{|r|}{" + runNumber.toString() + "} ";
        for (Integer ele : results) {
            eleList = "& " + ele.toString() + " ";
        }
        fullRow = prefix + eleList + "\\\\\n \\hline";
        System.out.print(fullRow);
    }

    private static void loadRows(DataToInsert data, Connection con, Integer numberRows) {
        String baseQuery;
        baseQuery = "INSERT INTO benchmark (theKey, columnA, columnB, filler) VALUES (?, ?, ?, ?);";
        String variationName = data.getClass().getName();
        try {
            PreparedStatement stmt = con.prepareStatement(baseQuery);
            for (int i = 0; i < numberRows; i++) {
                stmt.setInt(1, data.getPrimaryKey().get(i));
                stmt.setInt(2, data.getColumnA().get(i));
                stmt.setInt(3, data.getColumnB().get(i));
                stmt.setString(4, data.getTextColumn().get(i));
                stmt.addBatch();
            }
            long startTime = System.currentTimeMillis();
            stmt.executeBatch();
            long endTime = System.currentTimeMillis();
            System.out.println(variationName + " took " + ((endTime - startTime) / 1000) + " seconds.");
        } catch (SQLException e) {
        }
    }

    private static void createTable(Connection con) {
        String createStr;
        Statement createStatement;
        createStr = "CREATE TABLE benchmark (theKey INTEGER PRIMARY KEY, columnA INTEGER, columnB INTEGER, filler CHAR(247));";

        try {
           createStatement = con.createStatement();
           createStatement.executeUpdate(createStr);

        } catch (SQLException e) {
            if (e.getMessage().contains("relation \"benchmark\" already exists")) {
                System.out.println("relation already exists");
            } else {
                e.printStackTrace();
            }
        }
    }

    private static void resetDatabase(Connection con) {
        String dropStr;
        Statement dropStatement;
        dropStr = "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public to postgres; GRANT ALL ON SCHEMA public to public;";
        try {
            dropStatement = con.createStatement();
            dropStatement.executeUpdate(dropStr);
        } catch (SQLException e) {
            System.out.println("Could not drop public schema");
            e.printStackTrace();
        }
    }
}
