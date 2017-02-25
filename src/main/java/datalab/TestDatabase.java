package datalab;

import java.sql.SQLException;
import java.sql.*;

/**
 * Created with loving care by howie on 2/25/17.
 */
public class TestDatabase {

    private Connection connection;
    private Integer numberRows;

    public TestDatabase(Integer numRows) {

        numberRows = numRows;
    }

    public void loadRows(DataToInsert data) throws SQLException {
        String baseQuery;
        baseQuery = "INSERT INTO benchmark (theKey, columnA, columnB, filler) VALUES (?, ?, ?, ?);";
        String variationName = data.getClass().getName();
        connection.setAutoCommit(false);
        PreparedStatement stmt = connection.prepareStatement(baseQuery);
        for (int i = 0; i < numberRows; i++) {
            stmt.setInt(1, data.getPrimaryKey().get(i));
            stmt.setInt(2, data.getColumnA().get(i));
            stmt.setInt(3, data.getColumnB().get(i));
            stmt.setString(4, data.getTextColumn().get(i));
            stmt.addBatch();
        }
        long startTime = System.currentTimeMillis();
        stmt.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println(variationName + " took " + ((endTime - startTime) / 1000.0) + " seconds.");
        connection.setAutoCommit(true);
    }

    public void createSingleConfig(String colName) throws SQLException {
        String indexStr;
        Statement indexStmt;
        indexStr = "CREATE INDEX " + colName + "_idx ON benchmark (" + colName + ");";
        indexStmt = connection.createStatement();
        indexStmt.executeUpdate(indexStr);
    }

    public void createBothConfig() throws SQLException {
        createSingleConfig("columnA");
        createSingleConfig("columnB");
    }

    public void resetDatabase() throws SQLException {
        resetSchema();
        createTable();
    }

    public void closeCon() throws SQLException {
        connection.close();
    }

    public void openCon() throws SQLException {
        // I know to not normally code db credentials into my code
        connection = DriverManager.getConnection("jdbc:postgresql://localhost:32768/postgres", "postgres", "postgres");
    }

    private void createTable() throws SQLException {
        String createStr;
        Statement createStatement;
        createStr = "CREATE TABLE benchmark (theKey INTEGER PRIMARY KEY, columnA INTEGER, columnB INTEGER, filler CHAR(247));";

        try {
            createStatement = connection.createStatement();
            createStatement.executeUpdate(createStr);

        } catch (SQLException e) {
            if (e.getMessage().contains("relation \"benchmark\" already exists")) {
                System.out.println("relation already exists");
            } else {
                throw e;
            }
        }
    }

    private void resetSchema() throws SQLException {
        String dropStr;
        Statement dropStatement;
        dropStr = "DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public to postgres; GRANT ALL ON SCHEMA public to public;";
        dropStatement = connection.createStatement();
        dropStatement.executeUpdate(dropStr);
    }
}
