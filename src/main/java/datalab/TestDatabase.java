package datalab;

import java.sql.SQLException;
import java.sql.*;
import java.util.*;

/**
 * Created with loving care by howie on 2/25/17.
 */
class TestDatabase {

    private Connection connection;
    private Integer numberRows, numTests;
    private String columnAName, columnBName, noIndex;
    private Map<String, ArrayList<Integer>> testSets;

    TestDatabase(Integer numRows, String colAName, String colBName, Map<String, ArrayList<Integer>> tests, String noIdx) {

        numberRows = numRows;
        columnAName = colAName;
        columnBName = colBName;
        noIndex = noIdx;
        testSets = tests;
        numTests = testSets.keySet().stream().map(p -> testSets.get(p)).map(p -> p.size()).reduce((a, b) -> Math.min(a, b)).get();
    }

    Map<String, ArrayList<Long>> runOneGeneratorTests(DataToInsert data) throws SQLException {
        // resultSet : {'physical organization': [times for each query]}
        Map<String, ArrayList<Long>> resultSet = new HashMap<>();
        ArrayList<Long> oneOrgResults = new ArrayList<>(4);
        Long insertTime;

        resetDatabase();
        insertTime = loadRows(data);
        oneOrgResults.add(insertTime);
        oneOrgResults.addAll(runThreeQueries());
        resultSet.put(noIndex, new ArrayList<>(oneOrgResults));
        oneOrgResults.clear();

        resetDatabase();
        createSingleConfig(columnAName);
        insertTime = loadRows(data);
        oneOrgResults.add(insertTime);
        oneOrgResults.addAll(runThreeQueries());
        resultSet.put(columnAName, new ArrayList<>(oneOrgResults));
        oneOrgResults.clear();

        resetDatabase();
        createSingleConfig(columnBName);
        insertTime = loadRows(data);
        oneOrgResults.add(insertTime);
        oneOrgResults.addAll(runThreeQueries());
        resultSet.put(columnBName, new ArrayList<>(oneOrgResults));
        oneOrgResults.clear();

        resetDatabase();
        createBothConfig();
        insertTime = loadRows(data);
        oneOrgResults.add(insertTime);
        oneOrgResults.addAll(runThreeQueries());
        resultSet.put(columnAName + columnBName, new ArrayList<>(oneOrgResults));
        oneOrgResults.clear();

        return resultSet;
    }

    private ArrayList<Long> runThreeQueries() throws SQLException {
        ArrayList<Long> testResults = new ArrayList<>(numTests);
        Map<String, ArrayList<Integer>> testCase = new HashMap<>();
        testCase.put(columnAName, testSets.get(columnAName));
        testResults.add(testOneQuery(testCase));
        testCase.clear();
        testCase.put(columnBName, testSets.get(columnBName));
        testResults.add(testOneQuery(testCase));
        testCase.clear();
        testCase.put(columnAName, testSets.get("testSetC1"));
        testCase.put(columnBName, testSets.get("testSetC2"));
        testResults.add(testOneQuery(testCase));
        testCase.clear();

        return testResults;
    }

    private Long testOneQuery(Map<String, ArrayList<Integer>> testCases) throws SQLException {
        String queryStr;
        ArrayList<String> cols = new ArrayList<>(1);
        if (testCases.size() == 1) {
            // implicit assumption that the name of the key is the name of the column
            cols.add(testCases.keySet().iterator().next());
            queryStr = "SELECT * FROM benchmark WHERE benchmark." + cols.get(0) + "= ?;";
        } else {
            cols.add(columnAName);
            cols.add(columnBName);
            queryStr = "SELECT * FROM benchmark WHERE benchmark." + columnAName + " = ? AND benchmark." + columnBName + " = ?;";
        }
        long avgTime = 0;
        long numberTests = (long) numTests;
        long startTime, endTime;
        int queryParamPos;
        PreparedStatement stmt = connection.prepareStatement(queryStr);

        for (int testNum = 0; testNum < numTests; testNum++) {
            for (queryParamPos = 0; queryParamPos < testCases.size(); queryParamPos++) {
                stmt.setInt(queryParamPos + 1, testCases.get(cols.get(queryParamPos)).get(testNum));
            }
            startTime = System.currentTimeMillis();
            stmt.executeQuery();
            endTime = System.currentTimeMillis();
            avgTime += (endTime - startTime);
        }
        avgTime /= numberTests;
        return avgTime;

    }

    private Long loadRows(DataToInsert data) throws SQLException {
        String baseQuery;
        baseQuery = "INSERT INTO benchmark (theKey, " + columnAName + ", " + columnBName + ", filler) VALUES (?, ?, ?, ?);";
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
        long timeTaken = endTime - startTime;
        System.out.println(variationName + " took " + (timeTaken / 1000.0) + " seconds.");
        connection.setAutoCommit(true);

        return timeTaken;
    }

    private void createSingleConfig(String colName) throws SQLException {
        String indexStr;
        Statement indexStmt;
        indexStr = "CREATE INDEX " + colName + "_idx ON benchmark (" + colName + ");";
        indexStmt = connection.createStatement();
        indexStmt.executeUpdate(indexStr);
    }

    private void createBothConfig() throws SQLException {
        createSingleConfig(columnAName);
        createSingleConfig(columnBName);
    }

    void resetDatabase() throws SQLException {
        resetSchema();
        createTable();
    }

    void closeCon() throws SQLException {
        connection.close();
    }

    void openCon() throws SQLException {
        // I know to not normally code db credentials into my code
        connection = DriverManager.getConnection("jdbc:postgresql://localhost:32768/postgres", "postgres", "postgres");
    }

    private void createTable() throws SQLException {
        String createStr;
        Statement createStatement;
        createStr = "CREATE TABLE benchmark (theKey INTEGER PRIMARY KEY, " + columnAName + " INTEGER, " + columnBName + " INTEGER, filler CHAR(247));";

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
