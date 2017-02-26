package datalab;

import java.sql.SQLException;
import java.sql.*;
import java.util.*;

/**
 * Created with loving care by howie on 2/25/17.
 */
public class TestDatabase {

    private Connection connection;
    private Integer numberRows, numTests;
    private String columnAName, columnBName, noIndex;
    private Map<String, ArrayList<Integer>> testSets;

    public TestDatabase(Integer numRows, String colAName, String colBName, Map<String, ArrayList<Integer>> tests, String noIdx) {

        numberRows = numRows;
        columnAName = colAName;
        columnBName = colBName;
        noIndex = noIdx;
        testSets = tests;
        numTests = testSets.keySet().stream().map(p -> testSets.get(p)).map(p -> p.size()).reduce((a, b) -> Math.min(a, b)).get();
    }

    public Map<String, ArrayList<Long>> runOneGeneratorTests(DataToInsert data) throws SQLException {
        // resultSet : {'physical organization': [times for each query]}
        Map<String, ArrayList<Long>> resultSet = new HashMap<>();

        resetDatabase();
        loadRows(data);
        resultSet.put(noIndex, runThreeQueries());

        resetDatabase();
        loadRows(data);
        createSingleConfig(columnAName);
        resultSet.put(columnAName, runThreeQueries());

        resetDatabase();
        loadRows(data);
        createSingleConfig(columnBName);
        resultSet.put(columnBName, runThreeQueries());

        resetDatabase();
        loadRows(data);
        createBothConfig();
        resultSet.put(columnAName + columnBName, runThreeQueries());

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
        testResults.add(testOneQuery(testSets));

        return testResults;
    }

    private Long testOneQuery(Map<String, ArrayList<Integer>> testCases) throws SQLException {
        String queryStr;
        ArrayList<String> cols = new ArrayList<>(1);
        if (testCases.size() == 1) {
            cols.add(testCases.keySet().iterator().next());
            queryStr = "SELECT * FROM benchmark WHERE benchmark." + cols.get(0) + "= ?;";
        } else {
            cols.add(columnAName);
            cols.add(columnBName);
            queryStr = "SELECT * FROM benchmark WHERE benchmark." + columnAName + " = ? AND benchmark." + columnBName + " = ?;";
        }
        long avgTime = 0;
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
        avgTime /= numTests;
        return avgTime;

    }

//    public Long runBiVarQuery(ArrayList<Integer> testCasesA, ArrayList<Integer> testCasesB) throws SQLException {
//        String queryStr = "SELECT * FROM benchmark WHERE benchmark.columnA = ? AND benchmark.columnB = ?;";
//        long avgTime = 0;
//        PreparedStatement stmt =  connection.prepareStatement(queryStr);
//        long startTime, endTime;
//        Integer minSize = Math.min(testCasesA.size(), testCasesB.size());
//        for (int i = 0; i < minSize; i++) {
//            stmt.setInt(1, testCasesA.get(i));
//            stmt.setInt(2, testCasesB.get(i));
//            startTime = System.currentTimeMillis();
//            stmt.executeQuery();
//            endTime = System.currentTimeMillis();
//            avgTime += (endTime - startTime);
//        }
//        avgTime /= minSize;
//
//        return avgTime;
//    }
//
//    public Long runUniVarQuery(ArrayList<Integer> testCases, String column) throws SQLException {
//        String queryStr = "SELECT * FROM benchmark WHERE benchmark." + column + "= ?;";
//        long avgTime = 0;
//        PreparedStatement stmt = connection.prepareStatement(queryStr);
//        long startTime, endTime;
//        for (Integer ele : testCases) {
//            stmt.setInt(1, ele);
//            startTime = System.currentTimeMillis();
//            stmt.executeQuery();
//            endTime = System.currentTimeMillis();
//            avgTime += (endTime - startTime);
//        }
//        avgTime /= testCases.size();
//
//        return avgTime;
//    }

    public void loadRows(DataToInsert data) throws SQLException {
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
        createSingleConfig(columnAName);
        createSingleConfig(columnBName);
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
