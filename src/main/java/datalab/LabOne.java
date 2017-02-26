package datalab;
import java.sql.*;
import java.util.*;

/**
 * Created with loving care by howie on 2/21/17.
 */

public class LabOne {

    public static void main(String[] argv) {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return;
        }

        Integer numRows = 5000000;
        Integer minColValue = 1;
        Integer maxColValue = 50000;
        String columnAName = "columnA";
        String columnBName = "columnB";
        String noIndex = "noIndex";
        if (numRows < maxColValue) {
            maxColValue = numRows;
        }
        Integer numTests = 3;
        ArrayList<Integer> testSetA = generateTestCases(numTests, minColValue, maxColValue);
        ArrayList<Integer> testSetB = generateTestCases(numTests, minColValue, maxColValue);
        Map<String, ArrayList<Integer>> fullTestSet = new HashMap<>();
        fullTestSet.put(columnAName, testSetA);
        fullTestSet.put(columnBName, testSetB);

        TestDatabase tdb = new TestDatabase(numRows, columnAName, columnBName, fullTestSet, noIndex);

        try {
            tdb.openCon();
            tdb.resetDatabase();

            VariationOne firstVariationData = new VariationOne(numRows, minColValue, maxColValue);
            VariationTwo secondVariationData = new VariationTwo(numRows, minColValue, maxColValue);

            tdb.loadRows(firstVariationData);
            tdb.resetDatabase();
            tdb.loadRows(secondVariationData);


            Map<String, ArrayList<Long>> testResultsLoadOne, testResultsLoadTwo = new HashMap<>();
            testResultsLoadOne = tdb.runOneGeneratorTests(firstVariationData);
            testResultsLoadTwo = tdb.runOneGeneratorTests(secondVariationData);
            formLatexRows(testResultsLoadOne, testResultsLoadTwo);


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                tdb.closeCon();
            } catch (SQLException se) {
            }
        }

    }

    private static void formLatexRows(Map<String, ArrayList<Long>> rs1, Map<String, ArrayList<Long>> rs2) {
       ArrayList<Long> latexRow = new ArrayList<Long>();
       Integer numQueryTypes = 3;
       Integer physOrgNum = 1;
       for (String key : rs1.keySet()) {
           for (int i = 0; i < numQueryTypes; i++) {
               latexRow.add(rs1.get(key).get(i));
               latexRow.add(rs1.get(key).get(i));
           }
           printLatexRow(latexRow, physOrgNum++);
           latexRow.clear();
       }
    }


    private static ArrayList<Integer> generateTestCases(Integer numTests, Integer minValue, Integer maxValue) {
        ArrayList<Integer> testCases = new ArrayList<>(numTests);
        Random rando = new Random();
        for (int i = 0; i < numTests; i++) {
            testCases.add(rando.nextInt(maxValue) + minValue);
        }
        return testCases;
    }


    private static void printLatexRow(ArrayList<Long> results, Integer runNumber) {
        String prefix, eleList, fullRow;
        eleList = "";
        prefix = "    \\multicolumn{2}{|r|}{" + runNumber.toString() + "} ";
        for (Long ele : results) {
            eleList = eleList + "& " + ele.toString() + " ";
        }
        fullRow = prefix + eleList + "\\\\\n \\hline";
        System.out.println(fullRow);
    }
}
