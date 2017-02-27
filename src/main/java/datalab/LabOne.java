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
        Long fullProgStartTIme = System.currentTimeMillis();
        // Important global variables
        Integer numRows = 5000000;
        Integer numTests = 10;

        Integer minColValue = 1;
        Integer maxColValue = 50000;
        String columnAName = "columnA";
        String columnBName = "columnB";
        String noIndex = "noIndex";
        if (numRows < maxColValue) {
            maxColValue = numRows;
        }
        ArrayList<Integer> testSetA = generateTestCases(numTests, minColValue, maxColValue);
        ArrayList<Integer> testSetB = generateTestCases(numTests, minColValue, maxColValue);
        ArrayList<Integer> testSetC1 = generateTestCases(numTests, minColValue, maxColValue);
        ArrayList<Integer> testSetC2 = generateTestCases(numTests, minColValue, maxColValue);
        System.out.println("TestSetA" + testSetA);
        System.out.println("TestSetB" + testSetB);
        Map<String, ArrayList<Integer>> fullTestSet = new HashMap<>();
        fullTestSet.put(columnAName, testSetA);
        fullTestSet.put(columnBName, testSetB);
        fullTestSet.put("testSetC1", testSetC1);
        fullTestSet.put("testSetC2", testSetC2);

        TestDatabase tdb = new TestDatabase(numRows, columnAName, columnBName, fullTestSet, noIndex);

        try {
            tdb.openCon();
            tdb.resetDatabase();

            VariationOne firstVariationData = new VariationOne(numRows, minColValue, maxColValue);
            VariationTwo secondVariationData = new VariationTwo(numRows, minColValue, maxColValue);

            Map<String, ArrayList<Long>> testResultsLoadOne, testResultsLoadTwo;
            testResultsLoadOne = tdb.runOneGeneratorTests(firstVariationData);
            testResultsLoadTwo = tdb.runOneGeneratorTests(secondVariationData);
            formLatexRows(testResultsLoadOne, testResultsLoadTwo, noIndex);

            Long fullProgEndTime = System.currentTimeMillis();
            System.out.println("Full program run time was " + (fullProgEndTime - fullProgStartTIme)/(1000.0*60.0) + " minutes.");


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                tdb.closeCon();
            } catch (SQLException ignored) {
            }
        }

    }

    private static void formLatexRows(Map<String, ArrayList<Long>> rs1, Map<String, ArrayList<Long>> rs2, String noIdx) {
       ArrayList<Long> rawLatexRow = new ArrayList<>();
       Long rs1EleRaw, rs2EleRaw;
       Integer numQueryTypes = 4;
       for (String key : rs1.keySet()) {
           for (int i = 0; i < numQueryTypes; i++) {
               rs1EleRaw = rs1.get(key).get(i);
               rs2EleRaw = rs2.get(key).get(i);
               rawLatexRow.add(rs1EleRaw);
               rawLatexRow.add(rs2EleRaw);
           }
           System.out.println(key + " Data: " +rawLatexRow);
           printLatexRow(rawLatexRow, key);
           rawLatexRow.clear();
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


    private static void printLatexRow(ArrayList<Long> results, String physOrg) {
        String prefix, eleList, fullRow;
        eleList = "";
        prefix = "    \\multicolumn{2}{|r||}{" + physOrg + "} ";
        for (Long ele : results) {
            eleList = eleList + "& " + ele.toString() + " ";
        }
        fullRow = prefix + eleList + "\\\\\n \\hline";
        System.out.println(fullRow);
    }
}
