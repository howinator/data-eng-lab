package datalab;
import java.sql.*;
import java.util.ArrayList;
import java.util.Random;

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

        Integer numRows = 100000;
        Integer minColValue = 1;
        Integer maxColValue = 50000;
        TestDatabase tdb = new TestDatabase(numRows);

        try {
            tdb.openCon();
            tdb.resetDatabase();

            VariationOne firstVariationData = new VariationOne(numRows, minColValue, maxColValue);
            VariationTwo secondVariationData = new VariationTwo(numRows, minColValue, maxColValue);

            tdb.loadRows(firstVariationData);
            tdb.resetDatabase();
            tdb.loadRows(secondVariationData);

            Integer numRuns = 5;
            ArrayList<Integer> testCases = generateTestCase(numRuns, minColValue, maxColValue);


        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                tdb.closeCon();
            } catch (SQLException se) {
            }
        }

    }

    private static ArrayList<Integer> generateTestCase(Integer numTests, Integer minValue, Integer maxValue) {
        ArrayList<Integer> testCases = new ArrayList<>(numTests);
        Random rando = new Random();
        for (int i = 0; i < numTests; i++) {
            testCases.add(rando.nextInt(maxValue) + minValue);
        }
        return testCases;
    }


    private static void printLatexRow(ArrayList<Integer> results, Integer runNumber) {
        String prefix, eleList, fullRow;
        eleList = "";
        prefix = "    \\multicolumn{2}{|r|}{" + runNumber.toString() + "} ";
        for (Integer ele : results) {
            eleList = "& " + ele.toString() + " ";
        }
        fullRow = prefix + eleList + "\\\\\n \\hline";
        System.out.print(fullRow);
    }
}
