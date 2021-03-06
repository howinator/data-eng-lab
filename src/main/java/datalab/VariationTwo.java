package datalab;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created with loving care by howie on 2/22/17.
 */
public class VariationTwo implements DataToInsert {
    private ArrayList<Integer> primaryKey, columnA, columnB;
    private ArrayList<String> textColumn;
    private Integer numElements;

    VariationTwo(Integer numEle, Integer minColValue, Integer maxColValue){
        numElements = numEle;
        RandomData randomData = new RandomData(numElements);
        primaryKey = generatePrimaryKey();
        columnA = randomData.generateColumn(minColValue, maxColValue);
        columnB = randomData.generateColumn(minColValue, maxColValue);
        textColumn = randomData.generateText();

        assert primaryKey.size() == numElements;
        assert columnA.size() == primaryKey.size();
        assert columnB.size() == columnA.size();
        assert textColumn.size() == columnB.size();
    }

    private ArrayList<Integer> generatePrimaryKey() {
        Integer domainOfIntegers = 150000000;
        ArrayList<Integer> masterList = new ArrayList<>(domainOfIntegers);
        ArrayList<Integer> colValues = new ArrayList<>(numElements);
        for (int i = 0; i < domainOfIntegers; i++) {
            masterList.add(i);
        }
        Collections.shuffle(masterList);
        for (int j = 0; j < numElements; j++) {
            colValues.add(masterList.get(j));
        }
        masterList = null;
        System.gc();
        return colValues;
    }
    @Override
    public ArrayList<Integer> getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public ArrayList<Integer> getColumnA() {
        return columnA;
    }

    @Override
    public ArrayList<Integer> getColumnB() {
        return columnB;
    }

    @Override
    public ArrayList<String> getTextColumn() {
        return textColumn;
    }
}
