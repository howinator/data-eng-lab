package datalab;
import java.util.ArrayList;

/**
 * Created with loving care by howie on 2/22/17.
 */
public class VariationOne implements DataToInsert {
    private ArrayList<Integer> primaryKey, columnA, columnB;
    private ArrayList<String> textColumn;
    private Integer numElements;
    private RandomData randomData;

    public VariationOne(Integer numEle, Integer minColValue, Integer maxColValue){
        numElements = numEle;
        randomData = new RandomData(numElements);
        primaryKey = generatePrimaryKey();
        columnA = randomData.generateColumn(minColValue, maxColValue);
        columnB = randomData.generateColumn(minColValue, maxColValue);
        textColumn = randomData.generateText();
    }

    public ArrayList<Integer> generatePrimaryKey() {
        ArrayList<Integer> key = new ArrayList<Integer>(numElements);
        for (int i = 0; i < numElements; i++){
            key.add(i);
        }
        return key;
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
