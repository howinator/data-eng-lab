package datalab;
import java.util.ArrayList;

/**
 * Created by howie on 2/22/17.
 */
public class VariationOne implements DataToInsert {
    private ArrayList<Integer> primaryKey, columnA, columnB;
    private ArrayList<String> textColumn;
    private Integer numElements;
    private RandomData randomData;

    public VariationOne(Integer numEle){
        numElements = numEle;
        randomData = new RandomData(numElements);
        primaryKey = randomData.generatePrimaryKey();
        columnA = randomData.generateColumn();
        columnB = randomData.generateColumn();
        textColumn = randomData.generateText();
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
