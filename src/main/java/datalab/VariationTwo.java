package datalab;
import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by howie on 2/22/17.
 */
public class VariationTwo implements DataToInsert {
    private ArrayList<Integer> primaryKey, columnA, columnB;
    private ArrayList<String> textColumn;
    private Integer numElements;
    private RandomData randomData;

    public VariationTwo(Integer numEle){
        numElements = numEle;
        randomData = new RandomData();
        primaryKey = generatePrimaryKey();
        columnA = randomData.generateColumn();
        columnB = randomData.generateColumn();
        textColumn = randomData.generateText();
    }
    private ArrayList<Integer> generatePrimaryKey() {
        ArrayList<Integer> key = new ArrayList<Integer>(numElements);
        for (int i = 0; i < numElements; i++){
            key.add(i);
        }
        Collections.shuffle(key);
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
