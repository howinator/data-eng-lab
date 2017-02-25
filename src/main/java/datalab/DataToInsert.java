package datalab;

import java.util.ArrayList;

/**
 * Created with loving care by howie on 2/23/17.
 */
public interface DataToInsert {

    ArrayList<Integer> getPrimaryKey();
    ArrayList<Integer> getColumnA();
    ArrayList<Integer> getColumnB();
    ArrayList<String> getTextColumn();

}
