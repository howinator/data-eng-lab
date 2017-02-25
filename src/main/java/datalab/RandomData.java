package datalab;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created by howie on 2/23/17.
 */
public class RandomData {
    private Integer numElements;

    public RandomData() {
        numElements = 5000000;
    }

    public ArrayList<Integer> generateColumn() {
        ArrayList<Integer> colValues = new ArrayList<Integer>(numElements);
        Integer maxValue = 50000;
        Integer minValue = 1;
        Random rando = new Random();
        for (int i = 0; i < numElements; i++) {
            colValues.add(rando.nextInt(maxValue) + minValue);
        }
        return colValues;
    }
    public ArrayList<String> generateText() {
        int eleInField = 240;
        char character, swapChar;
        String str;
        LinkedList<Character> masterString = new LinkedList<Character>();
        ArrayList<String> intermediateStrings = new ArrayList<String>(eleInField);
        ArrayList<String> column = new ArrayList<String>(numElements);
        masterString = IntStream.range(0, eleInField).boxed()
                .map(i -> i % 127)
                .map(i -> ((i < 33) ? i + 33 : i ))
                .map(c -> (char) (int) c)
                .collect(Collectors.toCollection(LinkedList::new));
        for (int j = 0; j < eleInField; j++) {
            intermediateStrings.add(masterString.stream().map(e->e.toString()).reduce((acc, e) -> acc + e).get());
            swapChar = masterString.removeFirst();
            masterString.add(swapChar);
        }
        for(int i = 0; i < numElements; i ++) {
            column.add(intermediateStrings.get(i % eleInField));
        }
        return column;
    }
}
