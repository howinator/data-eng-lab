package datalab;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Created with loving care by howie on 2/23/17.
 */
class RandomData {
    private Integer numElements;

    RandomData(Integer numEle) {
        numElements = numEle;
    }

    ArrayList<Integer> generateColumn(Integer minValue, Integer maxValue) {
        ArrayList<Integer> colValues = new ArrayList<>(numElements);
        for (int i = 0; i < numElements; i++) {
            colValues.add(i % maxValue + minValue);
        }
        Collections.shuffle(colValues);
        return colValues;
    }
    ArrayList<String> generateText() {
        int eleInField = 240;
        char swapChar;
        LinkedList<Character> masterString;
        ArrayList<String> intermediateStrings = new ArrayList<>(eleInField);
        ArrayList<String> column = new ArrayList<>(numElements);
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
