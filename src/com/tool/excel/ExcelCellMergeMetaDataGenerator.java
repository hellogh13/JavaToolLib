package com.tool.excel;

import org.apache.commons.collections4.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by peterchen on 20/11/2018.
 */
@FunctionalInterface
public interface ExcelCellMergeMetaDataGenerator<T, R> {

    R generate(T t);

}

class DefaultExcelCellMergeMetaDataGenerator<T> implements ExcelCellMergeMetaDataGenerator<List<T>, Map<Integer, List<Pair<Integer, Integer>>>> {

    private Map<Integer, Function<T, String>> mergeColumns;

    DefaultExcelCellMergeMetaDataGenerator(Map<Integer, Function<T, String>> mergeColumns) {

        this.mergeColumns = mergeColumns;
    }

    @Override
    public Map<Integer, List<Pair<Integer, Integer>>> generate(List<T> datas) {

        Map<Integer, List<Pair<Integer, Integer>>> mergeMetaDataMap = new HashMap<>(mergeColumns.size());
        mergeColumns.forEach((k, v) -> mergeMetaDataMap.put(k, this.assembleMergeMetaData(datas, v)));
        return mergeMetaDataMap;
    }

    private List<Pair<Integer, Integer>> assembleMergeMetaData(List<T> datas, Function<T, String> classifier) {

        Map<String, Integer> dataMapByCamp = datas.stream().collect(Collectors.groupingBy(classifier, LinkedHashMap::new, Collectors.reducing(0, e -> 1, Integer::sum)));
        Collection<Integer> dataSizes = dataMapByCamp.values();
        int size = dataSizes.size();
        List<Pair<Integer, Integer>> mergeMetaDatas = new ArrayList<>(size);
        int index = 0;
        for (Integer dataSize : dataSizes) {
            constructMergeMetaData(mergeMetaDatas, index, dataSize);
            index++;
        }
        this.removeUnnecessaryMerge(mergeMetaDatas);
        return mergeMetaDatas;
    }

    private void removeUnnecessaryMerge(List<Pair<Integer, Integer>> mergeMetaDatas) {

        List<Pair<Integer, Integer>> unnecessaryMerges = mergeMetaDatas.parallelStream().filter(x -> x.getFirst().equals(x.getSecond())).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(unnecessaryMerges)) {
            mergeMetaDatas.removeAll(unnecessaryMerges);
        }
    }

    private void constructMergeMetaData(List<Pair<Integer, Integer>> mergeMetaDatas, int index, Integer dataSize) {

        int start = this.calMergeStart(index, mergeMetaDatas);
        int end = this.calMergeEnd(start, dataSize);
        mergeMetaDatas.add(new Pair<>(start, end));
    }

    private int calMergeEnd(int start, int dataSize) {

        return start + dataSize - 1;
    }

    private int calMergeStart(int index, List<Pair<Integer, Integer>> mergeMetaDatas) {

        if (index == 0) {
            return 0;
        } else {
            return mergeMetaDatas.get(index - 1).getSecond() + 1;
        }
    }
}



