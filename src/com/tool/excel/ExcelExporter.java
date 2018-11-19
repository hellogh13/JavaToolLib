package com.tool.excel;


import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by peterchen on 02/11/2018.
 */
@Slf4j
public class ExcelExporter<Data> extends ExcelExporterV2<Data> {

    @Getter
    private Map<String, List<Data>> dataByJobs;

    public ExcelExporter(Map<String, List<Data>> dataByJobs, Set<String> hiddenColumns, Locale locale) {

        super(dataByJobs.get(JobCdEm.CDM.getCode()), JobCdEm.CDM.name(), null, hiddenColumns, locale);
        this.dataByJobs = dataByJobs;
    }

    @Override
    public void callback() {

        try {
//			this.mergeDataCell(3, dataByJobs.get(JobCdEm.CDM.getCode()));
            this.drawCLSheet();
            this.drawCSSheet();
        } catch (Exception e) {
            log.error("error is occurred");
            throw new RuntimeException("error is happening", e);
        }
    }

    private void mergeDataCell(int startRowNum, List<Data> datas) {
    }

    private void drawCSSheet() throws Exception {

        this.setHiddenColumns(Collections.EMPTY_SET);
        this.drawSheet(JobCdEm.CS.name(), dataByJobs.get(JobCdEm.CS.getCode()), true, 2);
    }

    private void drawCLSheet() throws Exception {

        this.setHiddenColumns(Stream.of("dailyAverageOt").collect(Collectors.toSet()));
        this.drawSheet(JobCdEm.CL.name(), dataByJobs.get(JobCdEm.CL.getCode()), false, 1);
    }

    private void drawSheet(String name, List<Data> datas, boolean hasLegend, int mergeStartRow) throws Exception {

        if (CollectionUtils.isNotEmpty(datas)) {
            this.createNewSheet(name, datas);
            int rowNum = this.drawTitle(hasLegend, 0);
            rowNum = this.drawHead(rowNum);
            this.drawBody(rowNum);
            //		this.mergeDataCell(mergeStartRow, datas);
        }
    }

    private int drawHead(int rowNum) throws Exception {

        rowNum = this.drawGroupHeader(rowNum);
        return this.drawSubHeader(rowNum);
    }

    private int drawTitle(boolean hasLegend, int startRowNum) {

        if (hasLegend) {
            return this.drawTitleLine(startRowNum);
        }
        return 1;
    }

    private void createNewSheet(String name, List<Data> datas) {

        sheet = wb.createSheet(name);
        this.sheetData = datas;
    }

    @Override
    protected int drawTitleLine(int rowNum) {

        HSSFRow titleRow = sheet.createRow(rowNum);
        int colNum = 0;
        String desc = MessageSourceService.getMessage("workingTime.Daily_average_ot_hours_less_than_two", this.locale);
        colNum = this.drawLegend(titleRow, rowNum, colNum, OTColor.GREEN.getColor(), desc);
        desc = MessageSourceService.getMessage("workingTime.Daily_average_ot_hours_between_two_and_twoHalf", this.locale);
        colNum = this.drawLegend(titleRow, rowNum, colNum, OTColor.YELLOW.getColor(), desc);
        desc = MessageSourceService.getMessage("workingTime.Daily_average_ot_hours_more_than_twoHalf", this.locale);
        this.drawLegend(titleRow, rowNum, colNum, OTColor.RED.getColor(), desc);
        return rowNum + 2;
    }

    private int drawLegend(HSSFRow titleRow, int rowNum, int colNum, short color, String desc) {

        fillColor(titleRow, colNum, color);
        int endCol = drawDesc(titleRow, rowNum, colNum, desc);
        return endCol + 1;
    }

    private int drawDesc(HSSFRow titleRow, int rowNum, int colNum, String desc) {

        colNum = colNum + 1;
        HSSFCell cell = titleRow.createCell(colNum);
        cell.setCellStyle(this.createDefaultBodyStyle());
        cell.setCellValue(desc);
        int endCol = colNum + desc.length() / 8;
        CellRangeAddress cra = new CellRangeAddress(rowNum, rowNum, colNum, endCol);
        setRegionBorder(1, cra, defaultBorderColor);
        sheet.addMergedRegion(cra);
        return endCol;
    }

    private void fillColor(HSSFRow titleRow, int colNum, short color) {

        HSSFCell cell = titleRow.createCell(colNum);
        cell.setCellStyle(this.createColorStyle(color));
    }

    @Override
    protected short findColor(Object obj) {

        float dailyAverageOt = (float) obj;
        return OTColor.getColorByOtTime(dailyAverageOt);
    }

    @Override
    protected Map<Integer, List<Pair<Integer, Integer>>> getMergeMetaData() {

        Map<Integer, List<Pair<Integer, Integer>>> mergeMetaDataMap = new HashMap<>(3);
        Arrays.stream(MergeColumn.values()).forEach(x -> {
            assembleMergeMetaData(mergeMetaDataMap, x.getColumn(), x.getClassifier());
        });
        return mergeMetaDataMap;
    }

    private void assembleMergeMetaData(Map<Integer, List<Pair<Integer, Integer>>> mergeMetaDataMap, int column, Function<Data, String> classifier) {

        Map<String, Integer> dataMapByCamp = this.allData.stream().collect(Collectors.groupingBy(classifier, LinkedHashMap::new, Collectors.reducing(0, e -> 1, Integer::sum)));
        Collection<Integer> dataSizes = dataMapByCamp.values();
        int size = dataSizes.size();
        List<Pair<Integer, Integer>> mergeMetaDatas = new ArrayList<>(size);
        int index = 0;
        for (Integer dataSize : dataSizes) {
            constructMergeMetaData(mergeMetaDatas, index, dataSize);
            index++;
        }
        this.removeUnnecessaryMerge(mergeMetaDatas);
        mergeMetaDataMap.put(column, mergeMetaDatas);
    }

    private void removeUnnecessaryMerge(List<Pair<Integer, Integer>> mergeMetaDatas) {

        List<Pair<Integer, Integer>> unnecessaryMerges = mergeMetaDatas.parallelStream().filter(x -> x.getFirst().equals(x.getSecond())).collect(Collectors.toList());
        if (CollectionUtils.isNotEmpty(unnecessaryMerges)) {
            mergeMetaDatas.removeAll(unnecessaryMerges);
        }
    }

    private void constructMergeMetaData(List<Pair<Integer, Integer>> mergeMetaDatas, int index, Integer dataSize) {

        int start = this.calMergeStart(index, mergeMetaDatas);
        int end = this.calMergeEnd(mergeMetaDatas, start, dataSize);
        mergeMetaDatas.add(new Pair<>(start, end));
    }

    private int calMergeEnd(List<Pair<Integer, Integer>> mergeMetaDatas, int start, int dataSize) {

        return start + dataSize - 1;
    }

    private int calMergeStart(int index, List<Pair<Integer, Integer>> mergeMetaDatas) {

        if (index == 0) {
            return 0;
        } else {
            return mergeMetaDatas.get(index - 1).getSecond() + 1;
        }
    }

    enum MergeColumn {

        REGION(0, Data::getRegionName), AREA(1, Data::getAreaName), CAMP(2, Data::getCampName);

        @Getter
        private final int column;
        @Getter
        private final Function<Data, String> classifier;

        MergeColumn(int column, Function<Data, String> classifier) {
            this.column = column;
            this.classifier = classifier;
        }
    }

    enum OTColor {

        GREEN(-1, 120, new HSSFColor.GREEN().getIndex()),
        YELLOW(120, 144, new HSSFColor.YELLOW().getIndex()),
        RED(144, 24 * 60, new HSSFColor.RED().getIndex());

        @Getter
        private int min;
        @Getter
        private int max;
        @Getter
        private short color;

        OTColor(int min, int max, short color) {

            this.min = min;
            this.max = max;
            this.color = color;
        }

        public static short getColorByOtTime(float otTime) {

            Optional<OTColor> otColor = Arrays.stream(OTColor.values()).filter(x -> otTime > x.getMin() && otTime <= x.getMax()).findAny();
            if (otColor.isPresent()) {
                return otColor.get().getColor();
            }
            return GREEN.getColor();
        }
    }

}
