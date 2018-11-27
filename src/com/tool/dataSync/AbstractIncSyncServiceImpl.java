package com.tool.dataSync;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by peterchen on 30/07/2018.
 */
@Slf4j
public abstract class AbstractIncSyncServiceImpl<Source, Stage extends StageBase, Target, Log extends LogBase> {

    private static final DecimalFormat FLOAT_ROUND = new DecimalFormat("#.######");

    protected final ThreadLocal<String> tables = new ThreadLocal<>();

    @Transactional(value = "lmsHrTransactionManager", rollbackFor = Exception.class)
    public void syncData(List<Source> sourceDatas, List<Stage> stageDatas) {

        if (CollectionUtils.isEmpty(sourceDatas) && CollectionUtils.isEmpty(stageDatas)) {
            return;
        }
        tables.set(this.getClass().getSimpleName());
        Pair<List<Stage>, List<Stage>> persistedStageDatas = this.syncToStage(sourceDatas, stageDatas);
        this.syncToTarget(persistedStageDatas.getFirst(), persistedStageDatas.getSecond());
    }

    private Pair<List<Stage>, List<Stage>> syncToStage(List<Source> sourceDatas, List<Stage> stageDatas) {

        if (stageDatas == null) {
            stageDatas = new ArrayList<>();
        }
        List<Log> logs = new ArrayList<>();
        List<Stage> syncResult;
        if (CollectionUtils.isEmpty(sourceDatas)) {
            syncResult = stageDatas;
        } else {
            Map<Object, List<Stage>> stageUniqueMap = this.convertToUniqueObjMap(stageDatas);
            Set<String> properties = new HashSet<>();
            sourceDatas.forEach(x -> {
                this.syncToStageOneByOne(properties, x, stageUniqueMap, logs);
            });
            syncResult = stageUniqueMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
            this.validateSyncResult(syncResult, sourceDatas.size());
        }
        return this.persistToStage(syncResult, logs);
    }

    private void validateSyncResult(List<Stage> syncResult, int sourceSize) {

        int deleteSize = (int) syncResult.stream().filter(x -> SyncAction.DELETE.getCode() == x.getAction()).count();
        int liveSize = syncResult.size() - deleteSize;
        if (sourceSize != liveSize) {
            throw new RuntimeException("match is failed");
        }
    }

    private Pair<List<Stage>, List<Stage>> persistToStage(List<Stage> syncResult, List<Log> logs) {

        final List<Stage> insertAndUpdate = new ArrayList<>();
        final List<Stage> delete = new ArrayList<>();
        syncResult.forEach(x -> {
            if (SyncAction.DELETE.getCode() == x.getAction()) {
                log.info("delete:{}", x);
                delete.add(x);
                logs.add(this.convertToLogEntity(x));
            } else if (SyncAction.INSERT.getCode() == x.getAction() || SyncAction.UPDATE.getCode() == x.getAction()) {
                insertAndUpdate.add(x);
            }
        });
        if (CollectionUtils.isNotEmpty(insertAndUpdate)) {
            this.getStageJpaRepository().save(insertAndUpdate);
        }
        logs.addAll(insertAndUpdate.stream().filter(x -> SyncAction.INSERT.getCode() == x.getAction()).map(x -> this.convertToLogEntity(x)).collect(Collectors.toList()));
        this.logChange(logs);
        if (CollectionUtils.isNotEmpty(delete)) {
            this.getStageJpaRepository().deleteInBatch(delete);
        }
        return new Pair<>(insertAndUpdate, delete);
    }

    private Map<Object, List<Stage>> convertToUniqueObjMap(List<Stage> stageDatas) {

        Map<Object, List<Stage>> uniqueObjMap = new HashMap<>();
        stageDatas.forEach(x -> {
            Object uniqueObj = this.composeStageUniqueObj(x);
            if (uniqueObjMap.containsKey(uniqueObj)) {
                uniqueObjMap.get(uniqueObj).add(x);
                if (log.isDebugEnabled()) {
                    log.debug("repeat key:{}", uniqueObjMap.get(uniqueObj));
                }
            } else {
                uniqueObjMap.put(uniqueObj, Lists.newArrayList(x));
            }
        });
        return uniqueObjMap;
    }

    protected abstract Object composeStageUniqueObj(Stage x);

    private void syncToStageOneByOne(Set<String> properties, Source source, Map<Object, List<Stage>> stageUniqueMap, List<Log> logs) {

        Object sourceUniqueObj = this.composeSourceUniqueObj(source);
        if (stageUniqueMap.containsKey(sourceUniqueObj)) {
            List<Stage> stagesByUniqueObj = stageUniqueMap.get(sourceUniqueObj);
            Optional<Stage> firstMatch = stagesByUniqueObj.stream().filter(x -> (SyncAction.DELETE.getCode() == x.getAction() && this.compareSameKey(source, x)))
                    .findFirst();
            if (!firstMatch.isPresent()) {
                firstMatch = stagesByUniqueObj.stream().filter(x -> (SyncAction.DELETE.getCode() == x.getAction()))
                        .findFirst();
            }
            checkMatch(properties, source, logs, stagesByUniqueObj, firstMatch);
        } else {
            Stage stage = constructInsert4Stage(source);
            stageUniqueMap.put(sourceUniqueObj, Lists.newArrayList(stage));
        }
    }

    private void checkMatch(Set<String> properties, Source source, List<Log> logs, List<Stage> stagesByUniqueObj, Optional<Stage> firstMatch) {

        if (firstMatch.isPresent()) {
            if (this.compareData(properties, source, firstMatch.get())) {
                if (log.isDebugEnabled()) {
                    log.debug("update: {}", firstMatch.get());
                }
                firstMatch.get().setAction(SyncAction.UPDATE.getCode());
                logs.add(this.convertToLogEntity(firstMatch.get()));
                this.convertSourceToStage(source, firstMatch.get());
            } else {
                firstMatch.get().setAction(SyncAction.DEFAULT.getCode());
            }
        } else {
            Stage stage = constructInsert4Stage(source);
            stagesByUniqueObj.add(stage);
        }
    }

    private void filterProperties(Set<String> properties) {

        if (CollectionUtils.isEmpty(properties)) {
            properties.addAll(Arrays.stream(this.newInstanceOfSource().getClass().getDeclaredFields()).
                    filter(x -> !x.isAnnotationPresent(IgnoreCompare.class)).map(x -> x.getName()).collect(Collectors.toSet()));
        }
    }

    protected abstract Object composeSourceUniqueObj(Source source);

    private Stage constructInsert4Stage(Source source) {

        if (log.isDebugEnabled()) {
            log.debug("insert:{}", source);
        }
        Stage stage = this.newInstanceOfStage();
        this.convertSourceToStage(source, stage);
        stage.setAction(SyncAction.INSERT.getCode());
        return stage;
    }

    protected Log convertToLogEntity(Stage stage) {

        Log log = this.newInstanceOfLog();
        BeanUtils.copyProperties(stage, log);
        return log;
    }

    protected abstract Stage newInstanceOfStage();

    protected abstract Source newInstanceOfSource();

    protected abstract Log newInstanceOfLog();

    private void convertSourceToStage(Source source, Stage stage) {

        BeanUtils.copyProperties(source, stage);
    }

    private boolean compareData(Set<String> properties, Object source, Object stage) {

        try {
            this.filterProperties(properties);
            return properties.stream().filter(x -> {
                return compareField(source, stage, x);
            }).findAny().isPresent();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return false;
        }
    }

    private boolean compareField(Object source, Object stage, String propertyName) {

        try {
            Object sourceValue = PropertyUtils.getProperty(source, propertyName);
            Object stageValue = PropertyUtils.getProperty(stage, propertyName);
            boolean compareResult = this.compareValue(sourceValue, stageValue);
            if (compareResult) {
                log.info("value is not match for table:{}/property:{} = source:{}/stage:{}", tables.get(), propertyName, sourceValue, stageValue);
            }
            return compareResult;
        } catch (Exception e) {
            log.error("error when compare field, table:{}/property name:{}", tables.get(), propertyName, e);
            return true;
        }
    }

    private boolean compareValue(Object source, Object target) {

        if (source == target) {
            return false;
        } else if (source != null && target != null) {
            source = this.roundValue(source);
            if (source.equals(target)) {
                return false;
            }
        }
        return true;
    }

    private Object roundValue(Object source) {

        if (source instanceof Float) {
            source = Float.valueOf(FLOAT_ROUND.format(source));
        }
        return source;
    }

    private void syncToTarget(List<Stage> insertAndUpdate, List<Stage> delete) {

        Pair<List<Target>, List<Target>> targetEntity = this.convertToTargetEntity(insertAndUpdate, delete);
        if (CollectionUtils.isNotEmpty(insertAndUpdate)) {
            this.getTargetJpaRepository().save(targetEntity.getFirst());
        }
        if (CollectionUtils.isNotEmpty(delete)) {
            this.getTargetJpaRepository().deleteInBatch(targetEntity.getSecond());
        }
    }

    private void logChange(List<Log> logs) {

        if (CollectionUtils.isNotEmpty(logs)) {
            this.getLogJpaRepository().save(logs);
        }
    }

    protected abstract JpaRepository getStageJpaRepository();

    protected abstract JpaRepository getTargetJpaRepository();

    protected abstract JpaRepository getLogJpaRepository();

    Pair<List<Target>, List<Target>> convertToTargetEntity(List<Stage> insertAndUpdateList, List<Stage> deleteList) {

        List<Target> first = new ArrayList<>();
        insertAndUpdateList.forEach(x -> {
            constructTarget(first, x);
        });
        List<Target> second = new ArrayList<>();
        deleteList.forEach(x -> {
            constructTarget(second, x);
        });
        return new Pair<>(first, second);
    }

    private void constructTarget(List<Target> targets, Stage stage) {

        Target target = this.newInstanceOfTarget();
        BeanUtils.copyProperties(stage, target);
//		this.setTargetPk(stage, target);
        targets.add(target);
    }

    protected abstract Target newInstanceOfTarget();

    protected boolean compareSameKey(Source source, Stage stage) {

        return true;
    }

}
