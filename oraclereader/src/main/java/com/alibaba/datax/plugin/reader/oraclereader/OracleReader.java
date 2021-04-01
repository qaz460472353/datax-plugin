package com.alibaba.datax.plugin.reader.oraclereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.reader.util.HintUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.fastjson.JSON;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OracleReader extends Reader {

    private static final DataBaseType DATABASE_TYPE = DataBaseType.Oracle;

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(OracleReader.Job.class);

        private Configuration originalConfig = null;
        private CommonRdbmsReader.Job commonRdbmsReaderJob;

        @Override
        public void init() {
            LOG.debug("Oracle JOB  init() begin...");
            this.originalConfig = super.getPluginJobConf();
            validateParameter();
            dealFetchSize(this.originalConfig);

            this.commonRdbmsReaderJob = new CommonRdbmsReader.Job(
                    DATABASE_TYPE);

            this.commonRdbmsReaderJob.init(this.originalConfig);

            // 注意：要在 this.commonRdbmsReaderJob.init(this.originalConfig); 之后执行，这样可以直接快速判断是否是querySql 模式
            dealHint(this.originalConfig);
        }

        private void validateParameter() {
            boolean batch = this.originalConfig.getBool(Key.BATCH, false);
            if (batch) {
                //批量模式下有table 模式修改欸querytable 模式
                Map<String, String> compare = this.originalConfig.get(Key.COMPARETABLE, Map.class);
                if (compare == null) {
                    throw DataXException.asDataXException(OracleReaderErrorCode.COMPARETABLE, "批量模式下要配置对比表对象");
                }
                String where = this.originalConfig.getString(com.alibaba.datax.plugin.rdbms.reader.Key.WHERE);
                List<Map> bacthtables = this.originalConfig.getList(Key.BATCHTABLE, Map.class);
                if (bacthtables == null || bacthtables.size() == 0) {
                    throw DataXException.asDataXException(OracleReaderErrorCode.BATCHTABLE, OracleReaderErrorCode.BATCHTABLE.toString());
                }
                List<Object> connList = this.originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK, Object.class);
                Configuration connConf = Configuration.from(connList.get(0).toString());
                List<String> querySqls = connConf.getList(com.alibaba.datax.plugin.rdbms.reader.Key.QUERY_SQL, String.class);
                if (querySqls != null && querySqls.size() == 0) {

                }
                List<String> querySqlStrings = new ArrayList<>();
                //获取所有的列
                List<String> columns = this.originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Key.COLUMN, String.class);
                List<String> columnFileds = new ArrayList<>();
                String fileds = "T.*";
                if (columns != null && columns.size() > 0) {
                    for (String col : columns) {
                        columnFileds.add("T." + col);
                    }
                    fileds = StringUtils.join(columnFileds, ",");
                }
                for (Map table : bacthtables) {
                    String sql = "select " + fileds + " from  %s T where %s in (select loginfo.%s from %s loginfo where  %s)";
                    sql = String.format(sql, table.get("name"), table.get("pk"), compare.get("relation"), compare.get("name"), where);
                    querySqlStrings.add(sql);
                }
                connConf.set(com.alibaba.datax.plugin.rdbms.reader.Key.QUERY_SQL, JSON.toJSON(querySqlStrings));

                this.originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK + "[0]", connConf);
                this.originalConfig.remove(com.alibaba.datax.plugin.rdbms.reader.Key.WHERE);
            }
        }

        @Override
        public void preCheck() {
            LOG.debug("Oracle JOB  preCheck() begin...");
            init();
            this.commonRdbmsReaderJob.preCheck(this.originalConfig, DATABASE_TYPE);
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("Oracle JOB  split() begin...");
            return this.commonRdbmsReaderJob.split(this.originalConfig,
                    adviceNumber);
        }

        @Override
        public void post() {
            LOG.debug(" Oracle JOB  post() begin...");
            this.commonRdbmsReaderJob.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            LOG.debug("Oracle JOB  destroy() begin...");
            this.commonRdbmsReaderJob.destroy(this.originalConfig);
        }

        private void dealFetchSize(Configuration originalConfig) {
            int fetchSize = originalConfig.getInt(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    Constant.DEFAULT_FETCH_SIZE);
            if (fetchSize < 1) {
                throw DataXException
                        .asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                                String.format("您配置的 fetchSize 有误，fetchSize:[%d] 值不能小于 1.",
                                        fetchSize));
            }
            originalConfig.set(
                    com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
                    fetchSize);
        }

        private void dealHint(Configuration originalConfig) {
            String hint = originalConfig.getString(com.alibaba.datax.plugin.rdbms.reader.Key.HINT);
            if (StringUtils.isNotBlank(hint)) {
                boolean isTableMode = originalConfig.getBool(com.alibaba.datax.plugin.rdbms.reader.Constant.IS_TABLE_MODE).booleanValue();
                if (!isTableMode) {
                    throw DataXException.asDataXException(OracleReaderErrorCode.HINT_ERROR, "当且仅当非 querySql 模式读取 oracle 时才能配置 HINT.");
                }
                HintUtil.initHintConf(DATABASE_TYPE, originalConfig);
            }
        }
    }

    public static class Task extends Reader.Task {
        private static final Logger LOG = LoggerFactory.getLogger(OracleReader.Task.class);
        private Configuration readerSliceConfig;
        private CommonRdbmsReader.Task commonRdbmsReaderTask;

        @Override
        public void init() {
            LOG.debug("Oracle Task  init() begin...");
            this.readerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsReaderTask = new CommonRdbmsReader.Task(
                    DATABASE_TYPE, super.getTaskGroupId(), super.getTaskId());
            this.commonRdbmsReaderTask.init(this.readerSliceConfig);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.debug("Oracle Task  startRead() begin...");
            int fetchSize = this.readerSliceConfig
                    .getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);

            this.commonRdbmsReaderTask.startRead(this.readerSliceConfig,
                    recordSender, super.getTaskPluginCollector(), fetchSize);
        }

        @Override
        public void post() {
            LOG.debug("Oracle Task  post() begin...");
            this.commonRdbmsReaderTask.post(this.readerSliceConfig);
        }

        @Override
        public void destroy() {
            LOG.debug(" Oracle Task  destroy() begin...");
            this.commonRdbmsReaderTask.destroy(this.readerSliceConfig);
        }

    }

}
