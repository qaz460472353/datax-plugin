package com.alibaba.datax.plugin.writer.sqlitewriter;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.util.DBUtil;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.JdbcConnectionFactory;
import com.alibaba.datax.plugin.rdbms.util.SqliteCommon;
import com.alibaba.datax.plugin.rdbms.writer.CommonRdbmsWriter;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.fastjson.JSON;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqliteWriter extends Writer {
    private static final DataBaseType DATABASE_TYPE = DataBaseType.Sqlite;
    private static final Logger LOG = LoggerFactory.getLogger(SqliteWriter.Job.class);
    private static final Logger TaskLOG = LoggerFactory.getLogger(SqliteWriter.Task.class);

    public static class Job extends Writer.Job {

        private Configuration originalConfig = null;
        private CommonRdbmsWriter.Job commonRdbmsWriterMaster;
        private FileFilter filefilter = null;

        @Override
        public void init() {
            LOG.debug("SqliteWriter JOB  init() begin...");
            this.originalConfig = super.getPluginJobConf();
            // DriverManager.setLogWriter(new PrintWriter(System.out));
            validateParameter();
            // warn：not like mysql, only support insert mode, don't use
            this.commonRdbmsWriterMaster = new CommonRdbmsWriter.Job(DATABASE_TYPE);
            this.commonRdbmsWriterMaster.init(this.originalConfig);
        }

        private void validateParameter() {
            this.originalConfig.set(com.alibaba.datax.plugin.rdbms.writer.Key.COPY_COLUMN,
                    this.originalConfig.get(com.alibaba.datax.plugin.rdbms.writer.Key.COLUMN));
            String filename = this.originalConfig.getNecessaryValue(Key.FILE_NAME,
                    SqliteWriterErrorCode.REQUIRED_VALUE);
            if (filename == null || filename.isEmpty()) {
                filename = "sm_exchange";
            }
            final String fileName = filename;
            this.filefilter = new FileFilter(fileName);
            this.originalConfig.set(Key.FILE_NAME, filename);
            String path = this.originalConfig.getNecessaryValue(Key.PATH, SqliteWriterErrorCode.REQUIRED_VALUE);

            try {
                // warn: 这里用户需要配一个目录
                File dir = new File(path);
                if (dir.isFile()) {
                    throw DataXException.asDataXException(SqliteWriterErrorCode.ILLEGAL_VALUE,
                            String.format("您配置的path: [%s] 不是一个合法的目录, 请您注意文件重名, 不合法目录名等情况.", path));
                }
                if (!dir.exists()) {
                    boolean createdOk = dir.mkdirs();
                    if (!createdOk) {
                        throw DataXException.asDataXException(SqliteWriterErrorCode.CONFIG_INVALID_EXCEPTION,
                                String.format("您指定的文件路径 : [%s] 创建失败.", path));
                    }
                }
                /*else { }*/
                String setJdbcUrlName = "";
                // 文件集合
                File[] files = dir.listFiles(this.filefilter);
                if (files != null && files.length > 0) {
                    Arrays.sort(files);
                        /*Arrays.sort(files, (f1, f2) -> {
                            long f1Time = 0, f2_time = 0;
                            if (f1.getName().contains("_")) {
                                f1Time = Long.parseLong(f1.getName().substring(fileName.length() + 1));
                            }
                            if (f2.getName().contains("_")) {
                                f2_time = Long.parseLong(f2.getName().substring(fileName.length() + 1));
                            }
                            long diff = f1Time - f2_time;
                            if (diff > 0) {
                                return 1;
                            } else if (diff == 0) {
                                return 0;
                            } else {
                                return -1;// 如果 if 中修改为 返回-1 同时此处修改为返回 1 排序就会是递减
                            }
                        });*/
                        //TODO 判断前一个生成的文件中是否含有数据
                   SqliteCommon sqlitecommon = new SqliteCommon(this.originalConfig);
                    for (File f : files) {
                        String jdbcUrl = "jdbc:sqlite:" + f.getAbsolutePath();
                        sqlitecommon.setJdbcUrl(jdbcUrl);
                        if (!sqlitecommon.isExits()) {
                            filename = f.getName();
                            setJdbcUrlName = f.getAbsolutePath();
                            break;
                        }
                    }
                    if ("".equals(setJdbcUrlName)) {//没有找到合适的文件，新建一个
                        filename = filename + "_" + System.currentTimeMillis();
                        setJdbcUrlName = path + File.separator + filename;
                    }
                } else {
                    filename = filename + "_" + System.currentTimeMillis();
                    setJdbcUrlName = path + File.separator + filename;
                }
                String jdbcUrl = "jdbc:sqlite:" + setJdbcUrlName;
                jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForReader(jdbcUrl);
                originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, 0,
                        com.alibaba.datax.plugin.rdbms.writer.Key.JDBC_URL), jdbcUrl);
                originalConfig.set(Key.FILE_NAME, filename);
            } catch (SecurityException se) {
                throw DataXException.asDataXException(SqliteWriterErrorCode.SECURITY_NOT_ENOUGH,
                        String.format("您没有权限创建文件路径 : [%s] ", path), se);
            }

        }

        // 检测数据库是否可以用
        private boolean testDb(String jdbcUrl) {
            SqliteCommon common = new SqliteCommon(this.originalConfig);
            return common.isExits();
        }

        @Override
        public void prepare() {
            LOG.debug("SqliteWriter JOB  prepare() begin...");
            this.commonRdbmsWriterMaster.prepare(this.originalConfig);
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            LOG.debug("SqliteWriter JOB  split() begin...");
            return this.commonRdbmsWriterMaster.split(this.originalConfig, mandatoryNumber);
            /*
             * List<Configuration> writerSplitConfigs = new ArrayList<Configuration>(); for
             * (int i = 0; i < mandatoryNumber; i++) {
             * writerSplitConfigs.add(originalConfig); } return writerSplitConfigs;
             */
        }

        @Override
        public void post() {
            LOG.debug("SqliteWriter JOB  post() begin...");
            this.commonRdbmsWriterMaster.post(this.originalConfig);
        }

        @Override
        public void destroy() {
            LOG.debug("SqliteWriter JOB  destroy() begin...");
            this.commonRdbmsWriterMaster.destroy(this.originalConfig);
        }

    }

    public static class Task extends Writer.Task {
        private Configuration writerSliceConfig;
        private CommonRdbmsWriter.Task commonRdbmsWriterSlave;

        @Override
        public void init() {
            TaskLOG.debug("SqliteWriter Task  init() begin...");
            this.writerSliceConfig = super.getPluginJobConf();
            this.commonRdbmsWriterSlave = new CommonRdbmsWriter.Task(DATABASE_TYPE);
            this.commonRdbmsWriterSlave.init(this.writerSliceConfig);
            //TaskPluginCollector cc=super.getTaskPluginCollector();

        }

        @Override
        public void prepare() {
            TaskLOG.debug("SqliteWriter prepare() begin...");
            this.commonRdbmsWriterSlave.prepare(this.writerSliceConfig);
        }

        @Override
        public void startWrite(RecordReceiver recordReceiver) {
            this.commonRdbmsWriterSlave.startWrite(recordReceiver, this.writerSliceConfig,
                    super.getTaskPluginCollector());

        }

        @Override
        public void post() {
            TaskLOG.debug("SqliteWriter post() begin...");
            this.commonRdbmsWriterSlave.post(this.writerSliceConfig);

        }

        @Override
        public void destroy() {
            TaskLOG.debug("SqliteWriter destroy() begin...");
            this.commonRdbmsWriterSlave.destroy(this.writerSliceConfig);
        }

    }

}
