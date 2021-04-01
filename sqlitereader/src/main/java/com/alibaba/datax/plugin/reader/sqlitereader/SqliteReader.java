package com.alibaba.datax.plugin.reader.sqlitereader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.reader.CommonRdbmsReader;
import com.alibaba.datax.plugin.rdbms.util.DBUtilErrorCode;
import com.alibaba.datax.plugin.rdbms.util.DataBaseType;
import com.alibaba.datax.plugin.rdbms.util.SqliteCommon;
import com.alibaba.fastjson.JSON;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SqliteReader extends Reader {

	private static final DataBaseType DATABASE_TYPE = DataBaseType.Sqlite;

	public static class Job extends Reader.Job {
		private static final Logger LOG = LoggerFactory.getLogger(Job.class);
		private List<String> path = null;
		private String filename = null;
		private FileFilter filefilter = null;
		private List<String> sourceFiles;
		private String targetFile = null;
		private Map<String, Pattern> pattern;

		private Map<String, Boolean> isRegexPath;
		private Configuration originalConfig;
		private CommonRdbmsReader.Job commonRdbmsReaderMaster;
		private boolean IsPass = false;
		private String jdbcUrlprex = "jdbc:sqlite:";
		private List<String> outDateDB = new ArrayList<String>();

		@Override
		public void init() {
			this.originalConfig = super.getPluginJobConf();
			this.pattern = new HashMap<String, Pattern>();
			this.isRegexPath = new HashMap<String, Boolean>();
			if (IsPass = this.validateParameter()) {
				int fetchSize = this.originalConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE,
						Constant.DEFAULT_FETCH_SIZE);
				if (fetchSize < 1) {
					throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
							String.format("您配置的fetchSize有误，根据DataX的设计，fetchSize : [%d] 设置值不能小于 1.", fetchSize));
				}
				this.originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE, fetchSize);

				this.commonRdbmsReaderMaster = new CommonRdbmsReader.Job(DATABASE_TYPE);
				this.commonRdbmsReaderMaster.init(this.originalConfig);
			}
		}

		private boolean validateParameter() {
			boolean result = true;
			String pathInString = this.originalConfig.getNecessaryValue(com.alibaba.datax.plugin.rdbms.reader.Key.PATH,
					SqliteReaderErrorCode.REQUIRED_VALUE);
			this.filename = this.originalConfig.getNecessaryValue(com.alibaba.datax.plugin.rdbms.reader.Key.FILE_NAME,
					SqliteReaderErrorCode.REQUIRED_VALUE);
			this.filefilter = new FileFilter(filename);
			if (StringUtils.isBlank(pathInString)) {
				throw DataXException.asDataXException(SqliteReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
			}
			if (!pathInString.startsWith("[") && !pathInString.endsWith("]")) {
				path = new ArrayList<String>();
				path.add(pathInString);
			} else {
				path = this.originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Key.PATH, String.class);
				if (null == path || path.size() == 0) {
					throw DataXException.asDataXException(SqliteReaderErrorCode.REQUIRED_VALUE, "您需要指定待读取的源目录或文件");
				}
			}
			// 获取路径下的所有文件
			for (String eachPath : this.path) {
				String regexString = eachPath.replace("*", ".*").replace("?", ".?");
				Pattern patt = Pattern.compile(regexString);
				this.pattern.put(eachPath, patt);
				this.sourceFiles = this.buildSourceTargets();
			}
			LOG.info(String.format("您即将读取的文件数为: [%s]", this.sourceFiles.size()));
			// 完善jdbcurl
			
			List<String> tables = null;
			List<Object> conns = originalConfig.getList(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK,
					Object.class);
			List<ConnectionInfo> newConns = new ArrayList<ConnectionInfo>();
			for (int i = 0, len = conns.size(); i < len; i++) {
				if (i == 0) {
					Configuration connConf = Configuration.from(conns.get(0).toString());
					tables = connConf.getList(com.alibaba.datax.plugin.rdbms.reader.Key.TABLE, String.class);
				}
			}
			// 暂时先读取第一个
			boolean have_update_db = false;
			for (String path : this.sourceFiles) {
				File _file = new File(path);
				if (_file.exists()) {
					targetFile = _file.getAbsolutePath();
					ConnectionInfo info = new ConnectionInfo();
					String filepath = _file.getAbsolutePath();
					info.addjdbcUrl(jdbcUrlprex + filepath);
					info.setTable(tables);
					newConns.add(info);
				}
				originalConfig.set(com.alibaba.datax.plugin.rdbms.reader.Constant.CONN_MARK, JSON.toJSON(newConns));
				SqliteCommon common = new SqliteCommon(this.originalConfig);//TODO
				if (!common.checkIsReaded()) {
					// 发现没有读取的db
					have_update_db = true;
					break;
				} else {
					outDateDB.add(_file.getAbsolutePath());
					common.delTable();
				}
			}
			if (!have_update_db) {
				// 没有要更新的文件，中止更新
				result = false;
			}
			return result;

		}

		@Override
		public void prepare() {
			LOG.debug("prepare() begin...");
			// warn:make sure this regex string
			// warn:no need trim

		}

		@Override
		public List<Configuration> split(int adviceNumber) {
			if (IsPass) {
				return this.commonRdbmsReaderMaster.split(this.originalConfig, adviceNumber);
			} else {
				return new ArrayList<Configuration>();
			}

		}

		@Override
		public void post() {
			if (IsPass) {
				this.commonRdbmsReaderMaster.post(this.originalConfig);
			}

		}

		@Override
		public void destroy() {
			if (IsPass) {
				this.commonRdbmsReaderMaster.destroy(this.originalConfig);
			}

			Boolean isdel = this.originalConfig.getBool(Key.IS_DEL);
			if (isdel) {
				if(!outDateDB.isEmpty()) {  //TODO
					for(String path:outDateDB) {
						SqliteCommon common = new SqliteCommon(jdbcUrlprex+path);
						common.removeDB(path);
					}
				}
			}

		}

		// validate the path, path must be a absolute path
		private List<String> buildSourceTargets() {
			// for eath path
			Set<String> toBeReadFiles = new HashSet<String>();
			for (String eachPath : this.path) {
				int endMark;
				for (endMark = 0; endMark < eachPath.length(); endMark++) {
					if ('*' != eachPath.charAt(endMark) && '?' != eachPath.charAt(endMark)) {
						continue;
					} else {
						this.isRegexPath.put(eachPath, true);
						break;
					}
				}

				String parentDirectory;
				if (BooleanUtils.isTrue(this.isRegexPath.get(eachPath))) {
					int lastDirSeparator = eachPath.substring(0, endMark).lastIndexOf(IOUtils.DIR_SEPARATOR);
					parentDirectory = eachPath.substring(0, lastDirSeparator + 1);
				} else {
					this.isRegexPath.put(eachPath, false);
					parentDirectory = eachPath;
				}
				this.buildSourceTargetsEathPath(eachPath, parentDirectory, toBeReadFiles);
			}
			List<String> list = Arrays.asList(toBeReadFiles.toArray(new String[0]));
			// 对读取到的文件进行按时间升序
			list.sort(String::compareTo);
			return list;
		}

		private void buildSourceTargetsEathPath(String regexPath, String parentDirectory, Set<String> toBeReadFiles) {
			// 检测目录是否存在，错误情况更明确
			boolean isExists = false;
			try {
				File dir = new File(parentDirectory);
				isExists = dir.exists();
				if (!isExists) {
					String message = String.format("您设定的目录不存在 : [%s]", parentDirectory);
					LOG.error(message);
					return;
					// throw DataXException.asDataXException(SqliteReaderErrorCode.FILE_NOT_EXISTS, message);
				}
			} catch (SecurityException se) {
				String message = String.format("您没有权限查看目录 : [%s]", parentDirectory);
				LOG.error(message);
				throw DataXException.asDataXException(SqliteReaderErrorCode.SECURITY_NOT_ENOUGH, message);
			}
			if (isExists) {
				directoryRover(regexPath, parentDirectory, toBeReadFiles);
			}
		}

		private void directoryRover(String regexPath, String parentDirectory, Set<String> toBeReadFiles) {
			File directory = new File(parentDirectory);
			// is a normal file
			if (!directory.isDirectory()) {
				if (this.isTargetFile(regexPath, directory.getAbsolutePath())) {
					toBeReadFiles.add(parentDirectory);
					LOG.info(String.format("add file [%s] as a candidate to be read.", parentDirectory));
				}
			} else {
				// 是目录
				try {
					// warn:对于没有权限的目录,listFiles 返回null，而不是抛出SecurityException
					File[] files = directory.listFiles(this.filefilter);
					if (null != files) {
						for (File subFileNames : files) {
							directoryRover(regexPath, subFileNames.getAbsolutePath(), toBeReadFiles);
						}
					} else {
						// warn: 对于没有权限的文件，是直接throw DataXException
						String message = String.format("您没有权限查看目录 : [%s]", directory);
						LOG.error(message);
						throw DataXException.asDataXException(SqliteReaderErrorCode.SECURITY_NOT_ENOUGH, message);
					}

				} catch (SecurityException e) {
					String message = String.format("您没有权限查看目录 : [%s]", directory);
					LOG.error(message);
					throw DataXException.asDataXException(SqliteReaderErrorCode.SECURITY_NOT_ENOUGH, message, e);
				}
			}
		}

		// 正则过滤
		private boolean isTargetFile(String regexPath, String absoluteFilePath) {
			if (this.isRegexPath.get(regexPath)) {
				return this.pattern.get(regexPath).matcher(absoluteFilePath).matches();
			} else {
				return true;
			}

		}

	}

	public static class Task extends Reader.Task {

		private Configuration readerSliceConfig;
		private CommonRdbmsReader.Task commonRdbmsReaderSlave;

		@Override
		public void init() {
			this.readerSliceConfig = super.getPluginJobConf();
			this.commonRdbmsReaderSlave = new CommonRdbmsReader.Task(DATABASE_TYPE);
			this.commonRdbmsReaderSlave.init(this.readerSliceConfig);
		}

		@Override
		public void startRead(RecordSender recordSender) {
			int fetchSize = this.readerSliceConfig.getInt(com.alibaba.datax.plugin.rdbms.reader.Constant.FETCH_SIZE);

			this.commonRdbmsReaderSlave.startRead(this.readerSliceConfig, recordSender, super.getTaskPluginCollector(),
					fetchSize);
			SqliteCommon common = new SqliteCommon(this.readerSliceConfig);
			common.writeReadRecord();
		}

		@Override
		public void post() {
			
			this.commonRdbmsReaderSlave.post(this.readerSliceConfig);
			
		}

		@Override
		public void destroy() {
			this.commonRdbmsReaderSlave.destroy(this.readerSliceConfig);
		}
	}

}
