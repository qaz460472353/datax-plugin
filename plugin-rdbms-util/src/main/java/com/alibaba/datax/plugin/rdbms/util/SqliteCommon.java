package com.alibaba.datax.plugin.rdbms.util;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import com.alibaba.druid.util.StringUtils;

public class SqliteCommon {

	private final DataBaseType DATABASE_TYPE = DataBaseType.Sqlite;
	private final String DATABASE_LOG = "DATAX_LOG";
	private final String LOG_COLUMNS = " tablename VARCHAR,success INTEGER,time TimeStamp NOT NULL DEFAULT (datetime('now','localtime'))";
	private String jdbcUrl = "";
	private String table = "";
	private JdbcConnectionFactory jdbcConnectionFactory;
	private Connection conn = null;
	private Statement statement = null;
	private ResultSet rs = null;
	private boolean isOpen = false;

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getJdbcUrl() {
		return jdbcUrl;
	}

	public void setJdbcUrl(String jdbcurl) {
		this.jdbcUrl = jdbcurl;
	}

	public SqliteCommon(Configuration originalConfig) {
		this.jdbcUrl = originalConfig.getString(Key.JDBC_URL);
		if (this.jdbcUrl == null) {
			List<Object> conns = originalConfig.getList(Constant.CONN_MARK, Object.class);
			Configuration connConf = Configuration.from(conns.get(0).toString());
			this.jdbcUrl = connConf.getString(Key.JDBC_URL);
			if (this.jdbcUrl != null && this.jdbcUrl.startsWith("[")) {
				List<String> jdbcurls = connConf.getList(Key.JDBC_URL, String.class);
				if (jdbcurls != null && jdbcurls.size() > 0) {
					this.jdbcUrl = jdbcurls.get(0);
				}

			}
			this.table = connConf.getList(Key.TABLE, String.class).get(0);
		} else {
			this.table = originalConfig.getString(Key.TABLE);
		}

	}

	public SqliteCommon(String jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}

	public void open() {
		try {
			if (!this.isOpen||(this.conn!=null&&this.conn.isClosed())) {
				this.jdbcConnectionFactory = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, "", "");
				this.conn = jdbcConnectionFactory.getConnecttion();
				isOpen = true;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// 检测数据库是否已经读取过
	public boolean checkIsReaded() {
		boolean result = false;

		boolean logexits = this.SqliteTableExits(DATABASE_LOG);
		if (!logexits) {
			return false;
		}
		this.open();
		String sql = "select *  from " + DATABASE_LOG + " where tablename='" + this.table + "'";
		try {
			statement = conn.createStatement();
			rs = statement.executeQuery(sql);
			while (rs.next()) {
				int count = rs.getInt("success");
				if (count > 0) {
					result = true;
				}
				break;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.close();
		}

		return result;

	}

	// 写入读取记录
	public void writeReadRecord() {
        this.open();
		if (!this.SqliteTableExits(DATABASE_LOG)) {
			this.creatLog();
		}
		try {
			this.open();
			statement = conn.createStatement();
			String insertsql = "insert into " + DATABASE_LOG + "(tablename,success) values('%s',%s)";
			insertsql = String.format(insertsql, this.table, 1);
			statement.executeUpdate(insertsql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			this.close();
		}
	}

	// 判断表是否存在
	public boolean isExits() {
		boolean isexits = false;
		this.open();
		if (this.conn != null) {
			isexits = this.SqliteTableExits();
			this.close();
		}
		return isexits;
	}

	// 删除数据库，判定所有的表都已经被读取过了
	public boolean delTable() {
		boolean result = false;
		this.open();

		try {
			statement = conn.createStatement();
			String sql = "drop table if exists " + this.table;
			statement.executeUpdate(sql);
			result = true;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			this.close();
		}
		return result;
	}

	// 删除已经读写完的DB 文件
	public boolean removeDB(String path) {
		// 首先获取所有的表，再按照Log 中的记录对比是否全部写入成功，如果都写入成功则删除
		// 再第一步读取的时候已经做了删除表的操作，如果只有除log 表外，没有其他其他表直接删除
		boolean del=false;
		this.open();
		try {
			statement = conn.createStatement();
			String querytableSQl = "SELECT count(*) from sqlite_master where type='table' and name<>'" +DATABASE_LOG + "'";
			rs = statement.executeQuery(querytableSQl);
			
			while (rs.next()) {
				int a = rs.getInt(1);
				if(a==0) {
					del=true;
				}
			}
			if(!del) {
				querytableSQl = "SELECT * from sqlite_master where type='table' and name<>'" +DATABASE_LOG + "'";
				rs = statement.executeQuery(querytableSQl);
				while (rs.next()) {
					String tablename = rs.getString("name");
					//查询这个表里是否有数据
					String tablecount="select count(*) from "+tablename;
					ResultSet tablec= statement.executeQuery(tablecount);
					while (tablec.next()) {
						int count=tablec.getInt(1);
						if(count==0) {
							deltable(tablename);
						}
					}
					tablec.close();
					//日志里面查询这个表的更新状态
					String logsql="SELECT * from "+DATABASE_LOG+" where tablename='"+tablename+"'";
					ResultSet rslog= statement.executeQuery(logsql);
					while (rslog.next()) {
						int success=rslog.getInt("success");
						if(success==1) {
							deltable(tablename);
						}else {
							del=false;
						}
					}
					rslog.close();
				}
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {
			this.close();
			if(del) {
				File file=new File(path);
				file.delete();
			}
		}
		return false;
	}
	private void deltable(String tablename) {
		String sql = "drop table if exists " + tablename;
		try {
			statement.executeUpdate(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	// 创建日志表
	private void creatLog() {
		this.open();
		this.creatSqliteTable(LOG_COLUMNS, DATABASE_LOG);
	}

	private void close() {
		if (this.conn != null) {
			try {
				DBUtil.closeDBResources(rs, statement, conn);
				this.isOpen = false;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public boolean SqliteTableExits(String tablename) {
		this.open();
		boolean result = false;
		try {
			statement = conn.createStatement();
			String querytableSQl = "SELECT count(*) from sqlite_master where type='table' and name='" + tablename + "'";
			rs = statement.executeQuery(querytableSQl);
			while (rs.next()) {
				int a = rs.getInt(1);
				if (a > 0) {
					result = true;
				} else {
					return false;
				}
			}
			rs.close();
			if (result) {
				// 查询是否含有数据
				String queryTable = "SELECT count(*) from " + tablename;
				rs = statement.executeQuery(queryTable);
				while (rs.next()) {
					int a = rs.getInt(1);
					if (a > 0) {
						result = true;
					} else {
						result = false;
					}
				}
			}
			rs.close();
			return result;
		} catch (SQLException e) {
			throw DataXException.asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
					String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tablename), e);

		} finally {
			this.close();
		}

	}

	/**
	 * 判断Sqlite表是否存在
	 * 
	 * @param
	 * @param
	 * @return
	 */
	public boolean SqliteTableExits() {
		return this.SqliteTableExits(this.table);
	}

	public boolean creatSqliteTable(String columnInfo) {
		return this.creatSqliteTable(columnInfo, this.table);
	}

	/**
	 * 创建表
	 * 
	 * @param columnInfo
	 * @param tablename
	 * @param columnInfo
	 * @return
	 */
	public boolean creatSqliteTable(String columnInfo, String tablename) {
		this.open();
		Boolean result = false;
		try {
			statement = conn.createStatement();
			String dropTable = "drop table if exists " + tablename;
			statement.executeUpdate(dropTable);

			String creatSql = "create table if not exists %s( %s );";
			creatSql = String.format(creatSql, tablename, columnInfo);
			statement.executeUpdate(creatSql);
			result = true;
			return result;
		} catch (SQLException e) {
			throw DataXException.asDataXException(DBUtilErrorCode.GET_COLUMN_INFO_FAILED,
					String.format("获取表:%s 的字段的元信息时失败. 请联系 DBA 核查该库、表信息.", tablename), e);
		} finally {
			this.close();
		}
	}
}
