package com.alibaba.datax.plugin.reader.sqlitereader;

import java.util.ArrayList;
import java.util.List;

public class ConnectionInfo {
	
	private List<String> table=new ArrayList<String>();
	private List<String> jdbcUrl=new ArrayList<String>();
	public List<String> getTable() {
		return table;
	}
	public void setTable(List<String> table) {
		this.table = table;
	}
	public List<String> getJdbcUrl() {
		return jdbcUrl;
	}
	public void setJdbcUrl(List<String> jdbcUrl) {
		this.jdbcUrl = jdbcUrl;
	}
	
	public void addjdbcUrl(String url) {
		this.jdbcUrl.add(url);
	}
	
	

}
