package com.alibaba.datax.plugin.writer.sqlitewriter;

import java.io.File;
import java.io.FilenameFilter;

public class FileFilter implements FilenameFilter {
	private String prefix="";
    public  boolean accept(File file, String name){
        return (name.startsWith(prefix));
    }
    
    public FileFilter(String prefix) {
    	this.prefix=prefix;
    }
}
