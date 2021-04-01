package com.alibaba.datax.plugin.writer.sqlitewriter;

import com.alibaba.datax.common.spi.ErrorCode;

public enum SqliteWriterErrorCode implements ErrorCode {
    
    CONFIG_INVALID_EXCEPTION("SqliteWriter-00", "您的参数配置错误."),
    REQUIRED_VALUE("SqliteWriter-01", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("SqliteWriter-02", "您填写的参数值不合法."),
    Write_FILE_ERROR("SqliteWriter-03", "您配置的目标文件在写入时异常."),
    Write_FILE_IO_ERROR("SqliteWriter-04", "您配置的文件在写入时出现IO异常."),
    SECURITY_NOT_ENOUGH("SqliteWriter-05", "您缺少权限执行相应的文件写入操作.");

    private final String code;
    private final String description;

    private SqliteWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code,
                this.description);
    }

}