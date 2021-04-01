package com.alibaba.datax.plugin.reader.sqlitereader;

import com.alibaba.datax.common.spi.ErrorCode;

public enum SqliteReaderErrorCode implements ErrorCode {
    HINT_ERROR("SqliteReader-00", "您的 Hint 配置出错."),
    REQUIRED_VALUE("SqliteReader-01", "您缺失了必须填写的参数值."),
    FILE_NOT_EXISTS("SqliteReader-02", "您配置的目录文件路径不存在."),
    SECURITY_NOT_ENOUGH("SqliteReader-03", "您缺少权限执行相应的文件操作."),
    NOT_DB("SqliteReader-04","没有要更新的DB文件")
    ;

    private final String code;
    private final String description;

    private SqliteReaderErrorCode(String code, String description) {
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
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
