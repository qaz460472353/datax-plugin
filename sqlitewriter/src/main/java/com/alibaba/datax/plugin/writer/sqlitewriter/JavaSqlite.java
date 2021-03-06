package com.alibaba.datax.plugin.writer.sqlitewriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JavaSqlite {
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        try {
            // 0 连接SQLite的JDBC
            String sql = "jdbc:sqlite:d:/test.db";
            Class.forName("org.sqlite.JDBC");

            // 1 建立一个数据库名zieckey.db的连接，如果不存在就在当前目录下创建之
            Connection conn = DriverManager.getConnection(sql);
            Statement stat = conn.createStatement();

            // 2 创建一个表tbl1，录入数据
            stat.executeUpdate("drop table if exists tbl1;");
            stat.executeUpdate("create table if not exists tbl1(name varchar(20), salary int);");// 创建一个表，两列
            stat.executeUpdate("insert into tbl1 values('ZhangSan',8000);"); // 插入数据
            stat.executeUpdate("insert into tbl1 values('LiSi',7800);");
            stat.executeUpdate("insert into tbl1 values('WangWu',5800);");
            stat.executeUpdate("insert into tbl1 values('ZhaoLiu',9100);");
            ResultSet rs = stat.executeQuery("select * from tbl1;"); // 查询数据
            System.out.println("创建表结构录入数据操作演示：");
            while (rs.next()) { // 将查询到的数据打印出来
                System.out.print("name = " + rs.getString("name") + ", "); // 列属性一
                System.out.println("salary = " + rs.getString("salary")); // 列属性二
            }

            rs.close();

            // 3 修改表结构，添加字段 address varchar(20) default 'changsha';
            stat.executeUpdate("alter table tbl1 add column address varchar(20) not null default 'changsha'; ");// 创建一个表，两列
            stat.executeUpdate("insert into tbl1 values('HongQi',9000,'tianjing');"); // 插入数据
            stat.executeUpdate("insert into tbl1(name,salary) values('HongQi',9000);"); // 插入数据
            rs = stat.executeQuery("select * from tbl1;"); // 查询数据
            System.out.println("表结构变更操作演示：");
            while (rs.next()) { // 将查询到的数据打印出来
                System.out.print("name = " + rs.getString("name") + ", "); // 列属性一
                System.out.print("name = " + rs.getString("name") + ", "); // 列属性二
                System.out.println("address = " + rs.getString("address")); // 列属性三
            }
            rs.close();
            conn.close(); // 结束数据库的连接
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
