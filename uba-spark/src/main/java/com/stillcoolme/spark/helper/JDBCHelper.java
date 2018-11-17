package com.stillcoolme.spark.helper;

import com.stillcoolme.spark.constant.Constants;
import com.stillcoolme.spark.utils.Config;
import com.stillcoolme.spark.utils.ConfigurationManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by zhangjianhua on 2018/10/22.
 * 在正式的项目的代码编写过程中，是完全严格按照大公司的coding标准来的
 * 也就是说，在代码中，是不能出现任何hard code（硬编码）的字符
 * 比如“张三”、“com.mysql.jdbc.Driver”
 * 所有这些东西，都需要通过常量来封装和使用
 */
public class JDBCHelper {

    //使用阻塞队列
    private LinkedBlockingQueue<Connection> queue=new LinkedBlockingQueue<Connection>();

    // 第一步：在静态代码块中，直接加载数据库的驱动
    // 加载驱动，不是直接简单的，使用com.mysql.jdbc.Driver就可以了
    // 之所以说，不要硬编码，他的原因就在于这里
    //
    // com.mysql.jdbc.Driver只代表了MySQL数据库的驱动
    // 那么，如果有一天，我们的项目底层的数据库要进行迁移，比如迁移到Oracle
    // 或者是DB2、SQLServer
    // 那么，就必须很费劲的在代码中，找，找到硬编码了com.mysql.jdbc.Driver的地方，然后改成
    // 其他数据库的驱动类的类名
    // 所以正规项目，是不允许硬编码的，那样维护成本很高
    //
    // 通常，我们都是用一个常量接口中的某个常量，来代表一个值
    // 然后在这个值改变的时候，只要改变常量接口中的常量对应的值就可以了
    //
    // 项目，要尽量做成可配置的
    // 就是说，我们的这个数据库驱动，更进一步，也不只是放在常量接口中就可以了
    // 最好的方式，是放在外部的配置文件中，跟代码彻底分离
    // 常量接口中，只是包含了这个值对应的key的名字
    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 第二步，实现JDBCHelper的单例化
    // 为什么要实现代理化呢？因为它的内部要封装一个简单的内部的数据库连接池
    // 为了保证数据库连接池有且仅有一份，所以就通过单例的方式
    // 保证JDBCHelper只有一个实例，实例中只有一份数据库连接池
    private static JDBCHelper instance = null;

    /**
     * 获取单例
     * @return 单例
     */
    public static JDBCHelper getInstance() {
        if(instance == null) {
            synchronized(JDBCHelper.class) {
                if(instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    // 数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    /**
     *
     * 第三步：实现单例的过程中，创建唯一的数据库连接池
     *
     * 私有化构造方法
     *
     * JDBCHelper在整个程序运行声明周期中，只会创建一次实例
     * 在这一次创建实例的过程中，就会调用JDBCHelper()构造方法
     * 此时，就可以在构造方法中，去创建自己唯一的一个数据库连接池
     *
     */
    private JDBCHelper() {
        // 首先第一步，获取数据库连接池的大小，就是说，数据库连接池中要放多少个数据库连接
        // 这个，可以通过在配置文件中配置的方式，来灵活的设定
        int datasourceSize = ConfigurationManager.getInteger(
                Constants.JDBC_DATASOURCE_SIZE);

        // 然后创建指定数量的数据库连接，并放入数据库连接池中
        for(int i = 0; i < datasourceSize; i++) {
            String runMode = Config.sparkProps.getProperty(Constants.SPARK_MASTER);

            String url = null;
            String user = null;
            String password = null;

            if(runMode.equals("local")) {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            } else {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
            }

            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 第四步，提供获取数据库连接的方法
     * 有可能，你去获取的时候，这个时候，连接都被用光了，你暂时获取不到数据库连接
     * 所以我们要自己编码实现一个简单的等待机制，去等待获取到数据库连接
     *
     */
    public synchronized Connection getConnection() {
        while(datasource.size() == 0) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    /**
     * 第五步：开发增删改查的方法
     * 1、执行增删改SQL语句的方法
     * 2、执行查询SQL语句的方法
     * 3、批量执行SQL语句的方法
     */

    /**
     * 执行增删改SQL语句
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;
        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);

            if(params != null && params.length > 0) {
                for(int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }
            rtn = pstmt.executeUpdate();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                datasource.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 执行查询SQL语句
     * @param sql
     * @param params
     * @param callback
     */
    public void executeQuery(String sql, Object[] params,
                             QueryCallback callback) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            if(params != null && params.length > 0) {
                for(int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
            }

            rs = pstmt.executeQuery();

            callback.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(conn != null) {
                datasource.push(conn);
            }
        }
    }

    /**
     * 批量执行sql语句
     * @param sql
     * @param params
     * @return
     */
    public int[] executeBatch(String sql,List<Object[]> params)
    {
        Connection connection=null;
        PreparedStatement statement=null;
        int[] res=null;
        try
        {
            connection=getConnection();
            statement=connection.prepareStatement(sql);
            //1.取消自动提交
            connection.setAutoCommit(false);
            //2.设置参数
            for (Object[] param:
                    params) {
                for (int i = 0; i < param.length; i++) {
                    statement.setObject(i+1,param[i]);
                }
                statement.addBatch();
            }
            //3.批量执行
            res=statement.executeBatch();
            //4.最后一步提交
            connection.commit();
            return res;

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            if(connection!=null)
            {
                try {
                    queue.put(connection);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        return res;
    }

    /**
     * 静态内部类：查询回调接口
     * @author Administrator
     *
     */
    public static interface QueryCallback {

        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;

    }

}
