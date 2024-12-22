import java.sql.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ThroughputTest {
    // 数据库连接信息
    private static final String DB_URL = "jdbc:postgresql://localhost:15432/postgres";
    private static final String DB_USER = "gaussdb";
    private static final String DB_PASSWORD = "Fangyoucheng123@";

    // 测试配置
    private static final int THREAD_COUNT = 30;
    private static final int TRANSACTIONS_PER_THREAD = 10000;

    public static void main(String[] args) {

        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < THREAD_COUNT; i++) {
            executorService.submit(() -> {
                try (Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD)) {
                    connection.setAutoCommit(false);
                    for (int j = 0; j < TRANSACTIONS_PER_THREAD; j++) {
                        executeTransaction(connection);
                    }
                    connection.commit();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            });
        }

        executorService.shutdown();
        try {
            executorService.awaitTermination(1, TimeUnit.HOURS); // 等待线程完成
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        long endTime = System.currentTimeMillis();


        long totalTimeMillis = endTime - startTime;
        double totalTimeSeconds = totalTimeMillis / 10000.0;
        int totalTransactions = THREAD_COUNT * TRANSACTIONS_PER_THREAD; 
        double tps = totalTransactions / totalTimeSeconds;
        System.out.println("总事务数: " + totalTransactions);
        System.out.println("总耗时: " + totalTimeSeconds + " 秒");
        System.out.println("TPS (每秒事务数): " + tps);
    }

    // 执行单个事务
    private static void executeTransaction(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            // 随机查询账户和分支余额
            //statement.executeQuery("SELECT bid, aid, abalance FROM test_accounts ORDER BY RANDOM() LIMIT 1");

            statement.executeQuery("SELECT SUM(bbalance) AS total_balance FROM test_branches");

            statement.executeQuery("SELECT bid, aid, delta, mtime FROM test_history ORDER BY mtime DESC LIMIT 10");
        }
    }
}
