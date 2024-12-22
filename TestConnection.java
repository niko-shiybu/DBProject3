import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class TestConnection {
    private static final String DB_URL = "jdbc:postgresql://localhost:15432/project3";
    private static final String DB_USER = "gaussdb";
    private static final String DB_PASSWORD = "Fangyoucheng123@";

    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver"); // 注册驱动
            Connection connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            System.out.println("数据库连接成功！");
            connection.close();
        } catch (ClassNotFoundException e) {
            System.out.println("JDBC 驱动未找到：" + e.getMessage());
        } catch (SQLException e) {
            System.out.println("数据库连接失败：" + e.getMessage());
        }
    }
}
