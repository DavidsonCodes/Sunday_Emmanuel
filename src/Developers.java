import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Developers {
    private static boolean mainExecuted = false;

    public static void main(String[] args) {
        if (!mainExecuted) {
            mainExecuted = true;
            Connection connection = null;
            ResultSet resultSet = null;
            try {
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/developer_db", "root", "Sunepa1234");
                populateTable(connection);
                List<Map<String, Object>> resultList = getLoadedContents(connection);
                for (Map<String, Object> row : resultList) {
                    System.out.println("Name: " + row.get("name") + ", Age: " + row.get("age") + ", Location: " + row.get("location") + ", Skill: " + row.get("skill"));
                }
                System.out.println("Successful");
            } catch (SQLException | IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (resultSet != null) {
                        resultSet.close();
                    }
                    if (connection != null) {
                        connection.close();
                    }
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void populateTable(Connection connection) throws SQLException, IOException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS developers (name VARCHAR(255), age INT, location VARCHAR(255), skill VARCHAR(255))";
        try (Statement statement = connection.createStatement()) {
            statement.execute(createTableSQL);
        }

        String insertSQL = "INSERT INTO developers (name, age, location, skill) VALUES (?, ?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
             BufferedReader reader = new BufferedReader(new FileReader("C:\\Users\\sunep\\Downloads\\betWise\\project1January_Cohort\\src\\developers_data.txt"))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] data = line.split(",");
                if (data.length == 4) {
                    preparedStatement.setString(1, data[0].trim());
                    preparedStatement.setInt(2, Integer.parseInt(data[1].trim()));
                    preparedStatement.setString(3, data[2].trim());
                    preparedStatement.setString(4, data[3].trim());
                    preparedStatement.executeUpdate();
                }
            }
        }
    }

    private static List<Map<String, Object>> getLoadedContents(Connection connection) throws SQLException {
        List<Map<String, Object>> resultList = new ArrayList<>();
        String query = "SELECT * FROM developers";
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();
                row.put("name", resultSet.getString("name"));
                row.put("age", resultSet.getInt("age"));
                row.put("location", resultSet.getString("location"));
                row.put("skill", resultSet.getString("skill"));
                resultList.add(row);
            }
        }
        return resultList;
    }
}
