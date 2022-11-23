
import netscape.javascript.JSObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.Properties;
public class Consumer {
    public static void main(String[] args) {
        KafkaConsumer consumer;
        String broker = "localhost:9092";
        String topic = "kseb";
        Properties prop = new Properties();
        prop.put("bootstrap.servers", broker);
        prop.put("group.id", "test.group");
        prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer(prop);
        consumer.subscribe(Arrays.asList(topic));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.value());
                JSONObject obj = new JSONObject(record.value());
                String user = String.valueOf(obj.getInt("user_id"));
                String unit = String.valueOf(obj.getInt("unit"));
                System.out.println(user);
                System.out.println(unit);
                try {
                    Class.forName("com.mysql.jdbc.Driver");
                    Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/ksebdb", "root", "");
                    String sql = "INSERT INTO `usage`(`consumerid`, `unit`, `date`) VALUES (?,?,now())";
                    PreparedStatement stmt = con.prepareStatement((sql));
                    stmt.setString(1, user);
                    stmt.setString(2, unit);
                    stmt.executeUpdate();
                    System.out.println("value inserted successfully.........!");
                } catch (Exception e) {
                    System.out.println((e));
                }
            }

        }
    }
}
