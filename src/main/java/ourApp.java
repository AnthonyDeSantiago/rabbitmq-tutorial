import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.util.Scanner;

public class ourApp {

    private static final String EXCHANGE_NAME = "topic_logs";

    private static Scanner scanner = new Scanner(System.in);
    private static String userInput;
    public static void main(String[] argv) throws Exception {
        boolean isSender = getType(argv);
        if (isSender) {
            System.out.println("Type 'exit' to end run");
            do {
                System.out.println("type in message: ");
                userInput = scanner.nextLine();
                if (userInput.equals("exit"))
                    break;
                String[] splitPayload = userInput.split("\\|");
                String message = splitPayload[1];
                String[] routingNums = splitPayload[0].split(" ");
                for (String routingNum : routingNums) {
                    ConnectionFactory factory = new ConnectionFactory();
                    factory.setHost("localhost");
                    try (Connection connection = factory.newConnection();
                         Channel channel = connection.createChannel()) {

                        channel.exchangeDeclare(EXCHANGE_NAME, "topic");

                        channel.basicPublish(EXCHANGE_NAME, routingNum, null, message.getBytes("UTF-8"));
                        System.out.println(" [x] Sent '" + routingNum + "':'" + message + "'");
                    }
                }
            } while (true);
        } else {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("localhost");
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            String queueName = channel.queueDeclare().getQueue();

            if (argv.length < 1) {
                System.err.println("Usage: ReceiveLogsTopic [binding_key]...");
                System.exit(1);
            }

            for (String bindingKey : argv) {
                channel.queueBind(queueName, EXCHANGE_NAME, bindingKey);
            }

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), "UTF-8");
                System.out.println(" [x] Received '" +
                        delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
            };
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });
        }
    }

    public static String getRouting(String[] argv) {
        return argv[1];
    }
    public static String getMessage(String[] argv) {
        return argv[2];
    }
    public static Boolean getType(String[] argv) {
        return argv[0].equals("s");
    }
    //..
}