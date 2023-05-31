package gr.upatras.publisher;

import java.util.Random;
import java.util.UUID;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Publisher implements MqttCallback {
    MqttClient myClient;
    MqttConnectOptions connOpt;

    static final String M2MIO_THING = UUID.randomUUID().toString();
    static final String BROKER_URL = "tcp://test.mosquitto.org:1883";

    private Random rnd = new Random();
    private static final Logger log = LoggerFactory.getLogger(Publisher.class);
    public static final String TOPIC = "machine1/count";

    int counter = rnd.nextInt(10000);

    public void connectionLost(Throwable t) {
        log.info("Connection lost!");
    }

    // published message by this client is successfully received by the broker.
    public void deliveryComplete(IMqttDeliveryToken token) {}
    
    // message is received on a subscribed topic.
	public void messageArrived(String topic, MqttMessage message) throws Exception {}

    // ************ main ************
    public static void main(String[] args) {
        Publisher publisher = new Publisher();
        publisher.runPublisher();
    }

    // Create a MQTT client, connect to the broker, publish messages, and disconnect.
    public void runPublisher() {
        // setup MQTT Client
        String clientID = M2MIO_THING;
        connOpt = new MqttConnectOptions();
        connOpt.setCleanSession(true);
        connOpt.setKeepAliveInterval(30);

        try {
            myClient = new MqttClient(BROKER_URL, clientID);
            myClient.setCallback(this);
            myClient.connect(connOpt);
        } catch (MqttException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        log.info("Connected to " + BROKER_URL);
        MqttTopic topic = myClient.getTopic(TOPIC);

        // publish messages
        while (true) {
            String val = String.valueOf(counter++);
            String pubMsg = "count = " + val;
            int pubQoS = 2;
            MqttMessage message = new MqttMessage(pubMsg.getBytes());
            message.setQos(pubQoS);
            message.setRetained(false);

            // Publish the message
            log.info("Publishing to topic " + topic + ", QoS = " + pubQoS + ", value = " + val);
            MqttDeliveryToken token = null;
            try {
                // publish message to broker
                token = topic.publish(message);
                // Wait until the message has been delivered to the broker
                token.waitForCompletion();
                Thread.sleep(1000);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
