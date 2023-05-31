package gr.upatras.subscriber;

import java.util.UUID;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
//import org.eclipse.paho.client.mqttv3.MqttTopic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Subscriber implements MqttCallback {
    MqttClient myClient;
    MqttConnectOptions connOpt;

    static final String M2MIO_THING = UUID.randomUUID().toString();
    static final String BROKER_URL = "tcp://test.mosquitto.org:1883";

    private static final Logger log = LoggerFactory.getLogger(Subscriber.class);
    public static final String TOPIC = "machine1/count";

    public void connectionLost(Throwable t) {
        log.info("Connection lost!");
    }
    
    public void deliveryComplete(IMqttDeliveryToken token) {}

    // message is received on a subscribed topic.
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        log.info("\n");
        log.info("-------------------------------------------------");
        log.info("| Topic:" + topic);
        log.info("| Message: " + new String(message.getPayload()));
        log.info("-------------------------------------------------");
        log.info("\n");
    }

    // ************ main ************
    public static void main(String[] args) {
        Subscriber subscriber = new Subscriber();
        subscriber.runSubscriber();
    }

    // Create an MQTT client, connect to the broker, subscribe to a topic, and wait for messages.
    public void runSubscriber() {
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

        // subscribe to topic
        try {
            int subQoS = 0;
            myClient.subscribe(TOPIC, subQoS);
            while (true) {
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}