/**
 * Copyright GISA GmbH, 2015
 * 
 * 
 */

package de.gisa.connect.client;

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;

public class GisaConnectClient implements Closeable
{
    protected final Connection connection;

    protected final Channel channel;

    protected final String exchangeName;

    public GisaConnectClient(String host, boolean useSsl, String username, String password, String exchangeName) throws IOException, TimeoutException, GeneralSecurityException
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        // Auf der Plattform wird ausschließlich via SSL kommuniziert
        if (useSsl)
        {
            factory.setPort(5671);
            factory.useSslProtocol();
        }
        factory.setVirtualHost("gisa");
        factory.setUsername(username);
        factory.setPassword(password);
        
        /**
         * Die Heartbeat- und Recover-Funktionen stellen sicher, dass Verbindungsabbrüche erkannt und
         * die Verbindung neu aufgebaut wird. In Verbindung mit Message-Confirm beim Senden sowie der
         * Bestätigung von empfangenen Nachrichten erst nachdem diese verarbeitet wurden kann sichergestellt
         * werden, dass Nachrichten gesichert zugestellt werden.
         */
        factory.setRequestedHeartbeat(2); // Timeput in Sekunden. Das Heartbeat-Intervall entspricht die Hälfte der Zeit. 
        factory.setAutomaticRecoveryEnabled(true);
        factory.setTopologyRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(5000); // in Millisekunden
        
        connection = factory.newConnection();
        try
        {
            channel = connection.createChannel();
            // Message-Confirm aktivieren: die Plattform bestätigt den Erhalt aller Nachrichten
            // diese Funltion muss pro Channel aktiviert werden 
            channel.confirmSelect();
        }
        catch (Exception ex)
        {
            connection.close();
            throw(ex);
        }
        this.exchangeName=exchangeName;
    }

    public synchronized void publishMessage(String messageId, String routingKey, byte[] payload) throws Exception
    {
        BasicProperties props=MessageProperties.MINIMAL_BASIC
                .builder()
                .contentType("text/plain")
                .messageId(messageId) // eindeutige MessageID
                .deliveryMode(2) // DeliveryMode "Persist"
                .build();
        
        channel.basicPublish(exchangeName, routingKey, props, payload);
        
        // Bleibt das confirm aus, erzeugt dies eine IOException.
        // hier wird maximal 1000ms (1 Sekunde) gewartet
        channel.waitForConfirmsOrDie(1000L);
    }

    public SimpleQueue consume(String queueName) throws IOException
    {
        /**
         *  Im Beispiel-Client wird ein Channel zum Senden und Empfangen verwendet. Beim Produktiven Einsatz sollten
         *  getrennte Channels verwendet werden.
         */
        SimpleQueue simpleQueue=new SimpleQueue(channel);
        
        /**
         * Die Funktion autoAck wird deaktiviert. Der Consumer ist verantwortlich, ein ACK zu senden, nachdem die Nachricht lokal
         * verarbeitet bzw. persistiert wurde,  
         */
        channel.basicConsume(queueName, false, simpleQueue.consumer);
        return simpleQueue;
    }

    public void close() throws IOException
    {
        try
        {
            if (channel.isOpen()) channel.close();
        }
        catch (TimeoutException ex)
        {
            ex.printStackTrace(); // FIXME: Logging
        }
        finally
        {
            if (connection.isOpen()) connection.close();
        }
    }
}
