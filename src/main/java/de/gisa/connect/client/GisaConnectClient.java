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
        if (useSsl)
        {
            factory.setPort(5671);
            factory.useSslProtocol();
        }
        factory.setVirtualHost("gisa");
        factory.setUsername(username);
        factory.setPassword(password);
        connection = factory.newConnection();
        try
        {
            channel = connection.createChannel();
            channel.confirmSelect(); // Message-Confirm aktivieren
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
        SimpleQueue simpleQueue=new SimpleQueue();
        channel.basicConsume(queueName, true, simpleQueue.consumer);
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
