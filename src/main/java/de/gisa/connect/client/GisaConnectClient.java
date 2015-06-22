package de.gisa.connect.client;

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class GisaConnectClient implements Closeable
{
    protected final Connection connection;

    protected final Channel channel;

    protected final String userPrefix;

    protected final String exchangeName;

    public GisaConnectClient(String host, boolean useSsl, String username, String password) throws IOException, TimeoutException, GeneralSecurityException
    {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        if (useSsl)
        {
            factory.setPort(5671);
            factory.useSslProtocol();
        }
        factory.setVirtualHost("VH_ppu");
        factory.setUsername(username);
        factory.setPassword(password);
        connection = factory.newConnection();
        channel = connection.createChannel(); // FIXME: 1 Channel für alles?
                                              // FIXME: connection schließen,
                                              // wenn Channel-create fehlschlägt

        userPrefix = username + ".";
        exchangeName = username + ".EXCHANGE";
    }

    public void publish(String routingTag, byte[] payload) throws IOException
    {
        channel.basicPublish(exchangeName, userPrefix + routingTag, null, payload);
    }

    public SimpleQueue consume(String queueName) throws IOException
    {
        SimpleQueue simpleQueue=new SimpleQueue();
        channel.basicConsume(userPrefix+queueName, true, simpleQueue.consumer);
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
