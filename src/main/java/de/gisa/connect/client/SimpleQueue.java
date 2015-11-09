package de.gisa.connect.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

public class SimpleQueue
{
    public static class Message
    {
        protected String messageId;
        protected String routingKey;
        protected byte[] payload;
        
        public Message(String messageId, String routingKey, byte[] payload)
        {
            super();
            this.messageId = messageId;
            this.routingKey = routingKey;
            this.payload = payload;
        }
        public String getMessageId()
        {
            return messageId;
        }
        public String getRoutingKey()
        {
            return routingKey;
        }
        public byte[] getPayload()
        {
            return payload;
        }
        public String getPayloadAsString()
        {
            return payload==null?null:new String(payload);
        }
    }
    
    public boolean isEmpty()
    {
        return getMessageCount()==0;
    }
    
    public int getMessageCount()
    {
        synchronized (messages)
        {
            return messages.size();
        }
    }
    
    public void clear()
    {
        synchronized (messages)
        {
            messages.clear();
        }
    }
    
    public Message retrieve()
    {
        synchronized (messages)
        {
            if (messages.isEmpty()) return null;
            return messages.remove(0);
        }
    }
    
    public List<Message> getMessages()
    {
        return messages;
    }
    
    protected final List<Message> messages=new ArrayList<Message>();
    
    protected Consumer consumer=new Consumer() {
        
        public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig)
        {
        }
        
        public void handleRecoverOk(String consumerTag)
        {
        }
        
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body)
        {
            synchronized (messages)
            {
                messages.add(new Message(properties.getMessageId(),envelope.getRoutingKey(),body));
                messages.notify();
            }
        }
        
        public void handleConsumeOk(String consumerTag)
        {
        }
        
        public void handleCancelOk(String consumerTag)
        {
        }
        
        public void handleCancel(String consumerTag) throws IOException
        {
        }
    };

}
