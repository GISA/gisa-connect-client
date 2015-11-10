/**
 * Copyright GISA GmbH, 2015
 * 
 * Muster-Anwendung, die sich als Client zur Plattform gisa.CONNECT verbindet.
 * Diese Musteranwendung ist nicht für den produktiven Einsatz geeignet, sondern soll
 * das Verfahren verdeutlichen.
 * 
 * Weitere Erläuterungen finden sich in @see de.gisa.connect.client.GisaConnectClient
 * 
 */

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;

import org.junit.Test;

import de.gisa.connect.client.GisaConnectClient;
import de.gisa.connect.client.SimpleQueue;

/**
 * Dieser Beispielcode zeigt exemplarisch die Kommunikation über die gisa.CONNECT Plattform.
 * 
 * Für diese Demonstration wurden die folgenden Benutzer, Verträge und Routing angelegt:
 * 
 * Benutzer "demo1", Kennwort "aoQuIXie", Exchange "demo1-ex", Queue "demo1-q-queue1"
 * Benutzer "demo2", Kennwort "IelaitCw", Exchange "demo2-ex", Queue "demo2-q-queue1"
 * 
 * Vertrag: von "demo1" mit Routing-Key "demo1.demo1tag1" in Queue "demo2-q-queue1"
 * Vertrag: von "demo2" mit Routing-Key "demo2.demo2tag1" in Queue "demo1-q-queue1"
 * 
 * 
 * Für die Kommunikation mit der Plattform müssen die folgenden Rahmenbedingungen eingehalten werden:
 *
 * - Beim Lesen einer Queue ist die "consume"-Funktion zu verwenden. Ein zyklisches Polling von Queues ist nicht erlaubt.
 * - Nachrichten müssen mit DeliveryMode 2 (persist) eingeliefert werden. Das stellt sicher, dass Nachrichten vor der Empfangsbestätigung persistiert werden.
 * - Nachrichten müssen mit einer eindeutigen Message-ID verschickt werden.
 * - Für den Versand muss "Publish-Confirm" verwendet werden. Dies stellt sicher, dass eine Nachricht von der Plattform korrekt verarbeitet wurde.
 *   Bleibt die Bestätigung aus, ist die Nachricht erneut zu senden.
 * - Das Auto-Connect-Feature des Clients sollte verwendet werden
 * - Der Consumer sollte den Erhalt von Nachrichten explitit bestätigen, wenn diese verarbeitet wurden. Die Funktion autoAck sollte daher nicht
 *   verwendet werden.  
 * - Es ist sinnvoll, unterschiedliche Channels zum Schreiben und Lesen zu verwenden. 
 * 
 * Der Beispielcode zeigt eine mögliche Umsetzung dieser Rahmenbedingungen mit dem Java-Client.
 *  
 * @author mwyraz
 *
 */
public class GisaConnectDemoClientTest
{
    @Test
    public void testMessagePingPong() throws Exception
    {
        try (GisaConnectClient demo1=new GisaConnectClient("test.connect.gisa.de",false,"demo1","aoQuIXie","demo1-ex");
             GisaConnectClient demo2=new GisaConnectClient("test.connect.gisa.de",false,"demo2","IelaitCw","demo2-ex"))
        {
            Thread.sleep(100);
            SimpleQueue demo2queue=demo2.consume("demo2-q-queue1");
            demo2queue.clear();
            
            // die Queue "demo2-q-queue1" ist leer
            assertThat(demo2queue.getMessages(),empty());
            
            // demo1 sendet eine Nachricht mit dem korrekten Routing-Key
            demo1.publishMessage("msg1","demo1.demo1tag1", "Hello #1 from demo1".getBytes());
            // demo1 sendet eine Nachricht mit einem fremden Routing-Key
            demo1.publishMessage("msg2","demo2.demo2tag1", "Hello #2 from demo1".getBytes());
            // demo1 sendet eine Nachricht mit einem falschen Routing-Key
            demo1.publishMessage("msg3","demo1.otherkey", "Hello #3 from demo1".getBytes());

            Thread.sleep(100);
            
            // die Queue "demo2-q-queue1" muss genau eine Nachricht enthalten
            assertThat(demo2queue.getMessages(),contains(
                    allOf(
                            hasProperty("messageId",equalTo("msg1")),
                            hasProperty("routingKey",equalTo("demo1.demo1tag1")),
                            hasProperty("payloadAsString",equalTo("Hello #1 from demo1"))
                    )
                    ));
            
        }
    }

}
