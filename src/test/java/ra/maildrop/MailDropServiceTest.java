package ra.maildrop;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.network.NetworkBuilderStrategy;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;
import java.util.logging.Logger;

public class MailDropServiceTest {

    private static final Logger LOG = Logger.getLogger(MailDropServiceTest.class.getName());

    private static MailDropService service;
    private static MockProducer producer;
    private static Properties props;
    private static boolean ready = false;

    @BeforeClass
    public static void init() {
        LOG.info("Init...");
        props = new Properties();

        producer = new MockProducer();
        service = new MailDropService(producer, null);

        ready = service.start(props);
    }

    @AfterClass
    public static void tearDown() {
        LOG.info("Teardown...");
        service.gracefulShutdown();
    }

    @Test
    public void sendAndPickupTest() {
        Envelope eSend = Envelope.documentFactory();
        String id = eSend.getId();
        eSend.addRoute(MailDropService.class, MailDropService.OPERATION_SEND);
        eSend.addNVP("test","1234");
        service.handleDocument(eSend);
        Assert.assertTrue(new File(service.mailBoxDirectory,id).exists());

        Envelope ePickup = Envelope.documentFactory(id);
        ePickup.addRoute(MailDropService.class, MailDropService.OPERATION_PICKUP);
        service.handleDocument(ePickup);
        Assert.assertTrue(new File(service.mailBoxDirectory,id).exists());
        Assert.assertTrue("1234".equals(ePickup.getValue("test")));

        Envelope ePickupRemove = Envelope.documentFactory(id);
        ePickupRemove.addRoute(MailDropService.class, MailDropService.OPERATION_PICKUP);
        ePickupRemove.addNVP("ra.maildrop.remove","true");
        service.handleDocument(ePickupRemove);
        Assert.assertTrue("1234".equals(ePickup.getValue("test")));
        Assert.assertFalse(new File(service.mailBoxDirectory,id).exists());

    }

}
