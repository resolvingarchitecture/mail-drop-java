package ra.maildrop;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ra.common.Envelope;

import java.io.File;
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
        eSend.setClient("client1");
        String id = eSend.getId();
        eSend.addRoute(MailDropService.class, MailDropService.OPERATION_SEND);
        eSend.addNVP("test","1234");
        service.handleDocument(eSend);
        Assert.assertTrue(new File(service.mailBoxDirectory+"/client1",id).exists());

        Envelope ePickup = Envelope.documentFactory(id);
        ePickup.setClient("client1");
        ePickup.addRoute(MailDropService.class, MailDropService.OPERATION_PICKUP);
        service.handleDocument(ePickup);
        Assert.assertTrue("1234".equals(ePickup.getValue("test")));

        Envelope ePickupClean = Envelope.documentFactory(id);
        ePickupClean.setClient("client1");
        ePickupClean.addRoute(MailDropService.class, MailDropService.OPERATION_CLEAN);
        service.handleDocument(ePickupClean);
        Assert.assertFalse(new File(service.mailBoxDirectory+"/client1",id).exists());

    }

}
