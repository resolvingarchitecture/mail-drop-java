package ra.maildrop;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import ra.common.DLC;
import ra.common.Envelope;
import ra.common.network.NetworkBuilderStrategy;

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

//    @Test
//    public void sendTest() {
//
//    }
//
//    @Test
//    public void pickUpTest() {
//
//    }
}
