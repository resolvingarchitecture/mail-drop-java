package ra.maildrop;

import ra.common.Envelope;
import ra.common.messaging.MessageProducer;
import ra.common.route.Route;
import ra.common.service.BaseService;
import ra.common.service.ServiceStatus;
import ra.common.service.ServiceStatusListener;
import ra.util.Config;

import java.util.*;
import java.util.logging.Logger;

/**
 * Mail Drop as a service
 */
public class MailDropService extends BaseService {

    private static final Logger LOG = Logger.getLogger(MailDropService.class.getName());

    public static final String OPERATION_SEND = "SEND";
    public static final String OPERATION_PICKUP = "PICKUP";

    public static final String RA_MAIL_DROP_CONFIG = "ra-mail-drop.config";
    public static final String RA_MAIL_DROP_DIR = "ra.maildrop.dir";

    protected Properties config;

    public MailDropService(MessageProducer producer, ServiceStatusListener listener) {
        super(producer, listener);
    }

    @Override
    public void handleDocument(Envelope envelope) {
        Route r = envelope.getDynamicRoutingSlip().getCurrentRoute();
        switch(r.getOperation()) {
            case OPERATION_SEND: {send(envelope);break;}
            case OPERATION_PICKUP: {pickUp(envelope);break;}
            default: {deadLetter(envelope);break;}
        }
    }

    @Override
    public boolean send(Envelope e) {
        // Persist message into mail box

        return true;
    }

    private void pickUp(Envelope e) {
        // Pickup messages for peer/client

    }

    @Override
    public boolean start(Properties p) {
        super.start(p);
        LOG.info("Initializing...");
        updateStatus(ServiceStatus.INITIALIZING);
        try {
            config = Config.loadFromClasspath(RA_MAIL_DROP_CONFIG, p, false);
        } catch (Exception e) {
            LOG.severe(e.getLocalizedMessage());
            return false;
        }
        config.put(RA_MAIL_DROP_DIR, getServiceDirectory().getAbsolutePath());
        updateStatus(ServiceStatus.STARTING);
        return true;
    }

    @Override
    public boolean pause() {
        LOG.warning("Pausing not yet supported.");
        return false;
    }

    @Override
    public boolean unpause() {
        LOG.warning("Pausing not yet supported.");
        return false;
    }

    @Override
    public boolean restart() {
        LOG.info("Restarting...");
        shutdown();
        start(config);
        LOG.info("Restarted.");
        return true;
    }

    @Override
    public boolean shutdown() {
        LOG.info("Shutting down...");
        updateStatus(ServiceStatus.SHUTDOWN);
        LOG.info("Shutdown.");
        return true;
    }

    @Override
    public boolean gracefulShutdown() {
        return shutdown();
    }

}
