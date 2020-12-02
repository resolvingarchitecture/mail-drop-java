package ra.maildrop;

import ra.common.Envelope;
import ra.common.messaging.MessageProducer;
import ra.common.route.Route;
import ra.common.service.BaseService;
import ra.common.service.ServiceStatus;
import ra.common.service.ServiceStatusObserver;
import ra.util.Config;
import ra.util.FileUtil;

import java.io.File;
import java.io.IOException;
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
    protected File mailBoxDirectory;

    public MailDropService() {
        super();
    }

    public MailDropService(MessageProducer producer, ServiceStatusObserver observer) {
        super(producer, observer);
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
        File envFile = new File(mailBoxDirectory, e.getId());
        try {
            if(!envFile.createNewFile()) {
                LOG.warning("Unable to create file to persist Envelope waiting on network");
                return false;
            }
        } catch (IOException ioException) {
            LOG.warning(ioException.getLocalizedMessage());
            return false;
        }
        FileUtil.writeFile(e.toJSON().getBytes(), envFile.getAbsolutePath());
        LOG.info("Persisted envelope (id="+e.getId()+") to file.");
        return true;
    }

    private boolean pickUp(Envelope e) {
        // Pickup messages for peer/client
        boolean remove = false;
        if(e.getValue("ra.maildrop.remove")!=null && "true".equals(e.getValue("ra.maildrop.remove"))) {
            remove = true;
        }
        File mail = new File(mailBoxDirectory, e.getId());
        if(!mail.exists()) {
            e.addErrorMessage("Envelope does not exist.");
            return true;
        }
        if(!mail.canRead()) {
            e.addErrorMessage("Unable to read persisted envelope.");
            return true;
        }
        byte[] bytes;
        try {
            bytes = FileUtil.readFile(mail.getAbsolutePath());
        } catch (IOException ex) {
            LOG.warning(ex.getLocalizedMessage());
            return false;
        }
        String json = new String(bytes);
        e.fromJSON(json);
        if(remove && !mail.delete()) {
            e.addErrorMessage("Unable to delete persisted envelope.");
        }
        return true;
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
        mailBoxDirectory = new File(getServiceDirectory(), "msg");
        if(!mailBoxDirectory.exists() && !mailBoxDirectory.mkdir()) {
            LOG.severe("Unable to create message hold directory.");
            return false;
        }
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
