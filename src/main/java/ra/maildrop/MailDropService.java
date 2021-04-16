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
    public static final String OPERATION_CLEAN = "CLEAN";
    public static final String OPERATION_PICKUP_CLEAN = "PICKUP_CLEAN";

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
            case OPERATION_CLEAN: {clean(envelope);break;}
            case OPERATION_PICKUP_CLEAN: {
                if(pickUp(envelope))
                    clean(envelope);
                break;
            }
            default: {deadLetter(envelope);break;}
        }
    }

    @Override
    public boolean send(Envelope e) {
        if(e.getClient()==null || e.getClient().isEmpty()) {
            e.addErrorMessage("Client is required to be set.");
            LOG.warning("Client is required to be set.");
            return false;
        }
        File clientMailbox = new File(mailBoxDirectory+"/"+e.getClient());
        if(!clientMailbox.exists() && !clientMailbox.mkdir()) {
            LOG.warning("Unable to create client mailbox directory.");
            return false;
        }
        // Persist message into mail box
        File msgFile = new File(clientMailbox, e.getId());
        try {
            if(!msgFile.createNewFile()) {
                LOG.warning("Unable to create msg file to persist Envelope.");
                return false;
            }
        } catch (IOException ioException) {
            LOG.warning(ioException.getLocalizedMessage());
            return false;
        }
        FileUtil.writeFile(e.toJSON().getBytes(), msgFile.getAbsolutePath());
        LOG.info("Persisted envelope (id="+e.getId()+") to file.");
        return true;
    }

    private boolean pickUp(Envelope e) {
        // Pickup messages for peer/client
        if(e.getClient()==null || e.getClient().isEmpty()) {
            e.addErrorMessage("Client is required to be set.");
            LOG.warning("Client is required to be set.");
            return false;
        }
        File clientMailBox = new File(mailBoxDirectory+"/"+e.getClient());
        if(!clientMailBox.exists()) {
            e.addErrorMessage("Client mailbox does not exist.");
            return true;
        }
        if(!clientMailBox.canRead()) {
            e.addErrorMessage("Unable to read client mailbox.");
            return false;
        }
        File[] messageFiles = clientMailBox.listFiles();
        List<Envelope> mail = new ArrayList<>();
        for(File msgFile : messageFiles) {
            try {
                Envelope envelope = Envelope.documentFactory();
                envelope.fromJSON(new String(FileUtil.readFile(msgFile.getAbsolutePath())));
                mail.add(envelope);
            } catch (IOException ex) {
                LOG.warning(ex.getLocalizedMessage());
                e.addErrorMessage("Failure occurred loading envelope from file: "+msgFile.getAbsolutePath());
            }
        }
        e.addNVP("ra.maildrop.Mail", mail);
        return true;
    }

    private void clean(Envelope e) {
        // Remove all messages for peer/client
        if(e.getClient()==null || e.getClient().isEmpty()) {
            e.addErrorMessage("Client is required to be set.");
            LOG.warning("Client is required to be set.");
            return;
        }
        File clientMailBox = new File(mailBoxDirectory+"/"+e.getClient());
        if(clientMailBox.exists() && clientMailBox.canWrite()) {
            File[] messageFiles = clientMailBox.listFiles();
            if(messageFiles!=null) {
                for (File msgFile : messageFiles) {
                    msgFile.delete();
                }
            }
        }
    }

    @Override
    public boolean start(Properties p) {
        super.start(p);
        LOG.info("Starting...");
        updateStatus(ServiceStatus.STARTING);
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
        updateStatus(ServiceStatus.RUNNING);
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
