package com.cloudera.fce.envelope.storage;

import java.util.Properties;

import org.kududb.client.KuduClient;
import org.kududb.client.KuduSession;
import org.kududb.client.SessionConfiguration.FlushMode;

public class KuduStorageSystem extends StorageSystem {

    public KuduStorageSystem(Properties props) {
        super(props);
    }

    private KuduClient client;
    private KuduSession session;
    
    @Override
    public void connect() throws Exception {
        String masterAddresses = props.getProperty("kudu.connection");
        client = new KuduClient.KuduClientBuilder(masterAddresses).build();
        session = client.newSession();
        
        // We don't want to silently drop duplicates because there shouldn't be any.
        session.setIgnoreAllDuplicateRows(false);
        // Tell the Kudu client that we will control when we want it to flush operations.
        // Without this we would flush individual operations and throughput would plummet.
        session.setFlushMode(FlushMode.MANUAL_FLUSH);
    }
    
    @Override
    public void disconnect() throws Exception {
        session.close();
        client.shutdown();
    }
    
    public KuduClient getClient() { return client; }
    
    public KuduSession getSession() { return session; }

    @Override
    protected StorageTable tableFor(Properties props) throws Exception {
        return new KuduStorageTable(this, props.getProperty("storage.table.name"));
    }

}
