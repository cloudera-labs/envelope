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
        String masterAddresses = props.getProperty("connection");
        client = new KuduClient.KuduClientBuilder(masterAddresses).build();
        session = client.newSession();
        
        session.setIgnoreAllDuplicateRows(false);
        session.setFlushMode(FlushMode.AUTO_FLUSH_BACKGROUND);
    }
    
    @Override
    public void disconnect() throws Exception {
        session.close();
        client.shutdown();
    }
    
    public KuduClient getClient() { return client; }
    
    public KuduSession getSession() { return session; }
    
    @Override
    public StorageTable tableFor(Properties props) throws Exception {
        return new KuduStorageTable(this, props.getProperty("storage.table.name"));
    }
    
}
