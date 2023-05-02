package com.gridgain;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class ClusterClient {

    private Ignite ignite = null;

    public ClusterClient() {
        System.setProperty("IGNITE_EVENT_DRIVEN_SERVICE_PROCESSOR_ENABLED", "true");
    }

    public Ignite startClient() {
        ignite = Ignition.start("client_config.xml");
        System.out.println("Ignite client started");
        return ignite;
    }

    public void closeClient() {
        ignite.close();
        System.out.println("Ignite client closed");
    }

}
