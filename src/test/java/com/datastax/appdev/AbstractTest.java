package com.datastax.appdev;

import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseSession;

public abstract class AbstractTest {
    
    protected static DseSession dseSession;
    
    protected static DseSession getSession(String... ip) {
       return DseCluster.builder()
                        .withPort(9042)
                        .addContactPoints(ip).build()
                        .connect("killrvideo");
    }

}
