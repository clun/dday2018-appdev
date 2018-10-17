package com.datastax.exercise;


import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.dse.DseCluster;
import com.datastax.driver.dse.DseCluster.Builder;
import com.datastax.driver.dse.DseSession;

/**
 * EXERCISE 1 : Initializing connection to Cassandra 
 *
 * @author YOU
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Exercise1 {
     
    @Test
    public void test1_initializeConnection() {
        
        // Builder
        System.out.print("- Setup Builder");
        Builder clusterBuilder = DseCluster.builder();
        
        /**
         * EXERCICE 1 :
         * Please provide the IP nodes of your cluster, which one is a Seed.
         * (1) - Connect to OpsCenter and list the IP of the 3 nodes in your cluster
         * (2) - Locate the Seed Node mark with a 'Star' in the list view
         * (3) - Fill the contact points with IP
         **/
        // =========================>
        clusterBuilder.addContactPoint("localhost");
        // <=========================
        
        // --- Reconnection POLICY
        clusterBuilder.withReconnectionPolicy(new ExponentialReconnectionPolicy(1000, 2000));
        // --- Retry POLICY
        clusterBuilder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
        // --- Load-Balancing
        LoadBalancingPolicy lbPolicy = DCAwareRoundRobinPolicy.builder().withLocalDc("SearchGraphAnalytics").build();
        clusterBuilder.withLoadBalancingPolicy(new TokenAwarePolicy(lbPolicy));
        // --- Options
        QueryOptions queryOptions = new QueryOptions();
        queryOptions.setConsistencyLevel(ConsistencyLevel.QUORUM);
        queryOptions.setFetchSize(1000);
        clusterBuilder.withQueryOptions(queryOptions);
        clusterBuilder.withPort(9042);
        System.out.println("\t\t[OK]");
        
        // Build
        System.out.print("- build()   => Cluster");
        DseCluster dseCluster = clusterBuilder.build();
        System.out.println("\t[OK]");
        
        try {
            
            // Connect
            System.out.print("- connect() => Session");
            DseSession dseSession = dseCluster.connect("killrvideo");
            Assert.assertEquals("killrvideo", dseSession.getLoggedKeyspace());
            System.out.println("\t[OK]");
            System.out.println("Congratulations, you complete Exercise #1");
            
        } catch(RuntimeException e) {
            
            System.out.println("\t[KO]");
            System.out.println("ERROR : " + e.getMessage());
            fail(e.getMessage());
            
        } finally {
            
            // Always, Always, Always cleanup
            dseCluster.close();
        }
    }
    
}
