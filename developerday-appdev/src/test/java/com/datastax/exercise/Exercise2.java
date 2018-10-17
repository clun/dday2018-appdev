package com.datastax.exercise;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.datastax.appdev.AbstractTest;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * (1) - Execute the test 'listUsers' to see the content of table users. (notice LIMIT to avoid to much data)
 * (2) - Select on userid and fill the attribute 'sampleUserId' with the value ex : 4c979e80-b8b1-4adb-a2ac-37f78005538b
 * 
 *
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Exercise2 extends AbstractTest {

    private static PreparedStatement pStatement;
    
    @BeforeClass
    public static void init() {
        dseSession = getSession("localhost");
        
        pStatement = dseSession.prepare(QueryBuilder
                .select("userid")
                .from("killrvideo", "users")
                .where(QueryBuilder.eq("userid", QueryBuilder.bindMarker())))
                .setConsistencyLevel(ConsistencyLevel.ONE);
    }
    
    @Test
    public void test21_listUsers() {
        System.out.println("Table users :");
        for (Row row : dseSession.execute(
                  "SELECT * "
                + "FROM users "
                + "LIMIT 10").all()) {
            System.out.println("- Userid=" + row.getUUID("userid") + ", email=" + row.getString("email"));
        }
    }
    
    // -- FILL THE SAMPLE USERID --
    private UUID sampleUserId = UUID.fromString("4c979e80-b8b1-4adb-a2ac-37f78005538b");
    
    @Test
    public void test22_isUserExistQueryString() {
        ResultSet rs = dseSession.execute(""
                + "SELECT userid "
                + "FROM users "
                + "WHERE userid = " + sampleUserId);
        // Then, There is a value returned
        Assert.assertFalse(rs.isExhausted());
        // Get Row
        Row row = rs.one();
        // Read attributes
        Assert.assertEquals(sampleUserId, row.getUUID("userid"));
    }
    
    @Test
    public void test23_isUserExistSimpleStatement() {
        SimpleStatement statement = new SimpleStatement(""
                + "SELECT userid "
                + "FROM users "
                + "WHERE userid = ?", sampleUserId);
        // Then, There is a value returned
        Assert.assertFalse(dseSession.execute(statement).isExhausted());
    }
    
    @Test
    public void test24_isKeyspaceExistPrepareStatement() {
        BoundStatement bStatement = pStatement.bind().setUUID(0, sampleUserId);
        Assert.assertFalse(dseSession.execute(bStatement).isExhausted());
        System.out.println("Congratulations, you complete Exercise #2");
    }
    
    @AfterClass
    public static void destoy() {
        dseSession.getCluster().close();
    }
    
}
