package com.datastax.exercise;

import java.util.Date;
import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.datastax.appdev.AbstractTest;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.datastax.driver.mapping.annotations.Accessor;
import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Param;
import com.datastax.driver.mapping.annotations.PartitionKey;
import com.datastax.driver.mapping.annotations.Query;
import com.datastax.driver.mapping.annotations.Table;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class Exercise3 extends AbstractTest {
    
    @Table(keyspace = "killrvideo", name = "users")
    public static class User {
        @PartitionKey
        private UUID userid;
        @Column
        private String firstname;
        @Column
        private String lastname;
        @Column
        private String email;
        @Column(name = "created_date")
        private Date createdAt;
        // Getters, Setters
        public UUID getUserid()                    { return userid;              }
        public void setUserid(UUID userid)         { this.userid = userid;       }
        public String getFirstname()               { return firstname;           }
        public void setFirstname(String firstname) { this.firstname = firstname; }
        public String getLastname()                { return lastname;            }
        public void setLastname(String lastname)   { this.lastname = lastname;   }
        public String getEmail()                   { return email;               }
        public void setEmail(String email)         { this.email = email;         }
        public Date getCreatedAt()                 { return createdAt;           }
        public void setCreatedAt(Date createdAt)   { this.createdAt = createdAt; }
    }
    
    @Accessor
    public interface UserAccessor {
        @Query("UPDATE users SET email = :email WHERE userid = :userid")
        void updateEmail(@Param("userid") UUID userId, @Param("email") String email);
    }
    
    private static MappingManager mappingManager;
    
    private static  Mapper<User> mapperUser;
    
    private static UserAccessor userAccessor;
    
    private static UUID newUID = UUID.randomUUID();
    
    @BeforeClass
    public static void init() {
        dseSession     = getSession("localhost");
        mappingManager = new MappingManager(dseSession);
        mapperUser     = mappingManager.mapper(User.class);
        userAccessor   = mappingManager.createAccessor(UserAccessor.class);
    }
    
    @Test
    public void test31_MapperSaveNewUser() {
        User u1 = new User();
        u1.setEmail("cedrick.lunven@datastax.com");
        u1.setFirstname("Cedrick");
        u1.setLastname("Lunven");
        u1.setUserid(newUID);
        u1.setCreatedAt(new Date());
        mapperUser.save(u1);
    }
    
    @Test
    public void test32_MapperListUsers() {
        Select selectStatement = QueryBuilder.select().all().from("killrvideo", "users").limit(10);
        Result<User> res = mapperUser.map(dseSession.execute(selectStatement));
        System.out.println("Table users:");
        for (User user : res) {
           System.out.println("- userid=" + user.getUserid() + ", email=" + user.getEmail());
        }
        System.out.println("Congratulations, you complete Exercise #3");
    }
    
    @Test
    public void test33_MapperAccessors() {
        userAccessor.updateEmail(newUID, "a.a@.com");
        
    }
    
    @AfterClass
    public static void destroy() {
        dseSession.getCluster().close();
    }
}
