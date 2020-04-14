package sk.upjs.nosql_cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.cql.CqlTemplate;

import java.net.InetSocketAddress;

@Configuration
public class CassandraConfig {

    static String  HOST = "localhost";
    static String KEYSPACE = "ks_dorcak";

    @Bean
    public Session getSession(){
        Cluster cluster = Cluster.builder().addContactPoint(HOST).build();
        return cluster.connect(KEYSPACE);
    }
    @Bean
    public CqlTemplate cqlTemplate(Session session){
        return new CqlTemplate(session);
    }
}
