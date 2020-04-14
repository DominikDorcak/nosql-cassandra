package sk.upjs.nosql_cassandra;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.DriverException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.cql.ResultSetExtractor;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.data.cassandra.core.cql.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.DropTableSpecification;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

public class App {
    public static void main(String[] args) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(CassandraConfig.class);
        CqlTemplate template = context.getBean(CqlTemplate.class);
        Session session = context.getBean(Session.class);

        template.execute("CREATE TABLE IF NOT EXISTS test (id uuid primary key,event text)");
        Long start = System.nanoTime();
        for (int i = 0; i <10 ; i++) {
            template.execute("INSERT INTO test (id,event) VALUES (?,?)", UUID.randomUUID(),"udalost cislo "+ (int) (1000*Math.random()));
        }
        System.out.println("Prvy insert: " + ((System.nanoTime() - start) / 1000000d) + " ms");
        template.query("SELECT * FROM test", new RowMapper<Void>() {
            @Override
            public Void mapRow(Row row, int i) throws DriverException {
                System.out.println("id: " + row.getUUID("id") + ", event: " + row.getString("event"));
                return null;
            }
        });

        template.execute("DROP TABLE test");

        CreateTableSpecification specification =  CreateTableSpecification.createTable("test2")
                .partitionKeyColumn("id_dept", DataType.bigint())
                .clusteredKeyColumn("name",DataType.text())
                .column("salary",DataType.decimal());


        template.execute(CreateTableCqlGenerator.toCql(specification));

        start = System.nanoTime();
        PreparedStatement statement = session.prepare("INSERT INTO test2(id_dept, name, salary) VALUES (?,?,?)");
        for (int i = 0; i <10 ; i++) {
            BoundStatement bind = statement.bind((long) (3*Math.random())+1,
                    "clovek" + (long) (10000*Math.random())+1,
                    new BigDecimal(Math.random()*5000).setScale(2, RoundingMode.HALF_UP));
            template.execute(bind);
        }
        System.out.println("Druhy insert: " + ((System.nanoTime() - start) / 1000000d) + " ms");

        start = System.nanoTime();
        BatchStatement batchStatement = new BatchStatement();
        statement = session.prepare("INSERT INTO test2(id_dept, name, salary) VALUES (?,?,?)");
        for (int i = 0; i <10 ; i++) {
            BoundStatement bind = statement.bind((long) (3*Math.random())+1,
                    "clovek" + (long) (10000*Math.random())+1,
                    new BigDecimal(Math.random()*5000).setScale(2, RoundingMode.HALF_UP));
                    batchStatement.add(bind);

        }
        template.execute(batchStatement);
        System.out.println("Treti insert: " + ((System.nanoTime() - start) / 1000000d) + " ms");

        template.query("SELECT * FROM test2", new ResultSetExtractor<Void>() {

            @Override
            public Void extractData(ResultSet resultSet) throws DriverException, DataAccessException {
                resultSet.forEach((row -> {
                    System.out.println("dept: " + row.getLong("id_dept")
                     + " name: " + row.getString("name")
                     + " salary: " + row.getDecimal("salary")
                    );
                }));
                return null;
            }
        });

        DropTableSpecification dropTableSpecification = DropTableSpecification.dropTable("test2");
        template.execute(DropTableCqlGenerator.toCql(dropTableSpecification));

        session.getCluster().close();
        context.close();
    }
}
