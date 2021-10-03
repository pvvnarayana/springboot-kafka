package mflix.config;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

@Configuration
@Service
public class MongoDBConfiguration {

    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public MongoClient mongoClient(@Value("${spring.mongodb.uri}") String connectionString) {

        ConnectionString connString = new ConnectionString(connectionString);

        //TODO> Ticket: Handling Timeouts - configure the expected
        // WriteConcern `wtimeout` and `connectTimeoutMS` values
        
        WriteConcern writeConcern = new WriteConcern("majority").withWTimeout(2500L, TimeUnit.MILLISECONDS);
        MongoClientSettings mongoClientSettings =  MongoClientSettings.builder().writeConcern(writeConcern).applyConnectionString(connString).build();
        	
        MongoClient mongoClient = MongoClients.create(mongoClientSettings);
        
        /**
         WriteConcern wc = WriteConcern.MAJORITY.withWTimeout(2500,
              TimeUnit.MILLISECONDS);
      MongoClientSettings settings =
              MongoClientSettings.builder()
                      .applyConnectionString(this.connectionString)
                      .writeConcern(wc)
                      .build();
         */
        return mongoClient;
    }
}
