package liquibase.ext.hibernate.customfactory;

import liquibase.ext.hibernate.database.HibernateDatabase;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import org.hibernate.boot.Metadata;

/**
 * Implement this interface to dynamically generate a hibernate:classic configuration.
 * For example, if you create a class called com.example.hibernate.MyConfig, specify a url of hibernate:classic:com.example.hibernate.MyConfig.
 */
public interface CustomClassicConfigurationFactory {

    /**
     * Create a hibernate Metadata for the given database and connection.
     */
    Metadata getConfiguration(HibernateDatabase hibernateDatabase, HibernateConnection connection);

}
