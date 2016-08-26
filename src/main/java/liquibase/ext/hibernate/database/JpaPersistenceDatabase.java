package liquibase.ext.hibernate.database;

import java.util.Collections;
import javax.persistence.spi.PersistenceUnitInfo;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import liquibase.ext.hibernate.database.connection.HibernateDriver;
import org.hibernate.boot.Metadata;
import org.hibernate.jpa.boot.internal.EntityManagerFactoryBuilderImpl;
import org.hibernate.jpa.boot.spi.Bootstrap;
import org.springframework.orm.jpa.persistenceunit.DefaultPersistenceUnitManager;

/**
 * Database implementation for JPA configurations.
 * This supports passing a JPA persistence XML file reference.
 */
public class JpaPersistenceDatabase extends HibernateDatabase {

    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return conn.getURL().startsWith("jpa:persistence:");
    }

    @Override
    public String getDefaultDriver(String url) {
        if (url.startsWith("jpa:persistence:")) {
            return HibernateDriver.class.getName();
        }
        return null;
    }

    @Override
    protected Metadata obtainMetadata(HibernateConnection connection) throws DatabaseException {
        return buildConfigurationFromXml(connection);
    }

    /**
     * Build a Configuration object assuming the connection path is a persistence XML configuration file.
     */

    protected Metadata buildConfigurationFromXml(HibernateConnection connection) {
        DefaultPersistenceUnitManager internalPersistenceUnitManager = new DefaultPersistenceUnitManager();

        internalPersistenceUnitManager.setPersistenceXmlLocation(connection.getPath());
        internalPersistenceUnitManager.setDefaultPersistenceUnitRootLocation(null);

        internalPersistenceUnitManager.preparePersistenceUnitInfos();
        PersistenceUnitInfo persistenceUnitInfo = internalPersistenceUnitManager.obtainDefaultPersistenceUnitInfo();

        EntityManagerFactoryBuilderImpl builder = (EntityManagerFactoryBuilderImpl) Bootstrap.getEntityManagerFactoryBuilder(persistenceUnitInfo,
                Collections.emptyMap(), (ClassLoader) null);
        builder.build();
        return builder.getMetadata();
    }


    @Override
    public String getShortName() {
        return "jpaPersistence";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "JPA Persistence";
    }

}
