package liquibase.ext.hibernate.database;

import java.util.Map;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.customfactory.CustomEjb3ConfigurationFactory;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import org.hibernate.boot.Metadata;
import org.hibernate.jpa.HibernatePersistenceProvider;
import org.hibernate.jpa.boot.internal.EntityManagerFactoryBuilderImpl;
import org.hibernate.jpa.boot.spi.EntityManagerFactoryBuilder;

/**
 * Database implementation for "ejb3" hibernate configurations.
 * This supports passing an persistence unit name or a {@link liquibase.ext.hibernate.customfactory.CustomEjb3ConfigurationFactory} implementation
 */
public class HibernateEjb3Database extends HibernateDatabase {

    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return conn.getURL().startsWith("hibernate:ejb3:");
    }

    @Override
    protected Metadata obtainMetadata(HibernateConnection connection) throws DatabaseException {

        if (isCustomFactoryClass(connection.getPath())) {
            return buildConfigurationFromFactory(connection);
        } else {
            return buildConfigurationfromFile(connection);
        }
    }
    /**
     * Build a Configuration object assuming the connection path is a hibernate XML configuration file.
     */
    protected Metadata buildConfigurationfromFile(HibernateConnection connection) {

        MyHibernatePersistenceProvider persistenceProvider = new MyHibernatePersistenceProvider();
        EntityManagerFactoryBuilderImpl builder = (EntityManagerFactoryBuilderImpl) persistenceProvider.getEntityManagerFactoryBuilderOrNull(connection.getPath(), connection.getProperties(), null);
        builder.build();
        return builder.getMetadata();
    }

    /**
     * Build a Configuration object assuming the connection path is a {@link CustomEjb3ConfigurationFactory} class name
     */
    protected Metadata buildConfigurationFromFactory(HibernateConnection connection) throws DatabaseException {
        try {
            return ((CustomEjb3ConfigurationFactory) Class.forName(connection.getPath()).newInstance()).getConfiguration(this, connection);
        } catch (InstantiationException e) {
            throw new DatabaseException(e);
        } catch (IllegalAccessException e) {
            throw new DatabaseException(e);
        } catch (ClassNotFoundException e) {
            throw new DatabaseException(e);
        }
    }


    /**
     * Return true if the given path is a {@link CustomEjb3ConfigurationFactory}
     */
    protected boolean isCustomFactoryClass(String path) {
        if (path.contains("/")) {
            return false;
        }

        try {
            Class<?> clazz = Class.forName(path);
            return CustomEjb3ConfigurationFactory.class.isAssignableFrom(clazz);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public String getShortName() {
        return "hibernateEjb3";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "Hibernate EJB3";
    }

    private static class MyHibernatePersistenceProvider extends HibernatePersistenceProvider {
        @Override
        protected EntityManagerFactoryBuilder getEntityManagerFactoryBuilderOrNull(String persistenceUnitName, Map properties, ClassLoader providedClassLoader) {
            return super.getEntityManagerFactoryBuilderOrNull(persistenceUnitName, properties, providedClassLoader);
        }
    }
}
