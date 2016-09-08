package liquibase.ext.hibernate.database;

import java.util.Map;
import java.util.Set;
import javax.persistence.metamodel.ManagedType;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.customfactory.CustomEjb3ConfigurationFactory;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
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
    protected Metadata buildConfigurationfromFile(HibernateConnection connection) throws DatabaseException {

        MyHibernatePersistenceProvider persistenceProvider = new MyHibernatePersistenceProvider();
        EntityManagerFactoryBuilderImpl builder = (EntityManagerFactoryBuilderImpl) persistenceProvider
                .getEntityManagerFactoryBuilderOrNull(connection.getPath(), connection.getProperties(), null);
        String dialectFromXml = (String) builder.getConfigurationValues().get(AvailableSettings.DIALECT);
        String physicalNamingStrategyFromXml = (String) builder.getConfigurationValues()
                .get(AvailableSettings.PHYSICAL_NAMING_STRATEGY);
        String implicitNamingStrategyFromXml = (String) builder.getConfigurationValues()
                .get(AvailableSettings.IMPLICIT_NAMING_STRATEGY);

        StandardServiceRegistryBuilder standardServiceRegistryBuilder = new StandardServiceRegistryBuilder();
        standardServiceRegistryBuilder.applySetting(AvailableSettings.DIALECT,
                configureDialect(connection.getProperties().getProperty(AvailableSettings.DIALECT, dialectFromXml)));

        MetadataSources metadataSources = new MetadataSources(standardServiceRegistryBuilder.build());
        Set<ManagedType<?>> managedTypes = builder.build().getMetamodel().getManagedTypes();
        for (ManagedType<?> mt : managedTypes) {
            Class<?> javaType = mt.getJavaType();
            if (javaType == null) {
                continue;
            }
            metadataSources.addAnnotatedClass(javaType);
        }

        Package[] packages = Package.getPackages();
        for (Package p : packages) {
            metadataSources.addPackage(p);
        }

        MetadataBuilder metadataBuilder = metadataSources.getMetadataBuilder();
        configurePhysicalNamingStrategy(metadataBuilder, physicalNamingStrategyFromXml);
        configureImplicitNamingStrategy(metadataBuilder, implicitNamingStrategyFromXml);

        return metadataBuilder.build();
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
