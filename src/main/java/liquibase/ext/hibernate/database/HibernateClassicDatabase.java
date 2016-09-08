package liquibase.ext.hibernate.database;

import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.customfactory.CustomClassicConfigurationFactory;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;

/**
 * Database implementation for "classic" hibernate configurations.
 * This supports passing a hibernate xml configuration file or a {@link CustomClassicConfigurationFactory} implementation
 */
public class HibernateClassicDatabase extends HibernateDatabase {

    @Override
    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return conn.getURL().startsWith("hibernate:classic:");
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
     * Build a Configuration object assuming the connection path is a {@link CustomClassicConfigurationFactory} class name
     */
    protected Metadata buildConfigurationFromFactory(HibernateConnection connection) throws DatabaseException {
        try {
            return ((CustomClassicConfigurationFactory) Class.forName(connection.getPath()).newInstance()).getConfiguration(this, connection);
        } catch (InstantiationException e) {
            throw new DatabaseException(e);
        } catch (IllegalAccessException e) {
            throw new DatabaseException(e);
        } catch (ClassNotFoundException e) {
            throw new DatabaseException(e);
        }
    }

    /**
     * Build a Configuration object assuming the connection path is a hibernate XML configuration file.
     */
    protected Metadata buildConfigurationfromFile(HibernateConnection connection) throws DatabaseException {
        StandardServiceRegistryBuilder standardServiceRegistryBuilder = new StandardServiceRegistryBuilder();
        standardServiceRegistryBuilder.configure(connection.getPath());
        String dialectFromXml = (String) standardServiceRegistryBuilder.getAggregatedCfgXml().getConfigurationValues()
                .get(AvailableSettings.DIALECT);
        String physicalNamingStrategyFromXml = (String) standardServiceRegistryBuilder.getAggregatedCfgXml().getConfigurationValues()
                .get(AvailableSettings.PHYSICAL_NAMING_STRATEGY);
        String implicitNamingStrategyFromXml = (String) standardServiceRegistryBuilder.getAggregatedCfgXml().getConfigurationValues()
                .get(AvailableSettings.IMPLICIT_NAMING_STRATEGY);
        standardServiceRegistryBuilder.applySetting(AvailableSettings.DIALECT,
                configureDialect(connection.getProperties().getProperty(AvailableSettings.DIALECT, dialectFromXml)));

        MetadataSources metadataSources = new MetadataSources(standardServiceRegistryBuilder.build());
        MetadataBuilder metadataBuilder = metadataSources.getMetadataBuilder();
        configurePhysicalNamingStrategy(metadataBuilder, physicalNamingStrategyFromXml);
        configureImplicitNamingStrategy(metadataBuilder, implicitNamingStrategyFromXml);

        return metadataBuilder.build();
    }

    /**
     * Returns true if the given path is a factory class
     */
    protected boolean isCustomFactoryClass(String path) {
        if (path.contains("/")) {
            return false;
        }

        try {
            Class<?> clazz = Class.forName(path);
            return CustomClassicConfigurationFactory.class.isAssignableFrom(clazz);
        } catch (ClassNotFoundException e) {
            return false;
        }
    }

    @Override
    public String getShortName() {
        return "hibernateClassic";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "Hibernate Classic";
    }
}