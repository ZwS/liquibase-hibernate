package liquibase.ext.hibernate.database;

import java.util.Collections;
import java.util.Set;
import javax.persistence.metamodel.ManagedType;
import javax.persistence.spi.PersistenceUnitInfo;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import liquibase.ext.hibernate.database.connection.HibernateDriver;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataBuilder;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.AvailableSettings;
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

    protected Metadata buildConfigurationFromXml(HibernateConnection connection) throws DatabaseException {
        DefaultPersistenceUnitManager internalPersistenceUnitManager = new DefaultPersistenceUnitManager();

        internalPersistenceUnitManager.setPersistenceXmlLocation(connection.getPath());
        internalPersistenceUnitManager.setDefaultPersistenceUnitRootLocation(null);

        internalPersistenceUnitManager.preparePersistenceUnitInfos();
        PersistenceUnitInfo persistenceUnitInfo = internalPersistenceUnitManager.obtainDefaultPersistenceUnitInfo();

        EntityManagerFactoryBuilderImpl builder = (EntityManagerFactoryBuilderImpl) Bootstrap.getEntityManagerFactoryBuilder(persistenceUnitInfo,
                Collections.emptyMap(), (ClassLoader) null);
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


    @Override
    public String getShortName() {
        return "jpaPersistence";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "JPA Persistence";
    }

}
