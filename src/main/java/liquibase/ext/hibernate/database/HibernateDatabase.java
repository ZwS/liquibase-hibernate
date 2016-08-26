package liquibase.ext.hibernate.database;

import liquibase.database.AbstractJdbcDatabase;
import liquibase.database.DatabaseConnection;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.DatabaseException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import liquibase.ext.hibernate.database.connection.HibernateDriver;
import liquibase.logging.LogFactory;
import liquibase.logging.Logger;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MySQLDialect;
import org.hibernate.jpa.event.spi.JpaIntegrator;

/**
 * Base class for all Hibernate Databases. This extension interacts with Hibernate by creating standard
 * liquibase.database.Database implementations that bridge what Liquibase expects and the Hibernate APIs.
 */
public abstract class HibernateDatabase extends AbstractJdbcDatabase {

    protected static final Logger LOG = LogFactory.getLogger("liquibase-hibernate");
    public static final String DEFAULT_SCHEMA = "HIBERNATE";

    private Metadata metadata;

    private Dialect dialect;

    private boolean indexesForForeignKeys = false;

    public HibernateDatabase() {
        setDefaultCatalogName(DEFAULT_SCHEMA);
        setDefaultSchemaName(DEFAULT_SCHEMA);
    }

    @Override
    public void setConnection(DatabaseConnection conn) {
        super.setConnection(conn);

        try {
            LOG.info("Reading hibernate configuration " + getConnection().getURL());

            this.metadata = obtainMetadata(((HibernateConnection) ((JdbcConnection) conn).getUnderlyingConnection()));
            this.dialect = configureDialect();

            afterSetup();
        } catch (DatabaseException e) {
            throw new UnexpectedLiquibaseException(e);
        }

    }

    /**
     * Return the dialect used by hibernate
     */
    protected Dialect configureDialect() {
        return metadata.getDatabase().getDialect();
    }

    /**
     * Configures the naming strategy use by the connection
     *
     * @param serviceRegistryBuilder
     * @param connection the {@link HibernateConnection}
     */
    protected void configurePhysicalNamingStrategy(StandardServiceRegistryBuilder serviceRegistryBuilder, HibernateConnection connection) {
        String physicalNamingStrategy = connection.getProperties().getProperty("hibernate.physical_naming_strategy");

        if (physicalNamingStrategy != null) {
            serviceRegistryBuilder.applySetting(Environment.PHYSICAL_NAMING_STRATEGY, physicalNamingStrategy);
        }
    }

    ;

/**
 * Perform any post-configuration setting logic.
 */
protected void afterSetup() {
        if (dialect instanceof MySQLDialect) {
            indexesForForeignKeys = true;
        }
    }

    /**
     * Concrete implementations use this method to create the hibernate Configuration object based on the passed URL
     */
    protected abstract Metadata obtainMetadata(HibernateConnection conn) throws DatabaseException;
    
    protected BootstrapServiceRegistry buildBootstrapServiceRegistry() {
        final BootstrapServiceRegistryBuilder bsrBuilder = new BootstrapServiceRegistryBuilder();

		bsrBuilder.applyIntegrator(new JpaIntegrator());
        
        return bsrBuilder.build();
    }

    @Override
    public boolean requiresPassword() {
        return false;
    }

    @Override
    public boolean requiresUsername() {
        return false;
    }

    @Override
    public String
            getDefaultDriver(String url) {
        if (url.startsWith("hibernate")) {
            return HibernateDriver.class
                    .getName();
        }
        return null;
    }

    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }

    @Override
    public boolean createsIndexesForForeignKeys() {
        return indexesForForeignKeys;
    }

    @Override
    public Integer getDefaultPort() {
        return 0;
    }

    @Override
    public boolean supportsInitiallyDeferrableColumns() {
        return false;
    }

    @Override
    public boolean supportsTablespaces() {
        return false;
    }

    public Dialect getDialect() throws DatabaseException {
        return dialect;
    }

    @Override
    protected String getConnectionCatalogName() throws DatabaseException {
        return getDefaultCatalogName();
    }

    @Override
    protected String getConnectionSchemaName() {
        return getDefaultSchemaName();
    }

    @Override
    public String getDefaultSchemaName() {
        return DEFAULT_SCHEMA;
    }

    @Override
    public String getDefaultCatalogName() {
        return DEFAULT_SCHEMA;
    }

    @Override
    public boolean isSafeToRunUpdate() throws DatabaseException {
        return true;
    }

    @Override
    public boolean isCaseSensitive() {
        return false;
    }

    @Override
    public boolean supportsSchemas() {
        return true;
    }

    @Override
    public boolean supportsCatalogs() {
        return false;
    }

    public Metadata getMetadata() {
        return metadata;
    }
}
