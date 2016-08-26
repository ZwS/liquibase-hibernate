package liquibase.ext.hibernate.database;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.persistence.spi.PersistenceUnitInfo;
import javax.xml.parsers.DocumentBuilderFactory;
import liquibase.database.DatabaseConnection;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.jpa.boot.internal.EntityManagerFactoryBuilderImpl;
import org.hibernate.jpa.boot.spi.Bootstrap;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyValue;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.TypedStringValue;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.ManagedProperties;
import org.springframework.beans.factory.support.SimpleBeanDefinitionRegistry;
import org.springframework.beans.factory.xml.XmlBeanDefinitionReader;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.orm.hibernate4.LocalSessionFactoryBean;
import org.springframework.orm.jpa.persistenceunit.DefaultPersistenceUnitManager;
import org.springframework.orm.jpa.persistenceunit.SmartPersistenceUnitInfo;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;

/**
 * Database implementation for "spring" hibernate configurations.
 * This supports passing a spring XML file reference and bean name or a package containing hibernate annotated classes.
 */
public class HibernateSpringDatabase extends HibernateDatabase {

    public boolean isCorrectDatabaseImplementation(DatabaseConnection conn) throws DatabaseException {
        return conn.getURL().startsWith("hibernate:spring:");
    }

    @Override
    protected Metadata obtainMetadata(HibernateConnection connection) throws DatabaseException {
        if (isXmlFile(connection.getPath())) {
            return buildConfigurationFromXml(connection);
        } else {
            return buildConfigurationFromScanning(connection);
        }
    }

    /**
     * Return true if the given path is a spring XML file.
     */
    protected boolean isXmlFile(String path) {
        if (path.contains("/")) {
            return true;
        }
        ClassPathResource resource = new ClassPathResource(path);
        try {
            if (resource.exists() && !resource.getFile().isDirectory()) {
                return true;
            } else {
                return false;
            }
        } catch (IOException e) {
            return false;
        }


    }

    /**
     * Parse the given URL assuming it is a spring XML file
     */
    protected Metadata buildConfigurationFromXml(HibernateConnection connection) throws DatabaseException {

        // Disable xml validation
        DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        documentBuilderFactory.setValidating(false);

        // Read configuration
        BeanDefinitionRegistry registry = new SimpleBeanDefinitionRegistry();
        XmlBeanDefinitionReader reader = new XmlBeanDefinitionReader(registry);
        reader.setNamespaceAware(true);
        reader.loadBeanDefinitions(new ClassPathResource(connection.getPath()));

        Properties props = connection.getProperties();
        Class<? extends LocalSessionFactoryBean> beanClass = LocalSessionFactoryBean.class;

        String beanName = props.getProperty("bean", null);
        String beanClassName = props.getProperty("beanClass", null);

        if (beanClassName != null) {
            beanClass = findClass(beanClassName, beanClass);
        }

        if (beanName == null) {
            throw new IllegalStateException("A 'bean' name is required, matching a '" + beanClassName + "' definition in '" + connection.getPath() + "'.");
        }

        BeanDefinition beanDef = registry.getBeanDefinition(beanName);
        if (beanDef == null) {
            throw new IllegalStateException("A bean named '" + beanName + "' could not be found in '" + connection.getPath() + "'.");
        }

        MutablePropertyValues properties = beanDef.getPropertyValues();

        StandardServiceRegistryBuilder standardServiceRegistryBuilder = new StandardServiceRegistryBuilder();
        ManagedProperties hibernateProperties = (ManagedProperties) properties.getPropertyValue("hibernateProperties").getValue();

        if (hibernateProperties != null) {
            Properties configurationProperties = new Properties();
            for (Map.Entry<?, ?> entry : hibernateProperties.entrySet()) {
                TypedStringValue key = (TypedStringValue) entry.getKey();
                TypedStringValue value = (TypedStringValue) entry.getValue();

                configurationProperties.setProperty(key.getValue(), value.getValue());
            }

            standardServiceRegistryBuilder.applySettings(configurationProperties);
        } else {
            throw new IllegalStateException("Please provide a 'hibernateProperties' property set to define the hibernate connection settings.");
        }

        MetadataSources metadataSources = new MetadataSources(standardServiceRegistryBuilder.build());

        // Add annotated classes list.
        PropertyValue annotatedClassesProperty = properties.getPropertyValue("annotatedClasses");
        if (annotatedClassesProperty != null) {
            List<TypedStringValue> annotatedClasses = (List<TypedStringValue>) annotatedClassesProperty.getValue();
            if (annotatedClasses != null) {
                for (TypedStringValue className : annotatedClasses) {
                    LOG.info("Found annotated class " + className.getValue());
                    metadataSources.addAnnotatedClass(findClass(className.getValue()));
                }
            }
        }

        try {
            // Add mapping locations
            PropertyValue mappingLocationsProp = properties.getPropertyValue("mappingLocations");
            if (mappingLocationsProp != null) {
                List<TypedStringValue> mappingLocations = (List<TypedStringValue>) mappingLocationsProp.getValue();
                if (mappingLocations != null) {
                    ResourcePatternResolver resourcePatternResolver = new PathMatchingResourcePatternResolver();
                    for (TypedStringValue mappingLocation : mappingLocations) {
                        LOG.info("Found mappingLocation " + mappingLocation.getValue());
                        Resource[] resources = resourcePatternResolver.getResources(mappingLocation.getValue());
                        for (int i = 0; i < resources.length; i++) {
                            LOG.info("Adding resource  " + resources[i].getURL());
                            metadataSources.addInputStream(resources[i].getInputStream());
                        }
                    }
                }
            }
        } catch (Exception e) {
            if (e instanceof DatabaseException) {
                throw (DatabaseException) e;
            } else {
                throw new DatabaseException(e);
            }
        }

        return metadataSources.buildMetadata();
    }

    private Class<?> findClass(String className) {
        return findClass(className, Object.class);
    }

    private <T> Class<? extends T> findClass(String className, Class<T> superClass) {
        try {
            Class<?> newClass = Class.forName(className);
            if (superClass.isAssignableFrom(newClass)) {
                return newClass.asSubclass(superClass);
            } else {
                throw new IllegalStateException("The provided class '" + className + "' is not assignable from the '" + superClass.getName() + "' superclass.");
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unable to find required class: '" + className + "'. Please check classpath and class name.");
        }
    }

    /**
     * Build hibernate configuration assuming the passed connection URL is a package to scan
     * @param connection
     * @return
     */
    public Metadata buildConfigurationFromScanning(HibernateConnection connection) {
        String[] packagesToScan = connection.getPath().split(",");

        for (String packageName : packagesToScan) {
            LOG.info("Found package "+packageName);
        }

        DefaultPersistenceUnitManager internalPersistenceUnitManager = new DefaultPersistenceUnitManager();

        internalPersistenceUnitManager.setPackagesToScan(packagesToScan);

        String dialectName = connection.getProperties().getProperty("dialect", null);
        if (dialectName == null) {
            throw new IllegalArgumentException("A 'dialect' has to be specified.");
        }
        LOG.info("Found dialect "+dialectName);

        internalPersistenceUnitManager.preparePersistenceUnitInfos();
        PersistenceUnitInfo persistenceUnitInfo = internalPersistenceUnitManager.obtainDefaultPersistenceUnitInfo();
        HibernateJpaVendorAdapter jpaVendorAdapter = new HibernateJpaVendorAdapter();
        jpaVendorAdapter.setDatabasePlatform(dialectName);

        String enhancedId = connection.getProperties().getProperty("hibernate.enhanced_id", "false");
        LOG.info("Found hibernate.enhanced_id" + enhancedId);

        Map<String, Object> jpaPropertyMap = jpaVendorAdapter.getJpaPropertyMap();
        jpaPropertyMap.put("hibernate.archive.autodetection", "false");
        jpaPropertyMap.put("hibernate.id.new_generator_mappings", enhancedId);

        if (persistenceUnitInfo instanceof SmartPersistenceUnitInfo) {
            ((SmartPersistenceUnitInfo) persistenceUnitInfo).setPersistenceProviderPackageName(jpaVendorAdapter.getPersistenceProviderRootPackage());
        }

        EntityManagerFactoryBuilderImpl builder = (EntityManagerFactoryBuilderImpl) Bootstrap.getEntityManagerFactoryBuilder(persistenceUnitInfo,
                jpaPropertyMap);
        builder.build();
        return builder.getMetadata();

    }

    @Override
    public String getShortName() {
        return "hibernateSpring";
    }

    @Override
    protected String getDefaultDatabaseProductName() {
        return "Hibernate Spring";
    }
}
