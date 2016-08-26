package com.example.customconfig;

import com.example.customconfig.auction.Item;
import liquibase.ext.hibernate.customfactory.CustomClassicConfigurationFactory;
import liquibase.ext.hibernate.database.HibernateDatabase;
import liquibase.ext.hibernate.database.connection.HibernateConnection;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.HSQLDialect;

public class CustomClassicConfigurationFactoryImpl implements CustomClassicConfigurationFactory {

    @Override
    public Metadata getConfiguration(HibernateDatabase hibernateDatabase, HibernateConnection connection) {
        StandardServiceRegistryBuilder standardServiceRegistryBuilder = new StandardServiceRegistryBuilder();
        standardServiceRegistryBuilder.applySetting(Environment.DIALECT, HSQLDialect.class.getName());
        MetadataSources metadataSources = new MetadataSources(standardServiceRegistryBuilder.build());
        metadataSources.addAnnotatedClass(Item.class);
        return metadataSources.buildMetadata();
    }
}
