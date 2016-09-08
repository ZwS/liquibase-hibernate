package liquibase.ext.hibernate.snapshot;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.database.HibernateDatabase;
import liquibase.ext.hibernate.snapshot.extension.ExtendedSnapshotGenerator;
import liquibase.ext.hibernate.snapshot.extension.MultipleHiLoPerTableSnapshotGenerator;
import liquibase.ext.hibernate.snapshot.extension.TableGeneratorSnapshotGenerator;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.Schema;
import liquibase.structure.core.Table;
import org.hibernate.boot.Metadata;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.RootClass;

public class TableSnapshotGenerator extends HibernateSnapshotGenerator {

    private List<ExtendedSnapshotGenerator<IdentifierGenerator, Table>> tableIdGenerators =
            new ArrayList<ExtendedSnapshotGenerator<IdentifierGenerator, Table>>();

    public TableSnapshotGenerator() {
        super(Table.class, new Class[]{Schema.class});
        tableIdGenerators.add(new MultipleHiLoPerTableSnapshotGenerator());
        tableIdGenerators.add(new TableGeneratorSnapshotGenerator());
    }

    @Override
    protected DatabaseObject snapshotObject(DatabaseObject example, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (example.getSnapshotId() != null) {
            return example;
        }
        org.hibernate.mapping.Table hibernateTable = findHibernateTable(example, snapshot);
        if (hibernateTable == null) {
            return example;
        }

        Table table = new Table().setName(hibernateTable.getName());
        LOG.info("Found table " + table.getName());
//        table.setSnapshotId(SnapshotIdService.getInstance().generateId());
        table.setSchema(example.getSchema());


        return table;
    }

    @Override
    protected void addTo(DatabaseObject foundObject, DatabaseSnapshot snapshot) throws DatabaseException, InvalidExampleException {
        if (!snapshot.getSnapshotControl().shouldInclude(Table.class)) {
            return;
        }

        if (foundObject instanceof Schema) {

            Schema schema = (Schema) foundObject;
            HibernateDatabase database = (HibernateDatabase) snapshot.getDatabase();
            Metadata metadata = database.getMetadata();

            Iterator<PersistentClass> tableMappings = metadata.getEntityBindings().iterator();

            while (tableMappings.hasNext()) {
                PersistentClass persistentClass = tableMappings.next();
                if (!persistentClass.isInherited()) {
                    IdentifierGenerator ig = persistentClass.getIdentifier().createIdentifierGenerator(
                            metadata.getIdentifierGeneratorFactory(),
                            database.getDialect(),
                            null,
                            null,
                            (RootClass) persistentClass
                    );
                    for (ExtendedSnapshotGenerator<IdentifierGenerator, Table> tableIdGenerator : tableIdGenerators) {
                        if (tableIdGenerator.supports(ig)) {
                            Table idTable = tableIdGenerator.snapshot(ig);
                            idTable.setSchema(schema);
                            schema.addDatabaseObject(snapshotObject(idTable, snapshot));
                            break;
                        }
                    }
                }
            }

            tableMappings = metadata.getEntityBindings().iterator();

            while (tableMappings.hasNext()) {
                PersistentClass pc = tableMappings.next();

                org.hibernate.mapping.Table hTable = pc.getTable();
                if (hTable.isPhysicalTable()) {
                    Table table = new Table().setName(hTable.getName());
                    table.setSchema(schema);
                    LOG.info("Found table " + table.getName());
                    schema.addDatabaseObject(snapshotObject(table, snapshot));
                }
            }

            Iterator<org.hibernate.mapping.Collection> collIter = metadata.getCollectionBindings().iterator();
            while (collIter.hasNext()) {
                org.hibernate.mapping.Collection coll = collIter.next();
                org.hibernate.mapping.Table hTable = coll.getCollectionTable();
                if (hTable.isPhysicalTable()) {
                    Table table = new Table().setName(hTable.getName());
                    table.setSchema(schema);
                    LOG.info("Found table " + table.getName());
                    schema.addDatabaseObject(snapshotObject(table, snapshot));
                }
            }
        }
    }
}
