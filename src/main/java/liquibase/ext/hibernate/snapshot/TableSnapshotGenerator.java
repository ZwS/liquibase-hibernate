package liquibase.ext.hibernate.snapshot;

import liquibase.datatype.LiquibaseDataType;
import liquibase.exception.DatabaseException;
import liquibase.ext.hibernate.database.HibernateDatabase;
import liquibase.ext.hibernate.snapshot.extension.ExtendedSnapshotGenerator;
import liquibase.ext.hibernate.snapshot.extension.MultipleHiLoPerTableSnapshotGenerator;
import liquibase.ext.hibernate.snapshot.extension.TableGeneratorSnapshotGenerator;
import liquibase.snapshot.DatabaseSnapshot;
import liquibase.snapshot.InvalidExampleException;
import liquibase.structure.DatabaseObject;
import liquibase.structure.core.*;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.RootClass;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.hibernate.boot.Metadata;

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

            Iterator<org.hibernate.mapping.Table> tableMappings = metadata.collectTableMappings().iterator();
            while (tableMappings.hasNext()) {
                org.hibernate.mapping.Table hibernateTable = (org.hibernate.mapping.Table) tableMappings.next();
                if (hibernateTable.isPhysicalTable()) {
                    Table table = new Table().setName(hibernateTable.getName());
                    table.setSchema(schema);
                    LOG.info("Found table " + table.getName());
                    schema.addDatabaseObject(snapshotObject(table, snapshot));
                }
            }

            Iterator<PersistentClass> classMappings = metadata.getEntityBindings().iterator();
            while (classMappings.hasNext()) {
                PersistentClass persistentClass = (PersistentClass) classMappings
                        .next();
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
        }
    }

	/**
	 * has <code>dataType</code> auto increment property ?
	 */
    //FIXME remove if will be accepted  https://github.com/liquibase/liquibase/pull/247
	private boolean isAutoIncrement(LiquibaseDataType dataType) {
		boolean retVal = false;
		String methodName = "isAutoIncrement";
		Method[] methods = dataType.getClass().getMethods();
		for (Method method : methods) {
			if (method.getName().equals(methodName)
					&& method.getParameterTypes().length == 0) {
				retVal = true;
				break;
			}
		}

		return retVal;
	}

}
