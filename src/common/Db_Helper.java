package common;

import datatypes.DataType_Int;
import datatypes.DataType_Text;
import datatypes.base.DType;
import Db_exceptions.Db_Exception;
import io.IOManager;
import io.model.DataRecord;
import io.model.InternalCondition;
import query.QueryHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static query.QueryHandler.USE_HELP_MESSAGE;


public class Db_Helper {

    private static Db_Helper dbHelper = null;

    public static Db_Helper getDbHelper() {
        if(dbHelper == null) {
            return new Db_Helper();
        }
        return dbHelper;
    }

    private IOManager manager;

    private Db_Helper() {
        manager = new IOManager();
    }

    public boolean databaseExists(String databaseName) {

        if (databaseName == null || databaseName.length() == 0) {
            QueryHandler.UnrecognisedCommand("", USE_HELP_MESSAGE);
            return false;
        }

        return new IOManager().databaseExists(databaseName);
    }

    public boolean tableExists(String databaseName, String tableName) {
        if (tableName == null || databaseName == null || tableName.length() == 0 || databaseName.length() == 0) {
            QueryHandler.UnrecognisedCommand("", USE_HELP_MESSAGE);
            return false;
        }

        return new IOManager().checkTableExists(databaseName, tableName);
    }

    public List<String> fetchAllTableColumns(String databaseName, String tableName) throws Db_Exception {
        List<String> columnNames = new ArrayList<>();
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new DataType_Text(databaseName)));
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataType_Text(tableName)));

        List<DataRecord> records = manager.findRecord(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseConstants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (DataRecord record : records) {
            Object object = record.getColumnValueList().get(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnNames.add(((DType) object).getStringValue());
        }

        return columnNames;
    }

    public boolean checkNullConstraint(String databaseName, String tableName, HashMap<String, Integer> columnMap) throws Db_Exception {

        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new DataType_Text(databaseName)));
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataType_Text(tableName)));

        List<DataRecord> records = manager.findRecord(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseConstants.SYSTEM_COLUMNS_TABLENAME, conditions, false);

        for (DataRecord record : records) {
            Object nullValueObject = record.getColumnValueList().get(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_IS_NULLABLE);
            Object object = record.getColumnValueList().get(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);

            String isNullStr = ((DType) nullValueObject).getStringValue().toUpperCase();
            boolean isNullable = isNullStr.equals("YES");
            
            //check for 
            
            if (!columnMap.containsKey(((DType) object).getStringValue()) && !isNullable) {
            	if(((DType) object).getStringValue().equals("row_id")) {
            		return true;
            	}
            	else {
            		Utils.printMessage("ERROR(100N): Field '" + ((DType) object).getStringValue() + "' cannot be NULL");
            	}
                return false;
            }

        }

        return true;
    }

    public HashMap<String, Integer> fetchAllTableColumnDataTypes(String databaseName, String tableName) throws Db_Exception {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new DataType_Text(databaseName)));
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataType_Text(tableName)));

        List<DataRecord> records = manager.findRecord(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseConstants.SYSTEM_COLUMNS_TABLENAME, conditions, false);
        HashMap<String, Integer> columDataTypeMapping = new HashMap<>();

        for (DataRecord record : records) {
            Object object = record.getColumnValueList().get(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            Object dataTypeObject = record.getColumnValueList().get(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_DATA_TYPE);

            String columnName = ((DType) object).getStringValue();
            int columnDataType = Utils.stringToDataType(((DType) dataTypeObject).getStringValue());
            columDataTypeMapping.put(columnName.toLowerCase(), columnDataType);
        }

        return columDataTypeMapping;
    }

    public String getTablePrimaryKey(String databaseName, String tableName) throws Db_Exception {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new DataType_Text(databaseName)));
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataType_Text(tableName)));
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_COLUMN_KEY, InternalCondition.EQUALS, new DataType_Text(Db_CLog_Helper.PRIMARY_KEY_IDENTIFIER)));

        List<DataRecord> records = manager.findRecord(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseConstants.SYSTEM_COLUMNS_TABLENAME, conditions, true);
        String columnName = "";
        if(records.size() > 0) {
            DataRecord record = records.get(0);
            Object object = record.getColumnValueList().get(Db_CLog_Helper.COLUMNS_TABLE_SCHEMA_COLUMN_NAME);
            columnName = ((DType) object).getStringValue();
        }

        return columnName;
    }

    public int getTableRecordCount(String databaseName, String tableName) throws Db_Exception {
        List<InternalCondition> conditions = new ArrayList<>();
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.TABLES_TABLE_SCHEMA_DATABASE_NAME, InternalCondition.EQUALS, new DataType_Text(databaseName)));
        conditions.add(InternalCondition.CreateCondition(Db_CLog_Helper.TABLES_TABLE_SCHEMA_TABLE_NAME, InternalCondition.EQUALS, new DataType_Text(tableName)));

        List<DataRecord> records = manager.findRecord(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseConstants.SYSTEM_TABLES_TABLENAME, conditions, true);
        int recordCount = 0;

        if(records.size() > 0) {
            DataRecord record = records.get(0);
            Object object = record.getColumnValueList().get(Db_CLog_Helper.TABLES_TABLE_SCHEMA_RECORD_COUNT);
            recordCount = Integer.valueOf(((DType) object).getStringValue());
        }

        return recordCount;
    }

    public boolean checkIfValueForPrimaryKeyExists(String databaseName, String tableName, int value) throws Db_Exception {
        IOManager manager = new IOManager();
        InternalCondition condition = InternalCondition.CreateCondition(0, InternalCondition.EQUALS, new DataType_Int(value));

        List<DataRecord> records = manager.findRecord(databaseName, tableName, condition, false);
        return records.size() > 0;
    }
}
