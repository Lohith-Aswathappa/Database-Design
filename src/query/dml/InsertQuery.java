package query.dml;

import common.DatabaseConstants;
import common.Db_Helper;
import datatypes.base.DType;
import common.Utils;
import datatypes.*;
import Db_exceptions.Db_Exception;
import io.IOManager;
import io.model.DataRecord;
import query.base.IQuery;
import query.model.parser.Literal;
import query.model.result.Result;

import java.util.*;

public class InsertQuery implements IQuery {
    public String tableName;
    public ArrayList<String> columns;
    private ArrayList<Literal> values;
    public String databaseName;

    public InsertQuery(String databaseName, String tableName, ArrayList<String> columns, ArrayList<Literal> values) {
        this.tableName = tableName;
        this.columns = columns;
        this.values = values;
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        try {
            IOManager manager = new IOManager();
            List<String> retrievedColumns = Db_Helper.getDbHelper().fetchAllTableColumns(this.databaseName, tableName);
            HashMap<String, Integer> columnDataTypeMapping = Db_Helper.getDbHelper().fetchAllTableColumnDataTypes(this.databaseName, tableName);

            DataRecord record = new DataRecord();
            generateRecords(record.getColumnValueList(), columnDataTypeMapping, retrievedColumns);

            int rowID = findRowID(retrievedColumns);
            record.setRowId(rowID);
            record.populateSize();

            Result result = null;
            boolean status = manager.writeRecord(this.databaseName, tableName, record);
            if (status) {
                result = new Result(1);
            } else {
                Utils.printMessage("ERROR(110F): Unable to insert record.");
            }

            return result;
        }
        catch (Db_Exception e) {
            Utils.printMessage(e.getMessage());
        }
        return null;
    }

    @Override
    public boolean ValidateQuery() {
        try {
            IOManager manager = new IOManager();
            if (!manager.checkTableExists(this.databaseName, tableName)) {
                Utils.printMissingTableError(this.databaseName, tableName);
                return false;
            }

            List<String> retrievedColumns = Db_Helper.getDbHelper().fetchAllTableColumns(this.databaseName, tableName);
            HashMap<String, Integer> columnDataTypeMapping = Db_Helper.getDbHelper().fetchAllTableColumnDataTypes(this.databaseName, tableName);

            if (columns == null) {
                if (values.size() > retrievedColumns.size()) {
                    Utils.printMessage("ERROR(110C): Column count doesn't match value count at row 1");
                    return false;
                }

                Utils utils = new Utils();
                if (!utils.checkDataTypeValidity(columnDataTypeMapping, retrievedColumns, values)) {
                    return false;
                }
            } else {
                if (columns.size() > retrievedColumns.size()) {
                    Utils.printMessage("ERROR(110C): Column count doesn't match value count at row 1");
                    return false;
                }

                boolean areColumnsValid = checkColumnValidity(retrievedColumns);
                if (!areColumnsValid) {
                    return false;
                }

                boolean areColumnsDataTypeValid = validateColumnDataTypes(columnDataTypeMapping);
                if (!areColumnsDataTypeValid) {
                    return false;
                }
            }

            boolean isNullConstraintValid = checkNullConstraint(retrievedColumns);
            if (!isNullConstraintValid) {
                return false;
            }

            boolean isPrimaryKeyConstraintValid = checkPrimaryKeyConstraint(retrievedColumns);
            if (!isPrimaryKeyConstraintValid) {
                return false;
            }
        }
        catch (Db_Exception e) {
            Utils.printMessage(e.getMessage());
            return false;
        }

        return true;
    }

    private boolean validateColumnDataTypes(HashMap<String, Integer> columnDataTypeMapping) {
        return checkColumnDataTypeValidity(columnDataTypeMapping);
    }

    private boolean checkColumnValidity(List<String> retrievedColumns) {
        boolean columnsValid = true;
        String invalidColumn = "";

        for (String tableColumn : columns) {
            if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
                columnsValid = false;
                invalidColumn = tableColumn;
                break;
            }
        }

        if (!columnsValid) {
            Utils.printMessage("ERROR(110C): Invalid column '" + invalidColumn + "'");
            return false;
        }

        return true;
    }

    private boolean checkNullConstraint(List<String> retrievedColumnNames) throws Db_Exception {
        HashMap<String, Integer> columnsList = new HashMap<>();

        if (columns != null) {
            for (int i = 0; i < columns.size(); i++) {
                columnsList.put(columns.get(i), i);
            }
        }
        else {
            for (int i = 0; i < values.size(); i++) {
                columnsList.put(retrievedColumnNames.get(i), i);
            }
        }

        return Db_Helper.getDbHelper().checkNullConstraint(this.databaseName, tableName, columnsList);
    }

    private boolean checkPrimaryKeyConstraint(List<String> retrievedColumnNames) throws Db_Exception {

        String primaryKeyColumnName = Db_Helper.getDbHelper().getTablePrimaryKey(databaseName, tableName);
        List<String> columnList = (columns != null) ? columns : retrievedColumnNames;

        if (primaryKeyColumnName.length() > 0) {
                if (columnList.contains(primaryKeyColumnName.toLowerCase())) {
                    int primaryKeyIndex = columnList.indexOf(primaryKeyColumnName);
                    if (Db_Helper.getDbHelper().checkIfValueForPrimaryKeyExists(this.databaseName, tableName, Integer.parseInt(values.get(primaryKeyIndex).value))) {
                        Utils.printMessage("ERROR(110P): Duplicate entry '" + values.get(primaryKeyIndex).value + "' for key 'PRIMARY'");
                        return false;
                    }
                }
        }

        return true;
    }

    private boolean checkColumnDataTypeValidity(HashMap<String, Integer> columnDataTypeMapping) {
        String invalidColumn = "";

        for (String columnName : columns) {
            int dataTypeIndex = columnDataTypeMapping.get(columnName);
            int idx = columns.indexOf(columnName);
            Literal literal = values.get(idx);

            if (literal.type != Utils.internalDataTypeToModelDataType((byte)dataTypeIndex)) {
                if (Utils.canUpdateLiteralDataType(literal, dataTypeIndex)) {
                    continue;
                }

                invalidColumn = columnName;
                break;
            }
        }

        boolean valid = invalidColumn.length() <= 0;

        if (!valid) {
            Utils.printMessage("ERROR(110CV): Invalid value for column '" + invalidColumn  + "'");
            return false;
        }

        return true;
    }

    private void generateRecords(List<Object> columnList, HashMap<String, Integer> columnDataTypeMapping, List<String> retrievedColumns) {
        for (int i=0; i < retrievedColumns.size(); i++) {
            String column = retrievedColumns.get(i);

            if (columns != null) {
                if (columns.contains(column) || column.equals("row_id")) {
                	
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();

                    int idx = columns.indexOf(column);

                    DType obj = getDataTypeObject(dataType);
       
                    String val = "";
                    if(column.equals("row_id") && columns.indexOf(column) == -1) {
                    	Db_Helper helper = Db_Helper.getDbHelper();
                    	columns.add(0, "row_id");
                    	try {
							val = Integer.toString(helper.getTableRecordCount(this.databaseName, tableName) + 1);
							values.add(0, Literal.CreateLiteral(val));
                    	} catch (Db_Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
                    }
                    else {
                    	val = values.get(idx).toString();
                    }
                    obj.setValue(getDataTypeValue(dataType, val));
                    columnList.add(obj);
                } else {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();
                    DType obj = getDataTypeObject(dataType);
                    obj.setNull(true);
                    columnList.add(obj);
                }
            }
            else {

                if (i < values.size()) {
                    Byte dataType = (byte) columnDataTypeMapping.get(column).intValue();

                    int columnIndex = retrievedColumns.indexOf(column);
                    DType obj = getDataTypeObject(dataType);
                    String val = values.get(columnIndex).toString();

                    obj.setValue(getDataTypeValue(dataType, val));
                    columnList.add(obj);
                }
                else {
                    Byte dataType = (byte)columnDataTypeMapping.get(column).intValue();
                    DType obj = getDataTypeObject(dataType);
                    obj.setNull(true);
                    columnList.add(obj);
                }
            }
        }
    }

    private DType getDataTypeObject(byte dataType) {

        switch (dataType) {
            case DatabaseConstants.TINYINT: {
                return new DataType_TinyInt();
            }
            case DatabaseConstants.SMALLINT: {
                return new DataType_SmallInt();
            }
            case DatabaseConstants.INT: {
                return new DataType_Int();
            }
            case DatabaseConstants.BIGINT: {
                return new DataType_BigInt();
            }
            case DatabaseConstants.REAL: {
                return new DataType_Real();
            }
            case DatabaseConstants.DOUBLE: {
                return new DataType_Double();
            }
            case DatabaseConstants.DATE: {
                return new DataType_Date();

            }
            case DatabaseConstants.DATETIME: {
                return new DataType_DateTime();
            }
            case DatabaseConstants.TEXT: {
                return new DataType_Text();
            }
            default: {
                return new DataType_Text();
            }
        }
    }

    private Object getDataTypeValue(byte dataType, String value) {

        switch (dataType) {
            case DatabaseConstants.TINYINT: {
                return Byte.parseByte(value);
            }
            case DatabaseConstants.SMALLINT: {
                return Short.parseShort(value);
            }
            case DatabaseConstants.INT: {
                return Integer.parseInt(value);
            }
            case DatabaseConstants.BIGINT: {
                return Long.parseLong(value);
            }
            case DatabaseConstants.REAL: {
                return Float.parseFloat(value);
            }
            case DatabaseConstants.DOUBLE: {
                return Double.parseDouble(value);
            }
            case DatabaseConstants.DATE: {
                return Utils.getDateEpoc(value, true);
            }
            case DatabaseConstants.DATETIME: {
                return Utils.getDateEpoc(value, false);
            }
            case DatabaseConstants.TEXT: {
                return value;
            }
            default: {
                return value;
            }
        }
    }

    private int findRowID (List<String> retrievedList) throws Db_Exception {
        Db_Helper helper = Db_Helper.getDbHelper();
        int rowCount = helper.getTableRecordCount(this.databaseName, tableName);
        String primaryKeyColumnName = helper.getTablePrimaryKey(databaseName, tableName);
        if (primaryKeyColumnName.length() > 0) {
            int primaryKeyIndex = (columns != null) ? columns.indexOf(primaryKeyColumnName) : retrievedList.indexOf(primaryKeyColumnName);
            return Integer.parseInt(values.get(primaryKeyIndex).value);
        }
        else {
            return rowCount + 1;
        }
    }
}