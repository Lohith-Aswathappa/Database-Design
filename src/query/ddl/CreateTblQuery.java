package query.ddl;

import common.DatabaseConstants;
import query.model.parser.Column;
import query.base.IQuery;
import query.model.result.Result;
import common.Db_CLog_Helper;
import common.Utils;
import Db_exceptions.Db_Exception;
import io.IOManager;
import io.model.InternalColumn;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class CreateTblQuery implements IQuery {
    public String tableName;
    public ArrayList<Column> columns;
    private boolean hasPrimaryKey;
    public String databaseName;

    public CreateTblQuery(String databaseName, String tableName, ArrayList<Column> columns, boolean hasPrimaryKey){
        this.tableName = tableName;
        this.columns = columns;
        this.hasPrimaryKey = hasPrimaryKey;
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        return new Result(1);
    }

    @Override
    public boolean ValidateQuery() {
        try {
            IOManager IOManager = new IOManager();

            if (!IOManager.databaseExists(this.databaseName)) {
                Utils.printMissingDatabaseError(databaseName);
                return false;
            }

            if (IOManager.checkTableExists(this.databaseName, tableName)) {
                Utils.printDuplicateTableError(this.databaseName, tableName);
                return false;
            }

            if (isduplicateColumnsPresent(columns)) {
                Utils.printMessage("ERROR(102C): Table cannot have duplicate columns.");
                return false;
            }


            List<InternalColumn> columnsList = new ArrayList<>();
            //For default primary key
            if(!this.hasPrimaryKey) {
            	InternalColumn internalColumnP = new InternalColumn("row_id", "INT", true, false);
            	columnsList.add(internalColumnP);
            }
            
            for (int i = 0; i < columns.size(); i++) {
                InternalColumn internalColumn = new InternalColumn();

                Column column = columns.get(i);
                internalColumn.setName(column.name);
                internalColumn.setDataType(column.type.toString());

                if (hasPrimaryKey && i == 0) {
                    internalColumn.setPrimary(true);
                } else {
                    internalColumn.setPrimary(false);
                }

                if (hasPrimaryKey && i == 0) {
                    internalColumn.setNullable(false);
                } else if (column.isNull) {
                    internalColumn.setNullable(true);
                } else {
                    internalColumn.setNullable(false);
                }

                columnsList.add(internalColumn);
            }

            boolean status = IOManager.createTable(this.databaseName, tableName + DatabaseConstants.DEFAULT_FILE_EXTENSION);
            if (status) {
                Db_CLog_Helper databaseHelper = new Db_CLog_Helper();
                int startingRowId = databaseHelper.update_System_Tables_Table(this.databaseName, tableName, columns.size());
                boolean systemTableUpdateStatus = databaseHelper.updateSystemColumnsTable(this.databaseName, tableName, startingRowId, columnsList);

                if (!systemTableUpdateStatus) {
                    Utils.printMessage("ERROR(102T): Failed to create table " + tableName);
                    return false;
                }
            }
        }
        catch (Db_Exception e) {
            Utils.printMessage(e.getMessage());
            return false;
        }

        return true;
    }

    private boolean isduplicateColumnsPresent(ArrayList<Column> columnArrayList) {
        HashMap<String, Integer> map = new HashMap<>();
        for (int i = 0; i < columnArrayList.size(); i++) {
            Column column = columnArrayList.get(i);
            if (map.containsKey(column.name)) {
                return true;
            }
            else {
                map.put(column.name, i);
            }
        }

        return false;
    }
}
