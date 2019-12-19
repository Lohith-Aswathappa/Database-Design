package query.dml;

import common.Db_Helper;
import common.Utils;
import datatypes.base.DType;
import Db_exceptions.Db_Exception;
import io.IOManager;
import io.model.InternalCondition;
import query.model.parser.Condition;
import query.model.result.Result;
import query.base.IQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class DeleteQuery implements IQuery {
    public String databaseName;
    public String tableName;
    public ArrayList<Condition> conditions;
    public boolean isInternal = false;

    public DeleteQuery(String databaseName, String tableName, ArrayList<Condition> conditions){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.conditions = conditions;
    }

    public DeleteQuery(String databaseName, String tableName, ArrayList<Condition> conditions, boolean isInternal){
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.conditions = conditions;
        this.isInternal = isInternal;
    }

    @Override
    public Result ExecuteQuery() {

        try {
            int rowCount;
            IOManager manager = new IOManager();

            if (conditions == null) {
                rowCount = manager.deleteRecord(databaseName, tableName, (new ArrayList<>()));
            } else {
                List<InternalCondition> conditionList = new ArrayList<>();
                InternalCondition internalCondition;

                for (Condition condition : this.conditions) {
                    internalCondition = new InternalCondition();
                    List<String> retrievedColumns = Db_Helper.getDbHelper().fetchAllTableColumns(this.databaseName, tableName);
                    int idx = retrievedColumns.indexOf(condition.column);
                    internalCondition.setIndex((byte) idx);

                    DType dataType = DType.CreateDT(condition.value);
                    internalCondition.setValue(dataType);

                    internalCondition.setConditionType(Utils.ConvertFromOperator(condition.operator));
                    conditionList.add(internalCondition);
                }

                rowCount = manager.deleteRecord(databaseName, tableName, conditionList);

            }

            return new Result(rowCount, this.isInternal);
        } catch (Db_Exception e) {
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

            if (this.conditions != null) {
                List<String> retrievedColumns = Db_Helper.getDbHelper().fetchAllTableColumns(this.databaseName, tableName);
                HashMap<String, Integer> columnDataTypeMapping = Db_Helper.getDbHelper().fetchAllTableColumnDataTypes(this.databaseName, tableName);

                for (Condition condition : this.conditions) {
                    if (!checkConditionColumnValidity(retrievedColumns)) {
                        return false;
                    }

                    if (!Utils.checkConditionValueDataTypeValidity(columnDataTypeMapping, retrievedColumns, condition)) {
                        return false;
                    }
                }
            }
        } catch (Db_Exception e) {
            Utils.printMessage(e.getMessage());
            return false;
        }
        return true;
    }


    private boolean checkConditionColumnValidity(List<String> retrievedColumns) {
        boolean columnsValid = true;
        String invalidColumn = "";

        for (Condition condition : this.conditions) {
            String tableColumn = condition.column;
            if (!retrievedColumns.contains(tableColumn.toLowerCase())) {
                columnsValid = false;
                invalidColumn = tableColumn;
            }

            if (!columnsValid) {
                Utils.printMessage("ERROR(106C): Column " + invalidColumn + " is not present in the table " + tableName + ".");
                return false;
            }
        }

        return true;
    }
}
