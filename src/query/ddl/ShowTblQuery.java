package query.ddl;

import common.DatabaseConstants;
import common.Db_Helper;
import common.Utils;
import query.base.IQuery;
import query.model.parser.Condition;
import query.model.result.Result;
import query.vdl.SelectQuery;

import java.util.ArrayList;

public class ShowTblQuery implements IQuery {

    public String databaseName;

    public ShowTblQuery(String databaseName) {
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        ArrayList<String> columns = new ArrayList<>();
        columns.add("table_name");

        Condition condition = Condition.CreateCondition(String.format("database_name = '%s'", this.databaseName));
        ArrayList<Condition> conditionList = new ArrayList<>();
        conditionList.add(condition);

        IQuery query = new SelectQuery(DatabaseConstants.DEFAULT_CATALOG_DATABASENAME, DatabaseConstants.SYSTEM_TABLES_TABLENAME, columns, conditionList, false);
        if (query.ValidateQuery()) {
            return query.ExecuteQuery();
        }

        return null;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = Db_Helper.getDbHelper().databaseExists(this.databaseName);
        if(!databaseExists){
            Utils.printMissingDatabaseError(this.databaseName);
        }
        return databaseExists;
    }
}
