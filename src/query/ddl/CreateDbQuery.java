package query.ddl;

import common.Db_Helper;
import common.Utils;
import query.base.IQuery;
import query.model.result.Result;

import java.io.File;


public class CreateDbQuery implements IQuery {
    public String databaseName;

    public CreateDbQuery(String databaseName){
        this.databaseName = databaseName;
    }

    @Override
    public Result ExecuteQuery() {
        File database = new File(Utils.getDatabasePath(this.databaseName));
        boolean isCreated = database.mkdir();

        if(!isCreated){
            System.out.println(String.format("ERROR(200): Unable to create database '%s'", this.databaseName));
            return null;
        }

        Result result = new Result(1);
        return result;
    }

    @Override
    public boolean ValidateQuery() {
        boolean databaseExists = Db_Helper.getDbHelper().databaseExists(this.databaseName);

        if(databaseExists){
            System.out.println(String.format("ERROR(104D): Database '%s' already exists", this.databaseName));
            return false;
        }

        return true;
    }
}
