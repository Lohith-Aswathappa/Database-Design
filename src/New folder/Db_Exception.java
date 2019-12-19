package Db_exceptions;

public class Db_Exception extends Exception {

    public static String BASE_ERROR_STRING = "ERROR(200): ";
    public static String INVALID_DATATYPE_EXCEPTION = BASE_ERROR_STRING + "Invalid datatype given.";
    public static String DATATYPE_MISMATCH_EXCEPTION = BASE_ERROR_STRING + "Invalid datatype given in WHERE clause. Expected %1.";
    public static String INVALID_CONDITION_EXCEPTION = BASE_ERROR_STRING + "Invalid condition given. Currently only %1 supported.";
    public static String GENERIC_EXCEPTION = BASE_ERROR_STRING + "Error!!!!  was encountered while executing the given request.";

    public Db_Exception(String message, String parameter) {
        super(message.replace("%1", parameter));
    }

    public Db_Exception(String message) {
        super(message);
    }

}
