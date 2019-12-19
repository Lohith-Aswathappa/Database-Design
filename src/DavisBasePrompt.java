import common.Db_CLog_Helper;
import common.DatabaseConstants;
import query.QueryHandler;

import java.util.Scanner;

import static query.QueryParser.isExit;
import static query.QueryParser.parseCommand;


public class DavisBasePrompt {

    private static Scanner scanner = new Scanner(System.in).useDelimiter(";");

    public static void main(String[] args) {

        Db_CLog_Helper.InitializeDatabase();
        splashScreen();

        while(!isExit) {
            System.out.print(DatabaseConstants.PROMPT);
            String command = scanner.next().replace("\n", "").replace("\r", " ").trim().toLowerCase();
            parseCommand(command);
        }
    }

    private static void splashScreen() {
        System.out.println(QueryHandler.line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
        QueryHandler.ShowVersionQueryHandler();
        System.out.println("\nType 'help;' to display supported commands.");
        System.out.println(QueryHandler.line("-",80));
    }
}