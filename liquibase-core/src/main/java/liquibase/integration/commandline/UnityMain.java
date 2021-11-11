package liquibase.integration.commandline;

import liquibase.exception.CommandLineParsingException;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class UnityMain {
    public static void main(String[] args) throws ConfigurationException, IOException, CommandLineParsingException, InterruptedException {
        if (args.length > 4 || args.length < 2) {
            printUsage();
            return;
        }
        boolean debug = false;
        String contexts = "PCI,NONPCI";
        for (int i = 2; i < args.length; i++) {
            if (args[i].equalsIgnoreCase("--debug")) {
                debug = true;
            } else if (args[i].startsWith("--contexts:")) {
                contexts = args[i].substring("--contexts:".length());
                System.out.println("Contexts specified: " + contexts);
            }
        }
        String env = args[0];
        File confDir = new File("conf/" + env);
        if (!confDir.exists() || !confDir.isDirectory())
            throw new RuntimeException("env " + env + " conf not found");
        System.setProperty("env", env);
        ExecutorService threadPool = Executors.newFixedThreadPool(10);
        Main.runByJunbo = true;
        boolean notConfigured = true;
        VaultServiceImpl vaultService = new VaultServiceImpl();
        Properties properties = vaultService.readVaultProperties();
        for (File confFile : confDir.listFiles()) {
            System.out.println("filePath:\t" + confFile.getAbsolutePath());
            String password;
            PropertiesConfiguration propertiesConfiguration = new PropertiesConfiguration(confFile);
            String changeLogFileName = confFile.getName().split("\\.")[0] + ".xml";
            String jdbcDriver = propertiesConfiguration.getString("jdbc_driver");
            String username = propertiesConfiguration.getString("login_username");
            if (propertiesConfiguration.containsKey("login_password")) {
                password = propertiesConfiguration.getString("login_password");
            } else if (propertiesConfiguration.containsKey("login_password.vaultPath")) {
                password = properties.getProperty(propertiesConfiguration.getString("login_password.vaultPath"));
            } else {
                throw new RuntimeException("login password not found in " + confFile.getName());
            }
            Iterator<String> iterator = propertiesConfiguration.getKeys();
            while (iterator.hasNext()) {
                String jdbcUrlKey = iterator.next();
                if (!jdbcUrlKey.startsWith("jdbc_url"))
                    continue;
                String[] jdbcUrlAndSchema = propertiesConfiguration.getString(jdbcUrlKey).split(";");
                final List<String> liquibaseArgs = new ArrayList<String>(8);
                liquibaseArgs.add("--driver=" + jdbcDriver);
                liquibaseArgs.add("--changeLogFile=changelogs/silkcloud/" + changeLogFileName);
                liquibaseArgs.add("--username=" + username);
                liquibaseArgs.add("--password=" + password);
                liquibaseArgs.add("--url=" + jdbcUrlAndSchema[0]);
                if (contexts != null)
                    liquibaseArgs.add("--contexts=" + contexts);
                liquibaseArgs.add("--defaultSchemaName=" + jdbcUrlAndSchema[1]);
                if (debug)
                    liquibaseArgs.add("--logLevel=debug");
                liquibaseArgs.add("update");
                if (notConfigured) {
                    Main.main(liquibaseArgs.<String>toArray(new String[0]));
                    notConfigured = false;
                    Main.isConfigured = true;
                    continue;
                }
                threadPool.execute(new Runnable() {
                    public void run() {
                        try {
                            Main.main((String[])liquibaseArgs.toArray((Object[])new String[0]));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }
        }
        threadPool.shutdown();
        threadPool.awaitTermination(1L, TimeUnit.HOURS);
        System.exit(0);
    }

    private static void printUsage() {
        System.out.println("<env> <key> [--debug] [-contexts:contexts]");
    }
}
