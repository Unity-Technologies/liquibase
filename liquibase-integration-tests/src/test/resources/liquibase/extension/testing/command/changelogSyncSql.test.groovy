package liquibase.extension.testing.command

import liquibase.change.ColumnConfig
import liquibase.change.core.CreateTableChange
import liquibase.change.core.TagDatabaseChange
import liquibase.exception.CommandValidationException

CommandTests.define {
    command = ["changelogSyncSql"]
    signature = """
Short Description: Output the raw SQL used by Liquibase when running changelogSync
Long Description: NOT SET
Required Args:
  changelogFile (String) The root changelog file
  url (String) The JDBC database connection URL
Optional Args:
  contexts (String) Context string to use for filtering which changes to mark as executed
    Default: null
  labels (String) Label expression to use for filtering which changes to mark as executed
    Default: null
  password (String) The database password
    Default: null
  username (String) The database username
    Default: null
"""

    run "Happy path", {
        arguments = [
                changelogFile: "changelogs/hsqldb/complete/rollback.tag.changelog.xml"
        ]

        setup {
            database = [
                    new CreateTableChange(
                            tableName: "FirstTable",
                            columns: [
                                    ColumnConfig.fromName("FirstColumn")
                                            .setType("VARCHAR(255)")
                            ]
                    ),
                    new CreateTableChange(
                            tableName: "SecondTable",
                            columns: [
                                    ColumnConfig.fromName("SecondColumn")
                                            .setType("VARCHAR(255)")
                            ]
                    ),
                    new TagDatabaseChange(
                            tag: "version_2.0"
                    ),
                    new CreateTableChange(
                            tableName: "liquibaseRunInfo",
                            columns: [
                                    ColumnConfig.fromName("timesRan")
                                            .setType("INT")
                            ]
                    ),
            ]
        }

        expectedResults = [
                statusCode   : 0
        ]
    }

    run "Happy path with output SQL", {
        arguments = [
                changelogFile: "changelogs/hsqldb/complete/rollback.tag.changelog.xml"
        ]

        setup {
            cleanResources("target/test-classes/changelogSync.sql")
            database = [
                    new CreateTableChange(
                            tableName: "FirstTable",
                            columns: [
                                    ColumnConfig.fromName("FirstColumn")
                                            .setType("VARCHAR(255)")
                            ]
                    ),
                    new CreateTableChange(
                            tableName: "SecondTable",
                            columns: [
                                    ColumnConfig.fromName("SecondColumn")
                                            .setType("VARCHAR(255)")
                            ]
                    ),
                    new TagDatabaseChange(
                            tag: "version_2.0"
                    ),
                    new CreateTableChange(
                            tableName: "liquibaseRunInfo",
                            columns: [
                                    ColumnConfig.fromName("timesRan")
                                            .setType("INT")
                            ]
                    ),
            ]
        }

        outputFile = new File("target/test-classes/changelogSync.sql")

        expectedFileContent = [
                //
                // Find the " -- Release Database Lock" line
                //
                "target/test-classes/changelogSync.sql" : [CommandTests.assertContains("-- Release Database Lock")]
        ]

        expectedResults = [
                statusCode   : 0
        ]
    }

    run "Run without URL should throw an exception",  {
        arguments = [
                url: "",
                changelogFile: "changelogs/hsqldb/complete/rollback.tag.changelog.xml"
        ]

        expectedException = CommandValidationException.class
    }

    run "Run without changeLogFile should throw an exception",  {
        arguments = [
                changelogFile: ""
        ]

        expectedException = CommandValidationException.class
    }

    run "Run without any arguments should throw an exception",  {
        arguments = [
                url: ""
        ]
        expectedException = CommandValidationException.class
    }
}
