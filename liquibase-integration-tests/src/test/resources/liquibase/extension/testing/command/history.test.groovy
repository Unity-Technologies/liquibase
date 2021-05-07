package liquibase.extension.testing.command

import liquibase.exception.CommandValidationException
import liquibase.extension.testing.setup.HistoryEntry

import java.util.regex.Pattern

CommandTests.define {
    command = ["history"]
    signature = """
Short Description: List all deployed changesets and their deployment ID
Long Description: NOT SET
Required Args:
  url (String) The JDBC database connection URL
Optional Args:
  changelogFile (String) The root changelog
    Default: null
  password (String) Password to use to connect to the database
    Default: null
  username (String) Username to use to connect to the database
    Default: null
"""
    run "Happy path", {
        setup {
            history = [
                    new HistoryEntry(
                            id: "1",
                            author: "nvoxland",
                            path: "db/changelog/db.changelog-master.xml",
                    ),
                    new HistoryEntry(
                            id: "raw",
                            author: "includeAll",
                            path: "db/changelog/sql/create_test2.sql",
                    ),
                    new HistoryEntry(
                            id: "raw",
                            author: "includeAll",
                            path: "db/changelog/sql/create_test3.sql",
                    ),
                    new HistoryEntry(
                            id: "1571079854679-2",
                            author: "nathan (generated)",
                            path: "db/changelog/changelog-x.xml",
                    ),
            ]
        }

        expectedOutput = [
                Pattern.compile("""
- Database updated at \\d+/\\d+.+. Applied 4 changeset\\(s\\) in \\d+.\\d+s, DeploymentId: \\d+
  db/changelog/db.changelog-master.xml::1::nvoxland
  db/changelog/sql/create_test2.sql::raw::includeAll
  db/changelog/sql/create_test3.sql::raw::includeAll
  db/changelog/changelog-x.xml::1571079854679-2::nathan \\(generated\\)
""".replace("\r", "").trim())
        ]

        expectedResults = [
                deployments: "1 past deployments",
                statusCode : 0
        ]
    }

    run "Happy path with an output file", {
        setup {
            cleanResources("target/test-classes/history.sql")
            history = [
                    new HistoryEntry(
                            id: "1",
                            author: "nvoxland",
                            path: "db/changelog/db.changelog-master.xml",
                    ),
                    new HistoryEntry(
                            id: "raw",
                            author: "includeAll",
                            path: "db/changelog/sql/create_test2.sql",
                    ),
                    new HistoryEntry(
                            id: "raw",
                            author: "includeAll",
                            path: "db/changelog/sql/create_test3.sql",
                    ),
                    new HistoryEntry(
                            id: "1571079854679-2",
                            author: "nathan (generated)",
                            path: "db/changelog/changelog-x.xml",
                    ),
            ]
        }

        outputFile = new File("target/test-classes/history.sql")

        expectedFileContent = [
                //
                // Find the " -- Release Database Lock" line
                //
                "target/test-classes/history.sql" : [CommandTests.assertContains("Database updated at")]
        ]

        expectedResults = [
                deployments: "1 past deployments",
                statusCode : 0
        ]
    }

    run "Run without any arguments should throw an exception",  {
        arguments = [
                url: ""
        ]
        expectedException = CommandValidationException.class
    }
}