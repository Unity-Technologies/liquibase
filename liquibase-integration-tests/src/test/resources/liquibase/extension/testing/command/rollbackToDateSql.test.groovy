package liquibase.extension.testing.command

import liquibase.exception.CommandValidationException

CommandTests.define {
    command = ["rollbackToDateSql"]
    signature = """
Short Description: Generate the SQL to rollback changes made to the database based on the specific date
Long Description: NOT SET
Required Args:
  changelogFile (String) The root changelog
  date (LocalDateTime) Date to rollback changes to
  url (String) The JDBC database connection URL
Optional Args:
  contexts (String) Changeset contexts to match
    Default: null
  labels (String) Changeset labels to match
    Default: null
  password (String) Password to use to connect to the database
    Default: null
  rollbackScript (String) Rollback script to execute
    Default: null
  username (String) Username to use to connect to the database
    Default: null
"""

    run "Happy path", {
        arguments = [
                date         : "2021-03-25T09:00:00",
                changelogFile: "changelogs/hsqldb/complete/rollback.changelog.xml",
        ]

        setup {
            runChangelog "changelogs/hsqldb/complete/rollback.changelog.xml"
        }

        expectedResults = [
                statusCode   : 0
        ]
    }

    run "Happy path with an output file", {
        arguments = [
                date         : "2021-03-25T09:00:00",
                changelogFile: "changelogs/hsqldb/complete/rollback.changelog.xml",
        ]

        setup {
            cleanResources("target/test-classes/rollbackToDate.sql")
            runChangelog "changelogs/hsqldb/complete/rollback.changelog.xml"
        }

        outputFile = new File("target/test-classes/rollbackToDate.sql")

        expectedFileContent = [
                //
                // Find the " -- Release Database Lock" line
                //
                "target/test-classes/rollbackToDate.sql" : [CommandTests.assertContains("-- Release Database Lock")]
        ]

        expectedResults = [
                statusCode   : 0
        ]
    }

    run "Run without any arguments should throw an exception",  {
        arguments = [
                url:  ""
        ]

        expectedException = CommandValidationException.class
    }

    run "Run without a date should throw an exception",  {
        arguments = [
                changelogFile: "changelogs/hsqldb/complete/rollback.tag.changelog.xml",
        ]
        expectedException = CommandValidationException.class
    }

    run "Run without a changeLogFile should throw an exception",  {
        arguments = [
                date         : "2021-03-25T09:00:00",
        ]
        expectedException = CommandValidationException.class
    }

    run "Run without a URL should throw an exception",  {
        arguments = [
                url          : "",
                changelogFile: "changelogs/hsqldb/complete/rollback.tag.changelog.xml",
                date         : "2021-03-25T09:00:00"
        ]
        expectedException = CommandValidationException.class
    }
}