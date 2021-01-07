# Command-line interface

```console
$ mongoengine_migrate --help
Usage: mongoengine_migrate [OPTIONS] COMMAND [ARGS]...

Options:
  -u, --uri URI                  MongoDB connect URI  [default:
                                 mongodb://localhost/mydb]

  -d, --directory DIR            Directory with migrations  [default:
                                 ./migrations]

  -c, --collection COLLECTION    Collection where schema and state will be
                                 stored  [default: mongoengine_migrate]

  --mongo-version MONGO_VERSION  Manually set MongoDB server version. By
                                 default it's determined automatically, but
                                 this requires a permission for 'buildinfo'
                                 admin command

  --log-level LOG_LEVEL          Logging verbosity level  [default: INFO]
  --help                         Show this message and exit.

Commands:
  downgrade       Downgrade db to the given migration
  makemigrations  Generate migration file based on mongoengine model changes
  migrate         Migrate db to the given migration. By default is to the last
                  one

  upgrade         Upgrade db to the given migration
```

There are several commands exist. Each command has its own help available by running
`mongoengine_migrate <command> --help`.

* `makemigrations` detects if mongoengine documents schema has changed and creates new migration
file if needed. By default it loads `models` module in order to read mongoengine document classes.
* `downgrade`, `upgrade` makes database downgrade or upgrade to the given migration appropriately.
* `migrate` command depending on a migration which database is stand on, it performs either
upgrade or downgrade database to the given migration. If migration parameter is missed then 
it upgrades database to the very last migration.

### Dry run mode

`downgrade`, `upgrade`, `migrate` have "dry-run mode" when they just print commands
which would be executed without actually applying\unapplying migration and making changes in 
database. Use `--dry-run` flag to run command in this mode.

Bear in mind that actual MongoDB commands could be slightly different from printed ones in this
mode, because the tool sees unchanged database, but behavior of some commands could depend on 
db changes made by previous commands.

### Migrate without changes

Use `--schema-only` flag to apply migration without making any changes in database. It could be
suitable if you want to upgrade\downgrade database to this migration without making any changes
in database.

This mode is equivalent to temporarily making all actions as "dummy".

### MongoDB version

Usually the version of MongoDB determines automatically. But this process requires right to
run "buildinfo" command on server. If it not possible, you can specify version manually using 
`--mongo-version` argument.
