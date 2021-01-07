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

There are several commands available. Each command has its own help available by running
`mongoengine_migrate <command> --help`.

* `makemigrations` detects if mongoengine documents schema has changed and creates new migration
file if needed. By default it loads `models` module in order to read mongoengine document classes.
* `downgrade`, `upgrade` makes downgrade or upgrade appropriately to the given migration
* `migrate` command includes the behavior of `downgrade` and `upgrade` commands. It downgrades or
upgrades to the given migration automatically. If migration parameter is missed then it upgrades
to the very last migration.

### Dry run mode

`downgrade`, `upgrade`, `migrate` have "dry-run mode" when they just print what would be
done without actually applying a migration. Use `--dry-run` flag to run command in this mode.

Bear in mind that actual MongoDB commands could be slightly different from printed ones, because
in dry-run mode the tool always sees unchanged database, but behavior of some of these commands 
depend on db changes made by previous commands.

### Migrate without changes

Use `--schema-only` flag to apply migration without making any changes in database. It could be
suitable if you want to start using migrations on database which already contains data.
Or if you want to skip migration changes, but you want to mark this migration as "applied".

### MongoDB version

Usually the version of MongoDB determines automatically, but it requires restriction to run
"buildinfo" command. If it not possible, you can specify version explicitly using 
`--mongo-version` argument.
