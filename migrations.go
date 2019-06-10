package cworm

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/codewinks/go-colors"
)

//go:generate go-bindata -pkg pg -mode 0644 -modtime 499137600 -o db_migrations_generated.go schema/

func Migrate(cmd string) (msg string, err error) {
	db, err := Connect(os.Getenv("DB_CONNECTION"), os.Getenv("DB_USERNAME"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_DATABASE"))
	if err != nil {
		return "", fmt.Errorf("Failed to connect to database: %s", err)
	}

	defer db.DB.Close()

	switch cmd {
	case "fresh":
		db.disableForeignKeyConstraints()
		err = db.dropAllTables()
		db.enableForeignKeyConstraints()
		err = db.runMigrations()
	case "refresh":
	case "reset":
	case "rollback":
	case "status":
		err = db.status()
	case "migrate":
		err = db.runMigrations()
	default:
		return "", fmt.Errorf(fmt.Sprintf("Command \"%s\" is not defined", cmd))
	}

	return
}

func (db *DB) runMigrations() (err error) {
	err = db.createMigrationsTable()
	if err != nil {
		return err
	}

	// TODO: [ ] Create better migration diff check
	count, err := db.countMigrations()
	if err != nil {
		return err
	}

	migrations, err := getMigrationFiles()

	if count >= len(migrations) {
		fmt.Println(colors.Green("Nothing to migrate."))
		return nil
	}

	batch := db.getNextBatch()

	for i, file := range migrations {
		// skip running ones we've clearly already ran
		if count > 0 {
			count--
			continue
		}

		cleanName := strings.TrimPrefix(file, "database/migrations/")

		fmt.Printf("%s%-12s%s %s\n", colors.YELLOW, "Migrating:", colors.NC, cleanName)

		migration, err := ioutil.ReadFile(file)
		if err != nil {
			return err
		}

		err = db.runMigration(i, migration)
		if err != nil {
			return err
		}

		err = db.recordMigration(cleanName, batch)
		if err != nil {
			return err
		}

		fmt.Printf("%s%-12s%s %s\n", colors.GREEN, "Migrated:", colors.NC, cleanName)
	}

	return nil
}
func getMigrationFiles() ([]string, error) {
	files, err := filepath.Glob("database/migrations/*.sql")
	if err != nil {
		panic(err.Error())
	}

	sort.Strings(files)

	return files, nil
}

func (db *DB) getBatch() (batch int) {
	rows := db.DB.QueryRow("SELECT MAX(batch) FROM migrations;")
	rows.Scan(&batch)

	return
}

func (db *DB) getNextBatch() (batch int) {
	rows := db.DB.QueryRow("SELECT MAX(batch)+1 FROM migrations;")
	rows.Scan(&batch)

	return
}

func (db *DB) dropAllTables() error {
	var tableName string
	var tableType string
	var allTables []string

	rows, err := db.DB.Query(`SHOW FULL TABLES WHERE table_type = 'BASE TABLE'`)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&tableName, &tableType)
		if err != nil {
			panic(err)
		}

		allTables = append(allTables, tableName)
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	if len(allTables) > 0 {
		_, err = db.DB.Exec(fmt.Sprintf(`DROP TABLE %s CASCADE`, strings.Join(allTables, ",")))
		if err != nil {
			panic(err)
		}
	}

	fmt.Println(colors.Green("Dropped all tables successfully."))

	return err
}

func (db *DB) createMigrationsTable() error {
	_, err := db.DB.Exec(`CREATE TABLE IF NOT EXISTS migrations (
		id int(10) unsigned NOT NULL AUTO_INCREMENT,
		migration varchar(191) COLLATE utf8mb4_unicode_ci NOT NULL,
		batch int(11) NOT NULL,
		PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`)

	return err
}

func (db *DB) countMigrations() (int, error) {
	row := db.DB.QueryRow(`SELECT count(id) FROM migrations;`)

	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0, err
	}

	return count, nil
}

func (db *DB) runMigration(num int, buf []byte) error {
	_, err := db.DB.Exec(string(buf))
	return err
}

func (db *DB) recordMigration(name string, batch int) error {
	_, err := db.DB.Query("INSERT INTO migrations (migration, batch) VALUES (?, ?);", name, batch)
	return err
}

func (db *DB) enableForeignKeyConstraints() error {
	_, err := db.DB.Exec(`SET FOREIGN_KEY_CHECKS=1;`)
	return err
}

func (db *DB) disableForeignKeyConstraints() error {
	_, err := db.DB.Exec(`SET FOREIGN_KEY_CHECKS=0;`)
	return err
}

func (db *DB) getMigrations() (map[string]int, error) {
	var name string
	var batch int
	migrations := make(map[string]int)

	rows, err := db.DB.Query(`SELECT migration, batch FROM migrations ORDER BY id DESC`)
	if err != nil {
		panic(err)
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&name, &batch)
		if err != nil {
			panic(err)
		}

		migrations[name] = batch
	}

	err = rows.Err()
	if err != nil {
		panic(err)
	}

	return migrations, nil
}

func (db *DB) status() error {
	migrations, err := getMigrationFiles()
	if err != nil {
		return err
	}

	currentMigrations, err := db.getMigrations()

	var tableRows []string
	var maxLength int

	for _, file := range migrations {
		name := strings.TrimPrefix(file, "database/migrations/")

		if len(name) > maxLength {
			maxLength = len(name)
		}
	}

	for _, file := range migrations {
		status := colors.Red("No")
		batch := ""
		name := strings.TrimPrefix(file, "database/migrations/")

		if val, ok := currentMigrations[name]; ok {
			status = colors.Green("Yes")
			batch = strconv.Itoa(val)
		}

		tableRows = append(tableRows, fmt.Sprintf("| %s | %s | %s |", fmt.Sprintf("%-15s", status), fmt.Sprintf("%-*s", maxLength, name), fmt.Sprintf("%-5s", batch)))
	}

	table := fmt.Sprintf(`+------+%[1]s--+-------+
| Ran? | %[2]s | Batch |
+------+%[1]s--+-------+
%[3]s
+------+%[1]s--+-------+
`, strings.Repeat("-", maxLength), fmt.Sprintf("%-*s", maxLength, "Migration"), strings.Join(tableRows, "\n"))

	fmt.Printf(table)

	return nil
}
