package stats

import (
	"database/sql"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3" // sqlite3
)

// StoreClientInformation stores the results of information of the test
func StoreClientInformation(hostname string, clientID int, consensus int, gomaxprocs int, fkExperiment int64, databaseFile string, conflicts int, writes int, testDuration time.Duration, successful int64, throughput int64, latencyAvg float64) (int64, error) {

	db, err := sql.Open("sqlite3", databaseFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := db.Prepare("INSERT INTO clients (fk_experiment, hostname, thread_id, duration, successful, throughput, latency_avg, conflicts, writes) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}
	result, err := stmt.Exec(fkExperiment, hostname, clientID, testDuration, successful, throughput, latencyAvg, conflicts, writes)

	tx.Commit()

	return result.LastInsertId()
}

// StoreReplicaInformation is used by replicas to store their param
func StoreReplicaInformation(databaseFile string, hostname string, experiment int64) error {
	db, err := sql.Open("sqlite3", databaseFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := db.Prepare("INSERT INTO replicas (hostname, fk_experiment) VALUES (?, ?)")
	if err != nil {
		panic(err)
	}
	stmt.Exec(hostname, experiment)

	tx.Commit()

	return err
}

// StoreExperimentInformation stores the initial information of the test
func StoreExperimentInformation(databaseFile string, fkProtocol int, thrifty bool, beacon bool, execute bool, executeThreads int, replyAfterExecute bool, populate int64, gomaxprocsReplicas int, gomaxprocsClients int, batchWait int) (int64, error) {

	db, err := sql.Open("sqlite3", databaseFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := db.Prepare("INSERT INTO experiments (fk_protocol, thrifty, beacon, execute, execute_threads, reply_after_execute, populate, gomaxprocs_replicas, gomaxprocs_clients, batchWait) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
	if err != nil {
		panic(err)
	}
	result, err := stmt.Exec(fkProtocol, thrifty, beacon, execute, executeThreads, replyAfterExecute, populate, gomaxprocsReplicas, gomaxprocsClients, batchWait)

	tx.Commit()

	return result.LastInsertId()
}

// StoreClientLatency stores the latency of information of each client
func StoreClientLatency(databaseFile string, fkClient int64, latency []int64, fkExperiment int64) error {

	db, err := sql.Open("sqlite3", databaseFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	for _, l := range latency {
		stmt, err := db.Prepare("INSERT INTO latency (fk_client, latency, fk_experiment) VALUES (?, ?, ?)")
		if err != nil {
			panic(err)
		}
		stmt.Exec(fkClient, l, fkExperiment)

	}
	tx.Commit()

	return err
}

// UpdateExperimentLatency is used by clients to store the results in database
func UpdateExperimentLatency(databaseFile string, fkExperiment int64, mean float64, median float64, percentile5 float64, percentile25 float64, percentile50 float64, percentile75 float64, percentile90 float64, percentile95 float64, percentile99 float64) error {
	db, err := sql.Open("sqlite3", databaseFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := db.Prepare("UPDATE experiments SET mean=?, median=?, percentile_5=?, percentile_25=?, percentile_50=?, percentile_75=?, percentile_90=?, percentile_95=?, percentile_99=? WHERE id_experiment=?")
	if err != nil {
		panic(err)
	}
	stmt.Exec(mean, median, percentile5, percentile25, percentile50, percentile75, percentile90, percentile95, percentile99, fkExperiment)
	tx.Commit()

	return err
}

// GetLastExperimentNumber return the ID of last experiment stored in database
func GetLastExperimentNumber(databaseFile string) int {
	db, err := sql.Open("sqlite3", databaseFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}
	stmt, err := db.Query("select max(experiment) as experiment from results")
	if err != nil {
		panic(err)
	}
	defer stmt.Close()
	for stmt.Next() {
		var experiment int
		err = stmt.Scan(&experiment)
		if err != nil {
			log.Fatal(err)
		}
		return experiment
	}
	tx.Commit()

	return 0
}
