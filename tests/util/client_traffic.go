package util

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

var CT = ClientTraffic{ReadTraffic: true, WriteTraffic: true, TransactionTraffic: true, StopRun: make(chan bool)}

type queryStats struct {
	successCount int
	failureCount int
}

type ClientTraffic struct {
	StopRun            chan bool
	ReadTraffic        bool
	WriteTraffic       bool
	TransactionTraffic bool
}

type ClientTrafficStats struct {
	stats map[string]map[int]*queryStats
}

func (ct ClientTraffic) getNextVal(conn *sql.Conn, ctx context.Context) (int, error) {
	query := "select id_seq.NEXTVAL FROM dual"
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, errors.New("should not have reached")
}

func (ct ClientTraffic) writeInTxn(conn *sql.Conn, ctx context.Context) (int, error) {
	txn, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	insertQuery := "insert into occ_test values(id_seq.NEXTVAL, 'txn-record', 1)"

	_, err = txn.ExecContext(ctx, insertQuery)
	if err != nil {
		txn.Rollback()
		return 0, err
	}

	dbId, err := ct.identifyDBTxn(txn)
	if err != nil {
		return 0, err
	}

	id, err := ct.getNextVal(conn, ctx)
	if err != nil {
		return 0, err
	}
	insertQuery = fmt.Sprintf("insert into occ_test values(%d, 'txnExample', 1)", id)

	_, err = txn.ExecContext(ctx, insertQuery)
	if err != nil {
		txn.Rollback()
		return 0, err
	}

	updateQuery := fmt.Sprintf("update occ_test set version = 2 where id =%d", id)
	_, err = txn.ExecContext(ctx, updateQuery)
	if err != nil {
		txn.Rollback()
		return 0, err
	}

	err = txn.Commit()
	if err != nil {
		txn.Rollback()
		return 0, err
	}

	return dbId, nil

}

func (ct ClientTraffic) identifyDBTxn(txn *sql.Tx) (int, error) {
	query := "select id FROM db_id_test"
	rows, err := txn.Query(query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, errors.New("should not have reached")
}

func (ct ClientTraffic) identifyDB(conn *sql.Conn, ctx context.Context) (int, error) {
	query := "select id FROM db_id_test"
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		return id, nil
	}
	return 0, errors.New("should not have reached")
}

func initQueryStats() map[int]*queryStats {
	r := make(map[int]*queryStats)

	r[0] = &queryStats{0, 0}
	r[1] = &queryStats{0, 0}
	r[2] = &queryStats{0, 0}
	return r
}

func (ct ClientTraffic) checkAndCreateStruct(utc int64, CTS map[int64]ClientTrafficStats) {
	timeMutex.Lock()
	_, ok := CTS[utc]

	if !ok {
		v := make(map[string]map[int]*queryStats)

		v[READ] = initQueryStats()
		v[WRITE] = initQueryStats()
		v[TXN] = initQueryStats()

		CTS[utc] = ClientTrafficStats{v}
	}
	timeMutex.Unlock()

}

func (ct ClientTraffic) CreateCounter(utc int64, counterType string, CTS map[int64]ClientTrafficStats) {
	ct.checkAndCreateStruct(utc, CTS)

	statMutex.Lock()
	_, ok := CTS[utc].stats[counterType]
	if !ok {
		CTS[utc].stats[counterType][0] = &queryStats{}
		CTS[utc].stats[counterType][1] = &queryStats{}
		CTS[utc].stats[counterType][2] = &queryStats{}
	}
	statMutex.Unlock()

}

func (ct ClientTraffic) txnTraffic(CTS map[int64]ClientTrafficStats, n int64) {
	ct.CreateCounter(n, TXN, CTS)

	conn, ctx, err := GetConnection()
	dbId := 0
	if err == nil {
		dbId, err = ct.writeInTxn(conn, ctx)
	}
	if err != nil {
		ct.incrementFailure(TXN, dbId, CTS[n].stats)
	} else {
		ct.incrementSuccess(TXN, dbId, CTS[n].stats)
	}
}

func (ct ClientTraffic) writeTraffic(CTS map[int64]ClientTrafficStats, n int64) {
	ct.CreateCounter(n, WRITE, CTS)
	id := 0
	conn, ctx, err := GetConnection()
	failed := true
	if err == nil {
		defer conn.Close()
		insertQuery := "insert into occ_test values(id_seq.NEXTVAL, 'write-record', 1)"

		txn, err := conn.BeginTx(ctx, nil)

		if err == nil {
			id, err = ct.identifyDBTxn(txn)
			if err == nil {
				_, err = txn.ExecContext(ctx, insertQuery)
				if err == nil {
					err = txn.Commit()
					failed = false
				} else {
					txn.Rollback()
				}
			} else {
				txn.Rollback()
			}
		} else {
			txn.Rollback()
		}
	}

	if failed {
		ct.incrementFailure(WRITE, id, CTS[n].stats)
	} else {
		ct.incrementSuccess(WRITE, id, CTS[n].stats)
	}

}

func (ct ClientTraffic) readTraffic(CTS map[int64]ClientTrafficStats, n int64) {
	ct.CreateCounter(n, READ, CTS)
	id := 0
	conn, ctx, err := GetConnection()
	if err == nil {
		defer conn.Close()
		id, err = ct.identifyDB(conn, ctx)
	}
	if err != nil {
		ct.incrementFailure(READ, id, CTS[n].stats)
	} else {
		ct.incrementSuccess(READ, id, CTS[n].stats)
	}
}

func (ct ClientTraffic) dumpStats(CTS map[int64]ClientTrafficStats) {
	fmt.Println("**************")
	fmt.Println("QueryStats")
	fmt.Println("**************")
	fmt.Println("UTC:\t\tDB1 Read Success/Fail\t\tDB2 Read Success/Fail\tDB1 Write Success/Fail" +
		"\tDB2 Write Success/Fail\tDB1 Txn Success/Fail\tDB2 Tx Success/Fail\nAll DB Failure")
	keys := make([]int64, 0)
	for k, _ := range CTS {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})

	for _, utc := range keys {
		cts := CTS[utc]
		fmt.Printf("%d:\t\t\t%d/%d\t\t\t%d/%d\t\t\t%d/%d\t\t\t%d/%d\t\t\t%d/%d\t\t\t%d/%d\t\t\t%d\n", utc,
			cts.stats[READ][1].successCount, cts.stats[READ][1].failureCount, cts.stats[READ][2].successCount, cts.stats[READ][2].failureCount,
			cts.stats[WRITE][1].successCount, cts.stats[WRITE][1].failureCount, cts.stats[WRITE][2].successCount, cts.stats[WRITE][2].failureCount,
			cts.stats[TXN][1].successCount, cts.stats[TXN][1].failureCount, cts.stats[TXN][2].successCount, cts.stats[TXN][2].failureCount,
			cts.stats[READ][0].failureCount+cts.stats[WRITE][0].failureCount+cts.stats[TXN][0].failureCount)
	}

}

func (ct ClientTraffic) SendClientTraffic(wg *sync.WaitGroup) chan map[int64]ClientTrafficStats {
	wg.Add(1)
	CTSChan := make(chan map[int64]ClientTrafficStats)
	go ct.traffic(wg, ct.StopRun, CTSChan)
	return CTSChan
}

func (ct ClientTraffic) StopClientTraffic(CTSChan chan map[int64]ClientTrafficStats) map[int64]ClientTrafficStats {
	ct.StopRun <- true
	d := <-CTSChan
	ct.dumpStats(d)
	return d
}

func (ct ClientTraffic) traffic(wg *sync.WaitGroup, stopRun chan bool, CTChan chan map[int64]ClientTrafficStats) {
	defer wg.Done()

	CTS := make(map[int64]ClientTrafficStats)

	cnt := 0
	fmt.Println("Sending Client Traffic")
	for {
		select {
		case <-stopRun:
			fmt.Println("Stopping Client Traffic")
			CTChan <- CTS
			return
		default:
			n := time.Now().Unix()
			if cnt == 0 {
				fmt.Printf("Traffic StartTime %d\n", n)
			}
			go ct.readTraffic(CTS, n)
			go ct.txnTraffic(CTS, n)
			go ct.writeTraffic(CTS, n)
			cnt += 1
			time.Sleep(300 * time.Millisecond)
		}
	}

}

func (ct ClientTraffic) getMutexForType(qsType string) *sync.Mutex {
	if qsType == READ {
		return &readMutex
	}
	if qsType == WRITE {
		return &writeMutex
	}
	if qsType == TXN {
		return &txnMutex
	}
	panic("unknown type")
}

func (ct ClientTraffic) incrementSuccess(qsType string, dbId int, stats map[string]map[int]*queryStats) {
	m := ct.getMutexForType(qsType)
	m.Lock()
	stats[qsType][dbId].successCount += 1
	m.Unlock()
}

func (ct ClientTraffic) incrementFailure(qsType string, dbId int, stats map[string]map[int]*queryStats) {
	m := ct.getMutexForType(qsType)
	m.Lock()
	stats[qsType][dbId].failureCount += 1
	m.Unlock()
}
