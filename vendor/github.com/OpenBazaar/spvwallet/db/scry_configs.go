package db

import (
	"database/sql"
	"fmt"
	"github.com/OpenBazaar/wallet-interface"
	"sync"
)

type ScryConfigsDB struct {
	db   *sql.DB
	lock *sync.RWMutex
}

func (sbdb *ScryConfigsDB) Get(scryKey string) (wallet.ScryConfig, error) {
	sbdb.lock.RLock()
	defer sbdb.lock.RUnlock()
	var sbs wallet.ScryConfig
	stmt, err := sbdb.db.Prepare("select * from scryConfigs where scryKey=?")
	if err != nil {
		return sbs, err
	}
	defer stmt.Close()
	var scryValue string
	err = stmt.QueryRow(scryKey).Scan(&scryValue)
	if err != nil {
		return sbs, err
	}
	sbs = wallet.ScryConfig{
		ScryKey:   scryKey,
		ScryValue: scryValue,
	}
	return sbs, nil
}

func (sbdb *ScryConfigsDB) Put(scryKey string, scryValue string) error {
	sbdb.lock.Lock()
	defer sbdb.lock.Unlock()
	tx, err := sbdb.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("insert or replace into scryConfigs(scryKey, scryValue) values(?,?,?)")
	defer stmt.Close()
	if err != nil {
		tx.Rollback()
		fmt.Println("err is ", err)
		return err
	}
	_, err = stmt.Exec(scryKey, scryValue)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (sbdb *ScryConfigsDB) UpdateBlock(scryKey string, scryValue string) error {
	sbdb.lock.Lock()
	defer sbdb.lock.Unlock()
	tx, err := sbdb.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("update scryConfigs set scryValue=? where scryKey=?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(scryValue, scryKey)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (sbdb *ScryConfigsDB) Delete(scryKey string) error {
	sbdb.lock.Lock()
	defer sbdb.lock.Unlock()
	_, err := sbdb.db.Exec("delete from scryConfigs where scryKey=?", scryKey)
	if err != nil {
		return err
	}
	return nil
}
