package db

import (
	"database/sql"
	"fmt"
	"github.com/OpenBazaar/wallet-interface"
	"sync"
)

type ScanBlocksDB struct {
	db   *sql.DB
	lock *sync.RWMutex
}

func (sbdb *ScanBlocksDB) Get(blockHeight int) (wallet.ScanBlock, error) {
	sbdb.lock.RLock()
	defer sbdb.lock.RUnlock()
	var sbs wallet.ScanBlock
	stmt, err := sbdb.db.Prepare("select * from scanBlocks where blockHeight=?")
	if err != nil {
		return sbs, err
	}
	defer stmt.Close()
	var blockHash string
	var isFixScan int
	err = stmt.QueryRow(blockHeight).Scan(&blockHash, &isFixScan)
	if err != nil {
		return sbs, err
	}
	sbs = wallet.ScanBlock{
		BlockHash:   blockHash,
		BlockHeight: blockHeight,
		IsFixScan:   isFixScan,
	}
	return sbs, nil
}

func (sbdb *ScanBlocksDB) Put(blockHeight int, blockHash string, isFixScan int) error {
	sbdb.lock.Lock()
	defer sbdb.lock.Unlock()
	tx, err := sbdb.db.Begin()
	if err != nil {
		return err
	}

	stmt, err := tx.Prepare("insert or replace into scanBlocks(blockHeight, blockHash, isFixScan) values(?,?,?)")
	defer stmt.Close()
	if err != nil {
		tx.Rollback()
		fmt.Println("err is ",err)
		return err
	}
	_, err = stmt.Exec(blockHeight, blockHash, isFixScan)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (sbdb *ScanBlocksDB) UpdateBlock(blockHeight int, blockHash string, isFixScan int) error {
	sbdb.lock.Lock()
	defer sbdb.lock.Unlock()
	tx, err := sbdb.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare("update scanBlocks set blockHash=?, isFixScan=? where blockHeight=?")
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(blockHeight, int(isFixScan), blockHeight)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (sbdb *ScanBlocksDB) Delete(blockHeight int) error {
	sbdb.lock.Lock()
	defer sbdb.lock.Unlock()
	_, err := sbdb.db.Exec("delete from scanBlocks where blockHeight=?", blockHeight)
	if err != nil {
		return err
	}
	return nil
}
