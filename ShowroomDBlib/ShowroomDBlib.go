/*!
Copyright © 2024 chouette.21.00@gmail.com
Released under the MIT license
https://opensource.org/licenses/mit-license.php

SRGPC .. 配信終了時の貢献ポイントを毎回取得し、配信枠ごとのリスナー別貢献ポイントを算出する。

本パッケージはDBアクセスのための関数群


*/

package ShowroomDBlib

import (
	"fmt"
	//	"io/ioutil"
	"os"
	"time"

	"log"

	"gopkg.in/yaml.v2"

	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	"github.com/Chouette2100/exsrapi"
)

/*
Ver.20A00	結果をDBで保存する。Excel保存の機能は残存。次に向けての作り込み少々。
Ver.2.0B00		データ取得のタイミングをtimetableから得る。Excelへのデータの保存をやめる。
Ver.2.0B01	timetableの更新で処理が終わっていないものを処理済みにしていた問題を修正する。
Ver.2.0B01	timetableの更新で処理が終わっていないものを処理済みにしていた問題を修正する。
Ver.2.0B02	Prepare()に対するdefer Close()の抜けを補う。
Ver.3.0A00	SHOWROOMに新たに導入された貢献リスナーのユーザーIDがわかるAPIを利用して貢献ポイント算出の精度を上げる。
*/
const Version = "30A00"

type EventRank struct {
	Order    int
	Rank     int    //	貢献順位
	Listner  string //	リスナー名
	Lastname string //	前配信枠でのリスナー名

	LsnID       int //	リスナーのユーザID（Ver.3.0A00より前のバージョンではAPIで取得できなかったため0がセットされている）
	T_LsnID     int //	Ver.3.0A00より前のバージョンで用いたリスナー識別のための（仮の）ユーザーID（イベントごとに異なる）
	Point       int //	貢献ポイント
	Incremental int //	貢献ポイントの増分（＝配信枠別貢献ポイント）
	Status      int
}

// 構造体のスライス
type EventRanking []EventRank

// sort.Sort()のための関数三つ
func (e EventRanking) Len() int {
	return len(e)
}

func (e EventRanking) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// 降順に並べる
func (e EventRanking) Less(i, j int) bool {
	//	return e[i].point < e[j].point
	return e[i].Point > e[j].Point
}

type DBConfig struct {
	WebServer string `yaml:"WebServer"`
	HTTPport  string `yaml:"HTTPport"`
	SSLcrt    string `yaml:"SSLcrt"`
	SSLkey    string `yaml:"SSLkey"`
	Dbhost    string `yaml:"Dbhost"`
	Dbname    string `yaml:"Dbname"`
	Dbuser    string `yaml:"Dbuser"`
	Dbpw      string `yaml:"Dbpw"`
	UserApi   bool   `yaml:"UserApi"`
}

var Db *sql.DB
var Err error

// 設定ファイルを読み込む
//
//	以下の記事を参考にさせていただきました。
//	        【Go初学】設定ファイル、環境変数から設定情報を取得する
//	                https://note.com/artefactnote/n/n8c22d1ac4b86
func LoadConfig(filePath string) (dbconfig *DBConfig, err error) {
	//	content, err := ioutil.ReadFile(filePath)
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	content = []byte(os.ExpandEnv(string(content)))

	result := &DBConfig{}
	if err := yaml.Unmarshal(content, result); err != nil {
		return nil, err
	}

	return result, nil
}

func OpenDb(dbconfig *DBConfig) (status int) {

	status = 0

	if dbconfig.Dbhost == "" {
		Db, Err = sql.Open("mysql", (*dbconfig).Dbuser+":"+(*dbconfig).Dbpw+"@/"+(*dbconfig).Dbname+"?parseTime=true&loc=Asia%2FTokyo")
	} else {
		Db, Err = sql.Open("mysql", (*dbconfig).Dbuser+":"+(*dbconfig).Dbpw+"@tcp("+(*dbconfig).Dbhost+":3306)/"+(*dbconfig).Dbname+"?parseTime=true&loc=Asia%2FTokyo")
	}

	if Err != nil {
		status = -1
	}
	return
}

func InsertIntoEventrank(
	eventid string,
	userno int,
	sampletm2 time.Time,
	eventranking EventRanking,
) (
	status int,
) {

	status = 0

	ts := time.Now().Truncate(time.Minute)

	var stmt *sql.Stmt
	sql := "INSERT INTO eventrank(eventid, userid, ts, listner, lastname, lsnid, t_lsnid, norder, nrank, point, increment, status)"
	sql += " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
	stmt, Err = Db.Prepare(sql)
	if Err != nil {
		log.Printf("InsertIntoPoints() prepare() err=[%s]\n", Err.Error())
		status = -1
	}
	defer stmt.Close()

	for _, evr := range eventranking {

		_, Err = stmt.Exec(eventid, userno, ts, evr.Listner, evr.Lastname, evr.LsnID, evr.T_LsnID, evr.Order, evr.Rank, evr.Point, evr.Incremental, 0)

		if Err != nil {
			log.Printf("InsertIntoEventrank() exec() err=[%s]\n", Err.Error())
			status = -1
		}
	}

	return
}

func SelectMaxTsFromEventrank(
	eventid string,
	userid int,
) (
	ndata int,
	maxts time.Time,
) {

	//	獲得ポイントのデータが何セットあるか調べる。
	sql := "select count(ts) from (select distinct(ts) from eventrank where eventid = ? and userid = ? ) tmptable"
	Err = Db.QueryRow(sql, eventid, userid).Scan(&ndata)

	if Err != nil {
		log.Printf("select count(ts) from (select distinct(ts) from eventrank  where eventid = %s and userid = %d ) tmptable ==> %d\n", eventid, userid, ndata)
		log.Printf("err=[%s]\n", Err.Error())
		ndata = -1
		return
	}
	if ndata == 0 {
		return
	}

	//	直近の獲得ポイントデータのタイムスタンプを取得する。
	sql = "select max(ts) from eventrank where eventid = ? and userid = ? "
	Err = Db.QueryRow(sql, eventid, userid).Scan(&maxts)

	if Err != nil {
		log.Printf("error [select max(ts) from eventrank where eventid = %s and userid = %d]\n", eventid, userid)
		log.Printf("err=[%s]\n", Err.Error())
		ndata -= 1000
		return
	}
	log.Printf("select max(ts) from eventrank where eventid = %s and userid = %d ==> %v\n", eventid, userid, maxts)
	return

}

// 未処理の（＝配信枠別リスナー別獲得ポイントが算出されていない）配信枠で最初のもののイベントIDとルームIDとデータ取得時刻をDBから抽出する。
func SelectEidUidFromTimetable() (
	ndata int, //	未処理の配信枠の数
	eventid string, //	未処理の最初の配信枠のイベントID
	userid int, //	未処理の最初の配信枠のルームID
	sampletm1 time.Time, //	未処理の最初の配信枠のデータ取得時刻
) {

	//
	sql := "select count(*) from timetable where sampletm1 < ? and status = 0"
	tnow := time.Now()
	Err = Db.QueryRow(sql, tnow).Scan(&ndata)

	if Err != nil {
		log.Printf("error [select count(*) from timetable where sampletm1 < %v and status = 0 ]\n", tnow)
		log.Printf("err=[%s]\n", Err.Error())
		ndata = -1
		return
	}
	if ndata == 0 {
		return
	}

	//	獲得ポイントデータを取得すべきイベント、ユーザーIDを取得する。
	sql = "select eventid, userid, sampletm1 from timetable where status = 0 and sampletm1 = (select min(sampletm1) from timetable where status = 0)"
	Err = Db.QueryRow(sql).Scan(&eventid, &userid, &sampletm1)

	if Err != nil {
		log.Printf("error [select eventid, userid, sampletm1 from timetable where status = 0 and sampletm1 = (select min(sampletm1) from timetable where status = 0)]\n")
		log.Printf("err=[%s]\n", Err.Error())
		ndata -= 1000
		return
	}
	log.Printf("select eventid, userid, sampletm1 from timetable where status = 0 and sampletm1 = (select min(sampletm1) from timetable where status = 0) ==> %s %d %v\n", eventid, userid, sampletm1)
	return

}

func SelectMaxTlsnidFromEventranking(
	eventid string,
	userid int,
) (
	maxtlsnid int,
) {

	//
	sql := "select max(t_lsnid) from eventrank where eventid =  ? and userid = ? "
	Err = Db.QueryRow(sql, eventid, userid).Scan(&maxtlsnid)

	if Err != nil {
		log.Printf("error [select max(t_lsnid) from eventrank where eventid =  %s and userid = %d ]\n", eventid, userid)
		log.Printf("err=[%s]\n", Err.Error())
		maxtlsnid = -1000
	}
	return
}

func UpdateTimetable(
	eventid string,
	userid int,
	sampletm1 time.Time,
	sampletm2 time.Time,
	totalpoint int,
) (
	status int,
) {

	var stmt *sql.Stmt

	status = 0

	sql := "update timetable set sampletm2 = ?, totalpoint = ?, status = 1 where eventid = ? and userid = ? and sampletm1 = ? and status = 0"
	stmt, Err = Db.Prepare(sql)
	if Err != nil {
		log.Printf("update timetable set sampletm2 = %v, totalpoint = %d, status = 1 where eventid = %s and userid = %d and sampletm1 = %v and status = 0 error (Update/Prepare) err=%s\n", sampletm2, totalpoint, eventid, userid, sampletm1, Err.Error())
		status = -1
		return
	}
	defer stmt.Close()

	_, Err = stmt.Exec(sampletm2, totalpoint, eventid, userid, sampletm1)

	if Err != nil {
		log.Printf("update timetable set sampletm2 = %v, totalpoint = %d, status = 1 where eventid = %s and userid = %d and sampletm1 = %v and status = 0 error (Update/Prepare) err=%s\n", sampletm2, totalpoint, eventid, userid, sampletm1, Err.Error())
		status = -2
	}

	return
}

func SelectEventRankingFromEventrank(
	eventid string,
	userid int,
	ts time.Time,
) (
	eventranking EventRanking,
	status int,
) {

	var stmt *sql.Stmt
	var rows *sql.Rows

	status = 0

	//	直近の獲得ポイントデータを読み込む
	sql := "SELECT listner, lastname, lsnid, t_lsnid, norder, nrank, point, increment, status "
	sql += " FROM eventrank WHERE eventid = ? and userid = ? and ts = ? order by norder"

	stmt, Err = Db.Prepare(sql)
	if Err != nil {
		log.Printf("err=[%s]\n", Err.Error())
		status = -1
		return
	}
	defer stmt.Close()

	rows, Err = stmt.Query(eventid, userid, ts)
	if Err != nil {
		log.Printf("err=[%s]\n", Err.Error())
		status = -2
		return
	}
	defer rows.Close()

	var evr EventRank

	for rows.Next() {
		Err = rows.Scan(&evr.Listner, &evr.Lastname, &evr.LsnID, &evr.T_LsnID, &evr.Order, &evr.Rank, &evr.Point, &evr.Incremental, &evr.Status)
		if Err != nil {
			log.Printf("err=[%s]\n", Err.Error())
			status = -3
			return
		}
		eventranking = append(eventranking, evr)
	}
	if Err = rows.Err(); Err != nil {
		log.Printf("err=[%s]\n", Err.Error())
		status = -4
		return
	}

	return

}

func SelectFromEvent(eventid string) (
	peventinf *exsrapi.Event_Inf,
	err error,
) {

	Tevent := "event"

	eventinf := exsrapi.Event_Inf{}
	peventinf = &eventinf

	sql := "select eventid,ieventid,event_name, period, starttime, endtime, noentry, intervalmin, modmin, modsec, "
	sql += " Fromorder, Toorder, Resethh, Resetmm, Nobasis, Maxdsp, cmap, target, `rstatus`, maxpoint "
	sql += " from " + Tevent + " where eventid = ?"
	Dberr := Db.QueryRow(sql, eventid).Scan(
		&eventinf.Event_ID,
		&eventinf.I_Event_ID,
		&eventinf.Event_name,
		&eventinf.Period,
		&eventinf.Start_time,
		&eventinf.End_time,
		&eventinf.NoEntry,
		&eventinf.Intervalmin,
		&eventinf.Modmin,
		&eventinf.Modsec,
		&eventinf.Fromorder,
		&eventinf.Toorder,
		&eventinf.Resethh,
		&eventinf.Resetmm,
		&eventinf.Nobasis,
		&eventinf.Maxdsp,
		&eventinf.Cmap,
		&eventinf.Target,
		&eventinf.Rstatus,
		&eventinf.Maxpoint,
	)

	if Dberr != nil {
		if Dberr.Error() != "sql: no rows in result set" {
			peventinf = nil
			return
		} else {
			err = fmt.Errorf("row.Exec(): %w", Dberr)
			log.Printf("%s\n", sql)
			log.Printf("err=[%v]\n", err)
			return
		}
	}

	//	log.Printf("eventno=%d\n", Event_inf.Event_no)

	start_date := eventinf.Start_time.Truncate(time.Hour).Add(-time.Duration(eventinf.Start_time.Hour()) * time.Hour)
	end_date := eventinf.End_time.Truncate(time.Hour).Add(-time.Duration(eventinf.End_time.Hour())*time.Hour).AddDate(0, 0, 1)

	//	log.Printf("start_t=%v\nstart_d=%v\nend_t=%v\nend_t=%v\n", Event_inf.Start_time, start_date, Event_inf.End_time, end_date)

	eventinf.Start_date = float64(start_date.Unix()) / 60.0 / 60.0 / 24.0
	eventinf.Dperiod = float64(end_date.Unix())/60.0/60.0/24.0 - eventinf.Start_date

	eventinf.Gscale = eventinf.Maxpoint % 1000
	eventinf.Maxpoint = eventinf.Maxpoint - eventinf.Gscale

	//	log.Printf("eventinf=[%v]\n", eventinf)

	//	log.Printf("Start_data=%f Dperiod=%f\n", eventinf.Start_date, eventinf.Dperiod)

	return
}

