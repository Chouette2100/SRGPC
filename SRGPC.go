/*
!
Copyright © 2022 chouette.21.00@gmail.com
Released under the MIT license
https://opensource.org/licenses/mit-license.php

	SHOWROOMのイベント貢献ランキングを取得する関数の使用例です。

	使い方
		イベントページのURLが
			https://www.showroom-live.com/event/event_id
		で、配信者さんのID（SHOWROOMへの登録順を示すと思われる6桁以下の数字）が
			ID_Account
		のとき

			% 実行モジュール名 event_id ID_Account

		と実行すると貢献ランキングの一覧が表示されます。

		例えば現時点(2019/07/28)でいちばん目立つところにあるイベントの一位さんの貢献ランキングを知りたいときは

			% 実行モジュール名 kc19aw_final 120380

		とします。

		Webページからのデータ取得の参考としては以下などがあります。
			「はじめてのGo言語：Golangでスクレイピングをしてみた」
			https://qiita.com/ryo_naka/items/a08d70f003fac7fb0808
			※ ざくっとした内容ですがGO言語のインストールのところから書いてあります。
			※ お時間のある方は系統的に書かれたものを探してじっくり勉強してください。
*/
package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"log"

	//	"bufio"
	//	"io"
	//	"io/ioutil"
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"

	//	"database/sql"
	//	_ "github.com/go-sql-driver/mysql"

	"SRGPC/ShowroomDBlib"
	"github.com/go-gorp/gorp"
	//	"Showroomlib"

	"github.com/PuerkitoBio/goquery"

	"net/http"
	//	"net/url"

	"github.com/360EntSecGroup-Skylar/excelize"
	lsdp "github.com/deltam/go-lsd-parametrized"

	"github.com/Chouette2100/exsrapi/v2"
	"github.com/Chouette2100/srapi/v2"
	"github.com/Chouette2100/srdblib/v2"
)

/*
1.0A0	一致度のチェックで
				case second_v < 1.1 && second_v-first_v > 0.2:
			を
				case second_v < 1.1 && second_v-first_v > 0.2:
			と訂正した（"3C"と判定すべきものが"3B"と判定されていた）
1.0A1		「一致度のチェック対象が一つしかない」の判定条件を見直し
1.0B0		Sheet2への差分の書き込み処理を追加
1.0B1		順位（rank）をExcelに出力するようにした。
1.0C0		毎回別のファイルに書き込むようにした（データ書き込み後Excelファイルが壊れているケースがあるため）
1.0D0		Go Ver.1.17.4対応（import関連の修正、まだ3つの"問題"あり）
1.0D1		importのShowroomLibを正しいShowroomlibに修正する。
2.0A00		データの保存をDBに行う。
2.0B00		データ取得のタイミングをtimetableから得る。Excelへのデータの保存をやめる。
2.0B01		ムリな突き合わせをしないようにする。一致度を厳しくし、いったんランク外になったリスナーは突き合わせの対象としない。
2.0B02		リスナー名が変化していないときはLastnameをクリアする。
2.0B03		timetableの更新で処理が終わっていないものを処理済みにしていた問題を修正する。
			（この問題は通常発生しない。デバッグのため一つの配信に対して複数回の貢献ポイント取得を行ったときに発生した）
2.0C00		実行を指定した時間で打ち切るようにする。さくらインターネットのレンタルサーバでデーモンとみなされないための設定。
020AD00	WaitNextMinute()を取り込みShowroomlibをimportしない。
020AE00	ブロックランキングでEvent_url_keyに"?block_id=..."が付加されているときはこれを除いて貢献ランキングを取得する。
020AE01	exsrapiの関数の戻り値の変更に対する対応の誤りを正す。
020AF00	標準出力へのログ出力をやめる。
020AF01	標準出力への出力を削除する。
030AA04	貢献ポイントの取得にAPIを用い、リスナーの突き合わせにはuseridを用いる。
030AA05	引数のエラーがログファイルに出力されるようにする。
030AA06	引数のエラーのチェックをやめる。
030AB00	貢献ポイントを取得するときreturned empty rankingが発生した場合の（暫定）対策を行う
030AB01	貢献ポイントを取得するときreturned empty rankingが発生した場合の対策を行う
030AB02	Dbmapの設定誤りを修正する。
3.0A01 接続先の指定にDbportを追加する。
30AC01	パッケージをv2に変更する。ApiEventContribution_ranking()のエラーは時間をおいてリトライする。
30AD00	グレイスフルシャットダウンを行う
30AD01	fmt.Printf()を原則としてlog.Printf()に置き換える。ループでのウェイト時間の出力は行わない。

*/

const version = "30AD01"

const UseApi = true

type Environment struct {
	IntervalHour int
}

/*
type DBConfig struct {
	WebServer string `yaml:"WebServer"`
	HTTPport  string `yaml:"HTTPport"`
	SSLcrt    string `yaml:"SSLcrt"`
	SSLkey    string `yaml:"SSLkey"`
	Dbhost    string `yaml:"Dbhost"`
	Dbname    string `yaml:"Dbname"`
	Dbuser    string `yaml:"Dbuser"`
	Dbpw      string `yaml:"Dbpw"`
}
*/

//	var Db *sql.DB
//	var Err error

/*
type EventRank struct {
	Order       int
	Rank        int
	Listner     string
	Lastname    string
	LsnID		int
	T_LsnID		int
	Point       int
	Incremental int
	Status      int
}

// 構造体のスライス
type EventRanking []EventRank

//	sort.Sort()のための関数三つ
func (e EventRanking) Len() int {
	return len(e)
}

func (e EventRanking) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

//	降順に並べる
func (e EventRanking) Less(i, j int) bool {
	//	return e[i].point < e[j].point
	return e[i].Point > e[j].Point
}

*/

/*
GetPointsContByApi()
イベントページのURLと配信者さんのIDから、イベント貢献ランキングのリストを取得します。

引数
EvnetName	string	イベント名、下記イベントページURLの"event_id"の部分

	https://www.showroom-live.com/event/event_id

ID_Account	string	配信者さんのID

	SHOWROOMへの登録順を示すと思われる6桁(以下)の数字です。アカウントとは違います。

戻り値
TotaScore	int
eventranking	struct

	Rank	int		リスナーの順位
	Point	int		リスナーの貢献ポイント
	Listner string	リスナーの名前

status		int
*/
func GetPointsContByApi(
	client *http.Client,
	ieventid int,
	roomid int,
) (
	eventranking ShowroomDBlib.EventRanking,
	uidmap map[int]int,
	err error,
) {

	pranking, err := srapi.ApiEventContribution_ranking(client, ieventid, roomid)
	if err != nil {
		err = fmt.Errorf("ApiEventContribution_ranking() failed. %v", err)
		return
	}

	if len(pranking.Ranking) == 0 {
		err = fmt.Errorf("ApiEventContribution_ranking() returned empty ranking")
		return
	}

	uidmap = make(map[int]int)
	for i, r := range pranking.Ranking {
		er := ShowroomDBlib.EventRank{
			Order:   i + 1,
			Rank:    r.Rank,
			Listner: r.Name,
			Point:   r.Point,
			LsnID:   r.UserID,
		}
		eventranking = append(eventranking, er)

		uidmap[r.UserID] = i
	}

	return
}

/*
GetPointsCont()
イベントページのURLと配信者さんのIDから、イベント貢献ランキングのリストを取得します。

引数
EvnetName	string	イベント名、下記イベントページURLの"event_id"の部分

	https://www.showroom-live.com/event/event_id

ID_Account	string	配信者さんのID

	SHOWROOMへの登録順を示すと思われる6桁(以下)の数字です。アカウントとは違います。

戻り値
TotaScore	int
eventranking	struct

	Rank	int		リスナーの順位
	Point	int		リスナーの貢献ポイント
	Listner string	リスナーの名前

status		int

***
リスナーさんの日々のあるいは配信ごとの貢献ポイントの推移がすぐにわかれば配信者さんもいろいろ手の打ちよう(?)が
ありそうですが、「リスナーの名前」というのはリスナーさんが自由に設定・変更できるので貢献ポイントを追いかけて
行くのはけっこうたいへんです。
このプログラムではLevenshtein距離による類似度のチェックや貢献ランキングの特性を使ってニックネームの変更を追尾しています。
リスナーさんのuseridがわかればいいのですが、いろいろと面倒なところがあります。

「レーベンシュタイン距離 - Wikipedia」
https://ja.wikipedia.org/wiki/%E3%83%AC%E3%83%BC%E3%83%99%E3%83%B3%E3%82%B7%E3%83%A5%E3%82%BF%E3%82%A4%E3%83%B3%E8%B7%9D%E9%9B%A2

「カスタマイズしやすい重み付きレーベンシュタイン距離ライブラリをGoで書きました - サルノオボエガキ」
https://deltam.blogspot.com/2018/10/go.html

貢献ポイントをこまめに記録しておくと、減算ポイントが発生したときの原因アカウントの特定に使えないこともないです。
（実際やってみるとわかるのですが、これはこれでなかなかたいへんです）
なお、原因アカウントの特定、というのは犯人探しというような意味で言ってるわけじゃありませんので念のため。
*/
func GetPointsCont(EventName, ID_Account string) (
	TotalScore int,
	eventranking ShowroomDBlib.EventRanking,
	status int,
) {

	status = 0

	//	貢献ランキングのページを開き、データ取得の準備をします。
	//	_url := "https://www.showroom-live.com/event/contribution/" + EventName + "?room_id=" + ID_Account
	ename := EventName
	ename_a := strings.Split(EventName, "?")
	if len(ename_a) == 2 {
		ename = ename_a[0]
	}
	_url := "https://www.showroom-live.com/event/contribution/" + ename + "?room_id=" + ID_Account

	resp, error := http.Get(_url)
	if error != nil {
		log.Printf("GetEventInfAndRoomList() http.Get() err=%s\n", error.Error())
		status = 1
		return
	}
	defer resp.Body.Close()

	var doc *goquery.Document
	doc, error = goquery.NewDocumentFromReader(resp.Body)
	if error != nil {
		log.Printf("GetEventInfAndRoomList() goquery.NewDocumentFromReader() err=<%s>.\n", error.Error())
		status = 1
		return
	}

	/*
		u := url.URL{}
		u.Scheme = doc.Url.Scheme
		u.Host = doc.Url.Host
	*/

	//	各リスナーの情報を取得します。
	//	var selector_ranking, selector_listner, selector_point, ranking, listner, point string
	var ranking, listner, point string
	var iranking, ipoint int
	var eventrank ShowroomDBlib.EventRank

	TotalScore = 0

	//	eventranking = make([]EventRank)

	doc.Find(".table-type-01:nth-child(2) > tbody > tr").Each(func(i int, s *goquery.Selection) {
		if i != 0 {

			//	データを一つ取得するたびに(戻り値となる)リスナー数をカウントアップします。
			//	NoListner++

			//	以下セレクターはブラウザの開発ツールを使って確認したものです。

			//	順位を取得し、文字列から数値に変換します。
			//	selector_ranking = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 1)
			ranking = s.Find("td:nth-child(1)").Text()

			/*
				//	データがなくなったらbreakします。このときのNoListnerは通常100、場合によってはそれ以下です。
				if ranking == "" {
					break
				}
			*/

			iranking, _ = strconv.Atoi(ranking)

			//	リスナー名を取得します。
			//	selector_listner = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 2)
			listner = s.Find("td:nth-child(2)").Text()

			//	貢献ポイントを取得し、文字列から"pt"の部分を除いた上で数値に変換します。
			//	selector_point = fmt.Sprintf("table.table-type-01:nth-child(2) > tbody:nth-child(2) > tr:nth-child(%d) > td:nth-child(%d)", NoListner+2, 3)
			point = s.Find("td:nth-child(3)").Text()
			point = strings.Replace(point, "pt", "", -1)
			ipoint, _ = strconv.Atoi(point)
			TotalScore += ipoint

			//	戻り値となるスライスに取得したデータを追加します。
			eventrank.Rank = iranking
			eventrank.Point = ipoint
			eventrank.Listner = listner
			eventrank.Order = i
			eventranking = append(eventranking, eventrank)
		}
	})

	return
}

func MakeListInSheet(
	oldfilename,
	newfilename string,
	eventranking ShowroomDBlib.EventRanking,
	ncolw int,
	totalscore,
	totalincremental int,
) (
	status int,
) {

	status = 0

	no := len(eventranking)

	// Excelファイルをオープンする。
	//	fxlsx, err := excelize.OpenFile(EventID + ".xlsx")
	//	filename := event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	//	filename = "_tmp.xlsx"
	log.Printf(" inputfilename=<%s>\n", oldfilename)
	log.Printf(" outputfilename=<%s>\n", newfilename)
	fxlsx, err := excelize.OpenFile(oldfilename)
	if err != nil {
		log.Printf("<%v>\n", err)
		status = -1
		return
	}

	sheet1 := "Sheet1"
	sheet2 := "Sheet2"

	scolnew := CtoA(ncolw)
	//	scollast := CtoA(ncolw - 1)

	t19000101 := time.Date(1899, 12, 30, 0, 0, 0, 0, time.Local)
	tnow := time.Now()

	fxlsx.SetCellValue(sheet1, scolnew+"1", totalscore)
	fxlsx.SetCellValue(sheet2, scolnew+"1", totalincremental)

	//	fxlsx.SetCellValue(sheet, scolnew+"2", tnow)

	tserial := tnow.Sub(t19000101).Minutes() / 60.0 / 24.0
	fxlsx.SetCellValue(sheet1, scolnew+"3", tserial)
	fxlsx.SetCellValue(sheet2, scolnew+"3", tserial)

	fxlsx.SetCellValue(sheet1, scolnew+"4", tnow.Format("01/02 15:04"))
	fxlsx.SetCellValue(sheet2, scolnew+"4", tnow.Format("01/02 15:04"))

	for i := 0; i < no; i++ {
		loci := eventranking[i].Order
		srow := fmt.Sprintf("%d", loci+5)

		fxlsx.SetCellValue(sheet1, "A"+srow, eventranking[i].Rank)
		fxlsx.SetCellValue(sheet2, "A"+srow, eventranking[i].Rank)

		fxlsx.SetCellValue(sheet1, "C"+srow, eventranking[i].Listner)
		fxlsx.SetCellValue(sheet2, "C"+srow, eventranking[i].Listner)

		fxlsx.SetCellValue(sheet1, scolnew+srow, eventranking[i].Point)
		if eventranking[i].Incremental != -1 {
			fxlsx.SetCellValue(sheet2, scolnew+srow, eventranking[i].Incremental)
		} else {
			fxlsx.SetCellValue(sheet2, scolnew+srow, "n/a")
		}

		if eventranking[i].Lastname != "" {
			fxlsx.SetCellValue(sheet1, "B"+srow, eventranking[i].Lastname)
			/*
				//	Excelファイルの肥大化はこれが原因かも。あくまで"かも"。
				fxlsx.AddComment(sheet1, scollast+srow, `{"author":"Chouette: ","text":"`+eventranking[i].Lastname+`"}`)
			*/
		} else {
			fxlsx.SetCellValue(sheet1, "B"+srow, nil)
		}
	}

	//	serial++
	//	filename = event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	//	Printf(" filename(out) = <%s>\n", filename)
	err = fxlsx.SaveAs(newfilename)

	if err != nil {
		log.Printf(" error in SaveAs() <%s>\n", err)
		status = -1
	}

	return
}

func CtoA(col int) (acol string) {
	acol = string(rune('A') + int32((col-1)%26))
	if int((col-1)/26) > 0 {
		acol = string(rune('A')+(int32((col-1)/26))-1) + acol
	}
	return
}
func CRtoA1(col, row int) (a1 string) {
	a1 = CtoA(col) + fmt.Sprintf("%d", row)
	return
}

func CopyFile(inputfile, outputfile string) (status int) {

	status = 0

	// read the whole file at once
	b, err := os.ReadFile(inputfile)
	if err != nil {
		//	panic(err)
		log.Printf("error <%v>\n", err)
		status = -1
		return
	}

	// write the whole body at once
	err = os.WriteFile(outputfile, b, 0644)
	if err != nil {
		//	panic(err)
		log.Printf("error <%v>\n", err)
		status = -2
	}
	return

}

func ReadListInSheet(
	oldfilename string,
) (
	eventranking ShowroomDBlib.EventRanking,
	ncolw int,
	status int,
) {

	status = 0

	// Excelファイルをオープンする。
	//	fxlsx, err := excelize.OpenFile(EventID + ".xlsx")
	//	filename := event_id + "_" + room_id + "_" + fmt.Sprintf("%05d", serial) + ".xlsx"
	log.Printf(" inputfilename=<%s>\n", oldfilename)
	//	filename = "_tmp.xlsx"
	fxlsx, err := excelize.OpenFile(oldfilename)
	if err != nil {
		log.Printf("<%v>\n", err)
		status = -1
		return
	}

	sheet := "Sheet1"

	for i := 4; ; i++ {
		//	value, _ := fxlsx.GetCellValue(sheet, CRtoA1(i, 4))
		value := fxlsx.GetCellValue(sheet, CRtoA1(i, 4))
		if value == "" {
			ncolw = i
			if ncolw == 4 {
				return
			}
			break
		}
	}

	var eventrank ShowroomDBlib.EventRank
	//	eventranking = make([]EventRank)

	scol := CtoA(ncolw - 1)
	for i := 0; i < 200; i++ {
		srow := fmt.Sprintf("%d", i+5)
		//	listner, _ := fxlsx.GetCellValue(sheet, "C"+srow)
		//	spoint, _ := fxlsx.GetCellValue(sheet, scol+srow)
		listner := fxlsx.GetCellValue(sheet, "C"+srow)
		spoint := fxlsx.GetCellValue(sheet, scol+srow)
		if listner == "" && spoint == "" {
			log.Println("*** break *** i=", i)
			break
		}

		eventrank.Order = i

		eventrank.Listner = listner

		eventrank.Point, _ = strconv.Atoi(spoint)

		eventranking = append(eventranking, eventrank)
	}

	sort.Sort(eventranking)

	return
}

func CompareEventRankingByApi(
	last_eventranking ShowroomDBlib.EventRanking,
	new_eventranking ShowroomDBlib.EventRanking,
	uidmap map[int]int,
) (
	final_eventranking ShowroomDBlib.EventRanking,
	totalincremental int,
) {

	//	for j := 0; j < len(last_eventranking); j++ {
	for j, ler := range last_eventranking {
		if idx, ok := uidmap[ler.LsnID]; ok {
			if ler.Point != -1 {
				incremental := new_eventranking[idx].Point - ler.Point
				totalincremental += incremental
				last_eventranking[j].Incremental = incremental
			} else {
				last_eventranking[j].Incremental = -1
			}
			last_eventranking[j].Rank = new_eventranking[idx].Rank
			last_eventranking[j].Point = new_eventranking[idx].Point
			last_eventranking[j].Order = new_eventranking[idx].Order
			if new_eventranking[idx].Listner == ler.Listner {
				last_eventranking[j].Lastname = ""
			} else {
				last_eventranking[j].Listner = new_eventranking[idx].Listner
				last_eventranking[j].Lastname = ler.Listner
			}
			new_eventranking[idx].Status = 1
		} else {
			//	同一のuseridのデータがみつからなかった。
			last_eventranking[j].Point = -1
			last_eventranking[j].Incremental = -1
			last_eventranking[j].Status = -1
			last_eventranking[j].Order = 999
			last_eventranking[j].Lastname = ""
			log.Printf("*****         【%s】  not found.\n", last_eventranking[j].Listner)

		}
	}
	//	既存のランキングになかった新規のリスナーを既存のランキングに追加する。
	//	ソートはしない。ソートするとExcelにあるデータと整合性がとれなくなる。
	//	つまり、ソートはExcelで行う。
	var eventrank ShowroomDBlib.EventRank
	no := len(last_eventranking)
	for _, ner := range new_eventranking {

		if ner.Status != 1 {
			eventrank.Order = no
			no++
			eventrank.Listner = ner.Listner
			eventrank.Rank = ner.Rank
			eventrank.Point = ner.Point
			eventrank.Order = ner.Order
			//	eventrank.T_LsnID = ner.Order + idx*1000
			eventrank.T_LsnID = ner.LsnID
			eventrank.LsnID = ner.LsnID
			eventrank.Incremental = -1

			incremental := ner.Point
			totalincremental += incremental
			eventrank.Incremental = incremental

			last_eventranking = append(last_eventranking, eventrank)
		}
	}

	final_eventranking = last_eventranking

	return
}

func CompareEventRanking(
	last_eventranking ShowroomDBlib.EventRanking,
	new_eventranking ShowroomDBlib.EventRanking,
	idx int,
) (ShowroomDBlib.EventRanking, int) {

	totalincremental := 0

	log.Printf("          Phase 1\n")
	//	既存のデータとリスナー名が一致するデータがあったときは既存のデータを更新する。
	ncol := 1
	msg := ""
	for j := 0; j < len(last_eventranking); j++ {
		for i := 0; i < len(new_eventranking); i++ {
			if new_eventranking[i].Status == 1 {
				continue
			}
			if new_eventranking[i].Listner == last_eventranking[j].Listner {
				if new_eventranking[i].Point >= last_eventranking[j].Point {
					if last_eventranking[j].Point != -1 {
						incremental := new_eventranking[i].Point - last_eventranking[j].Point
						totalincremental += incremental
						last_eventranking[j].Incremental = incremental
					} else {
						last_eventranking[j].Incremental = -1
					}
					if new_eventranking[i].LsnID != 0 {
						last_eventranking[j].LsnID = new_eventranking[i].LsnID
					}
					last_eventranking[j].Rank = new_eventranking[i].Rank
					last_eventranking[j].Point = new_eventranking[i].Point
					last_eventranking[j].Order = new_eventranking[i].Order
					last_eventranking[j].Lastname = ""
					new_eventranking[i].Status = 1
					last_eventranking[j].Status = 1
					msg = msg + fmt.Sprintf("%3d/%3d  ", j, i)
					if ncol == 10 {
						log.Printf("%s\n", msg)
						ncol = 1
						msg = ""
					} else {
						ncol++
					}
					break
				}
			}
		}
	}
	if msg != "" {
		log.Printf("%s\n", msg)
	}

	log.Printf("          Phase 2\n")

	phase2 := func() {
		log.Printf("     vvvvv     Phase 2\n")

		//	現在のポイント以上のリスナーが一人しかいないなら同一人物のはず
	Outerloop:
		for j := 0; j < len(last_eventranking); j++ {
			if last_eventranking[j].Status == 1 {
				continue
			}
			noasgn := -1
			for i := 0; i < len(new_eventranking); i++ {
				if new_eventranking[i].Status == 1 {
					//	すでに突き合わせが終わったものは対象にしない。
					continue
				}
				if new_eventranking[i].Point < 0 {
					//	いったんランクキング表外に出たものは突き合わせの対象としない。
					continue
				}
				if new_eventranking[i].Point < last_eventranking[j].Point {
					break
				}

				if noasgn != -1 {
					//	現在のポイント以上のリスナーが複数人いるとき
					//	ここで処理を完全やめてしまうのは last_eventranking がソートしてあることが前提
					//	ソートされていないのであれば単なるbreakにすべき
					break Outerloop
				} else {
					//	現在のポイント以上のはじめてのリスナー
					noasgn = i
				}
			}
			if noasgn != -1 {
				//	現在のポイント以上のリスナーが一人しかいなかった
				if last_eventranking[j].Point != -1 {
					incremental := new_eventranking[noasgn].Point - last_eventranking[j].Point
					totalincremental += incremental
					last_eventranking[j].Incremental = incremental
				} else {
					last_eventranking[j].Incremental = -1
				}
				if new_eventranking[noasgn].LsnID != 0 {
					last_eventranking[j].LsnID = new_eventranking[noasgn].LsnID
				}
				last_eventranking[j].Rank = new_eventranking[noasgn].Rank
				last_eventranking[j].Point = new_eventranking[noasgn].Point
				last_eventranking[j].Order = new_eventranking[noasgn].Order
				new_eventranking[noasgn].Status = 1
				last_eventranking[j].Status = 1
				last_eventranking[j].Lastname = last_eventranking[j].Listner + " [2]"
				last_eventranking[j].Listner = new_eventranking[noasgn].Listner
				log.Printf("*****         【%s】 equals to 【%s】\n", new_eventranking[noasgn].Listner, last_eventranking[j].Lastname)
			}

		}
		log.Printf("     ^^^^^     Phase 2\n")
	}
	//	コメントにした理由を思い出す！
	//	phase2()

	log.Printf("          Phase 3\n")
	//	完全に一致するものがない場合は一致度が高いものを探す。
	// weighted
	wd := lsdp.Weights{Insert: 0.8, Delete: 0.8, Replace: 1.0}
	// weighted and normalized
	nd := lsdp.Normalized(wd)
	for j := 0; j < len(last_eventranking); j++ {
		if last_eventranking[j].Status == 1 {
			continue
		}
		log.Println("---------------")
		first_n := 0
		first_v := 2.0
		second_v := 2.0
		for i := 0; i < len(new_eventranking); i++ {
			if new_eventranking[i].Status == 1 {
				continue
			}
			if new_eventranking[i].Point < last_eventranking[j].Point {
				break
			}

			newlistner := new_eventranking[i].Listner
			lastlistner := last_eventranking[j].Listner
			value := nd.Distance(newlistner, lastlistner)
			log.Printf("%6.3f [%3d] 【%s】 [%3d] 【%s】\n", value, j, lastlistner, i, newlistner)
			if value < first_v {
				second_v = first_v
				first_v = value
				first_n = i
			} else if value < second_v {
				second_v = value
			}
		}

		phase3 := func(cond string, dist float64) {
			if last_eventranking[j].Point != -1 {
				incremental := new_eventranking[first_n].Point - last_eventranking[j].Point
				totalincremental += incremental
				last_eventranking[j].Incremental = incremental
			} else {
				last_eventranking[j].Incremental = -1
			}
			if new_eventranking[first_n].LsnID != 0 {
				last_eventranking[j].LsnID = new_eventranking[first_n].LsnID
			}
			last_eventranking[j].Rank = new_eventranking[first_n].Rank
			last_eventranking[j].Point = new_eventranking[first_n].Point
			last_eventranking[j].Order = new_eventranking[first_n].Order
			new_eventranking[first_n].Status = 1
			last_eventranking[j].Status = 1
			last_eventranking[j].Lastname = last_eventranking[j].Listner + " [" + cond + fmt.Sprintf("%6.3f", dist) + "]"
			last_eventranking[j].Listner = new_eventranking[first_n].Listner
			log.Printf("*****         【%s】 equals to 【%s】\n", last_eventranking[j].Lastname, new_eventranking[first_n].Listner)
		}

		switch {
		//	case first_v < 0.72:	//	この数値は大きすぎると思われる。0.6を超えて一致と判断されるものはあやしいものが多かった（2022-03-23)
		case first_v < 0.62:
			//	一致度が高い
			phase3("3A", first_v)
		case second_v < 1.1 && second_v-first_v > 0.2:
			//	一致度が他に比較して高い
			phase3("3B", first_v)
		case first_v < 1.1 && second_v > 1.1 &&
			last_eventranking[j].Point != -1 &&
			(j == len(last_eventranking)-1 || last_eventranking[j].Point != last_eventranking[j+1].Point):
			//	一致度のチェック対象が一つしかない
			//	ここで last_eventranking[j].Point != last_eventranking[j+1].Point の条件が成り立たないことはありえないはずだが...
			phase3("3C", first_v)
		default:
			//	同一と思われるデータがみつからなかった。
			last_eventranking[j].Point = -1
			last_eventranking[j].Incremental = -1
			last_eventranking[j].Status = -1
			last_eventranking[j].Order = 999
			last_eventranking[j].Lastname = ""
			log.Printf("*****         【%s】  not found.\n", last_eventranking[j].Listner)
		}

	}

	phase2()

	log.Printf("          Phase 4\n")

	//	既存のランキングになかったリスナーを既存のランキングに追加する。
	//	ソートはしない。ソートするとExcelにあるデータと整合性がとれなくなる。
	//	つまり、ソートはExcelで行う。
	var eventrank ShowroomDBlib.EventRank
	no := len(last_eventranking)
	for i := 0; i < len(new_eventranking); i++ {
		if new_eventranking[i].Status != 1 {
			eventrank.Order = no
			no++
			eventrank.Listner = new_eventranking[i].Listner
			eventrank.Rank = new_eventranking[i].Rank
			eventrank.Point = new_eventranking[i].Point
			eventrank.Order = new_eventranking[i].Order
			//	eventrank.T_LsnID = new_eventranking[i].Order + idx*1000
			eventrank.T_LsnID = new_eventranking[i].LsnID
			if new_eventranking[i].LsnID != 0 {
				eventrank.LsnID = new_eventranking[i].LsnID
			}

			eventrank.Incremental = -1

			incremental := new_eventranking[i].Point
			totalincremental += incremental
			eventrank.Incremental = incremental

			last_eventranking = append(last_eventranking, eventrank)
		}
	}

	return last_eventranking, totalincremental
}

func ExtractTask(
	sm *AppShutdownManager,
	environment *Environment,
	/*
		bmakesheet bool,
	*/
) (
	status int,
) {

	//	var hh, mm, hhn, mmn int
	var hhn, mmn int
	//	var event_id, room_id string
	//	var bmakesheet bool

	sm.Wg.Done()

	bmakesheet := true

	log.Printf("%s ***************** ExtractTaskGroup() ****************\n", time.Now().Format("2006/1/2 15:04:05"))
	defer log.Printf("%s ************* end of ExtractTaskGroup() *************\n", time.Now().Format("2006/1/2 15:04:05"))

	client, cookiejar, err := exsrapi.CreateNewClient("")
	if err != nil {
		log.Printf("exsrapi.CeateNewClient(): %s", err.Error())
		return //	エラーがあれば、ここで終了
	}
	defer cookiejar.Save()

	hhn = 99
	mmn = 99

	st := time.Now()
	log.Printf(" Start of ExtractTaskGroup() at %s\n", st.Format("2006/1/2 15:04:05"))

Outerloop:
	for {
		select {
		case <-sm.Ctx.Done():
			log.Println("Outer loop: Context cancelled, exiting.")
			return
		default:
			// Contextはまだ有効
		}

		//	毎分繰り返す。
		for {
			select {
			case <-sm.Ctx.Done():
				log.Println("Inner loop: Context cancelled, exiting.")
				return
			default:
				// Contextはまだ有効
			}

			//	リスナー別貢献ポイントの算出が必要な配信枠がなくなるまで繰り返す
			ndata, event_id, userno, sampletm1 := ShowroomDBlib.SelectEidUidFromTimetable()
			if ndata <= 0 {
				break
			}

			//	本来のイベントID（数字の方）を求めるためにイベント情報を抽出する。
			peventinf, err := ShowroomDBlib.SelectFromEvent(event_id)
			if err != nil {
				log.Printf("SelectFromEvent(): %s", err.Error())
				break Outerloop
			}

			room_id := fmt.Sprintf("%d", userno)

			log.Printf(" ndata = %d event_id [%s]  userno =%d.\n", ndata, event_id, userno)

			//	直近の配信枠後の貢献ポイントランキングを取得する。
			log.Printf("------------------- new_eventranking --------------------\n")
			//	totalscore, new_eventranking, _ := GetPointsCont(ieventid, room_id)
			var new_eventranking ShowroomDBlib.EventRanking
			uidmap := make(map[int]int)
			if UseApi {
				//	APIを利用してリスナー別の貢献ポイントを取得する。
				//	この方法ではリスナーの識別子を取得できる。
				new_eventranking, uidmap, err = GetPointsContByApi(client, peventinf.I_Event_ID, userno)
				if err != nil {
					log.Printf("GetPointsContByApi(): %s", err.Error())
					if strings.Contains(err.Error(), "returned empty ranking") {
						//	貢献ポイントが取得できず、原因が"returned empty ranking"の場合
						//	データ取得対象のルームが配信後貢献ポイントデータを取得する前にイベントへの参加を取り消したとき発生する
						//	次回以後貢献ポイント取得を実行しないようにするためにはtimetableのstatusを2にセットすること。
						updsql := "UPDATE timetable SET status = 2 WHERE eventid = ? and userid = ? and sampletm1 = ? "
						_, err := srdblib.Dbmap.Exec(updsql, event_id, userno, sampletm1)
						if err != nil {
							err = fmt.Errorf("Dbmap.Exec(): %s", err.Error())
							log.Printf("ExtractTask() err=%s\n", err.Error())
							return
						}

						continue
					}
					// break Outerloop
					log.Printf(" ==== Wait 10 seconds and retry ==== \n")
					// time.Sleep(10 * time.Second)
					select {
					case <-sm.Ctx.Done():
						log.Println("Wait cancelled by context.")
						// キャンセルされた場合の処理 (例: ループを抜ける)
						return
					case <-time.After(10 * time.Second):
						log.Println("Wait finished.")
						// 待機が完了した場合の処理
						continue
					}
				}
			} else {
				//	APIを使わずクロールでリスナー別の貢献ポイントを取得する。
				//	APIが実装される前の方法で、リスナー識別子は取得できずリスナー名を突き合わせてリスナーを特定する必要がある。
				_, new_eventranking, _ = GetPointsCont(event_id, room_id)
			}

			/*
				for i := 0; i < len(new_eventranking); i++ {
					log.Printf("%3d\t%7d\t【%s】\r\n", new_eventranking[i].rank, new_eventranking[i].point, new_eventranking[i].listner)
				}
				log.Printf("Total Score=%d\n", totalscore)
			*/

			log.Printf("------------------- last_eventranking --------------------\n")

			last_eventranking := make(ShowroomDBlib.EventRanking, 0)

			ndata, maxts := ShowroomDBlib.SelectMaxTsFromEventrank(event_id, userno)
			if ndata < 0 {
				//	データが取得できない。データベースアクセスエラー。
				log.Printf(" %d returned by SelectMaxTsFromEventrank()\n", ndata)
				break Outerloop
			} else if ndata > 0 {
				//	前回配信枠のデータが存在するので、それをDBから抽出する。
				last_eventranking, status = ShowroomDBlib.SelectEventRankingFromEventrank(event_id, userno, maxts)
			}
			/*	*/
			for i := 0; i < len(last_eventranking); i++ {
				log.Printf("%3d\t%7d\t【%s】\r\n", last_eventranking[i].Order, last_eventranking[i].Point, last_eventranking[i].Listner)
			}
			/*	*/

			//	ランキングを比較する。
			var final_eventranking ShowroomDBlib.EventRanking
			var totalincremental int
			log.Printf("------------------- compare --------------------\n")
			idx := ShowroomDBlib.SelectMaxTlsnidFromEventranking(event_id, userno) / 1000
			if idx >= 1000 {
				idx /= 1000
			}
			idx += 1
			if len(last_eventranking) == 0 || new_eventranking[0].LsnID != 0 && last_eventranking[0].LsnID != 0 { //	LsnIDを使わずに判別する方法は？
				//	新旧データともにAPIで取得したランキングである。
				final_eventranking, totalincremental = CompareEventRankingByApi(last_eventranking, new_eventranking, uidmap)
			} else {
				//	旧データがAPIで取得したデータではない。
				final_eventranking, totalincremental = CompareEventRanking(last_eventranking, new_eventranking, idx)
			}
			log.Printf("------------------- final_eventranking --------------------\n")
			for i := 0; i < len(final_eventranking); i++ {
				if final_eventranking[i].Lastname != "" {
					log.Printf("%3d%9d%9d%10d\t【%s】\t【%s】\r\n",
						final_eventranking[i].Order,
						final_eventranking[i].T_LsnID,
						final_eventranking[i].LsnID,
						final_eventranking[i].Point,
						final_eventranking[i].Listner,
						final_eventranking[i].Lastname)
				} else {
					log.Printf("%3d%9d%9d%10d\t【%s】\r\n",
						final_eventranking[i].Order,
						final_eventranking[i].T_LsnID,
						final_eventranking[i].LsnID,
						final_eventranking[i].Point,
						final_eventranking[i].Listner)
				}
			}

			if bmakesheet {

				sampletm2 := time.Now().Truncate(time.Minute)
				ier_status := ShowroomDBlib.InsertIntoEventrank(event_id, userno, sampletm2, final_eventranking)
				if ier_status != 0 {
					log.Printf(" Can`t insert into eventrank.\n")
				}
				ShowroomDBlib.UpdateTimetable(event_id, userno, sampletm1, sampletm2, totalincremental)

			} else {
				for i := 1; i < 100; i++ {
					log.Printf(" (%d) %s", i, CtoA(i))
				}
				log.Printf(".\n")
			}

		}
		// hhn, mmn, _ = WaitNextMinute()
		//	現在時（時分秒.....）
		t0 := time.Now()
		//	現在時（時分）
		t0tm := t0.Truncate(1 * time.Minute)
		//	次の時分（現在時が11時12分10秒であれば、11時13分00秒）
		t0tm = t0tm.Add(1 * time.Minute)
		//	次の時分までウェイトします。
		dt := t0tm.Sub(t0)
		// time.Sleep(dt + 100*time.Millisecond)
		select {
		case <-sm.Ctx.Done():
			log.Println("Wait cancelled by context.")
			// キャンセルされた場合の処理 (例: ループを抜ける)
			return
		case <-time.After(dt + 100*time.Millisecond):
			// log.Println("Wait finished.")
			// 待機が完了した場合の処理
		}
		//	現在時を戻り値にセットします。
		hhn, mmn, _ = time.Now().Clock()

		//	log.Printf("** %02d %02d\n", hhn, mmn)

		if (hhn+1)%environment.IntervalHour == 0 && mmn == 0 {
			log.Printf(" End of ExtractTaskGroup() t=%s\n", time.Now().Format("2006/1/2 15:04:05"))
			break
		}

	}

	status = 0

	return
}

/*
WaitNextMinute()
現在時の時分の次の時分までウェイトします。
現在時が11時12分10秒であれば、11時13分00秒までウェイトします。

引数
なし

戻り値
hhn		int		ウェイト終了後の時刻の時
mmn		int		ウェイト終了後の時刻の分
ssn		int		ウェイト終了後の時刻の秒
*/
func WaitNextMinute() (hhn, mmn, ssn int) {

	//	現在時（時分秒.....）
	t0 := time.Now()

	//	現在時（時分）
	t0tm := t0.Truncate(1 * time.Minute)

	//	次の時分（現在時が11時12分10秒であれば、11時13分00秒）
	t0tm = t0tm.Add(1 * time.Minute)

	//	次の時分までウェイトします。
	dt := t0tm.Sub(t0)
	time.Sleep(dt + 100*time.Millisecond)

	//	現在時を戻り値にセットします。
	hhn, mmn, ssn = time.Now().Clock()

	return
}

// シャットダウンに関連する状態をまとめた構造体
type AppShutdownManager struct {
	Ctx    context.Context
	Cancel context.CancelFunc // トップレベルでのみ使うことが多いが、構造体に含めることも可能
	Wg     *sync.WaitGroup
	// 他のリソース（DB接続など）を含めることも可能
	// DB *gorp.DbMap // 例
}

// CloseResources はシャットダウン処理とリソース解放を行います。
// defer で呼び出されることを想定しています。
func (sm *AppShutdownManager) CloseResources() {
	log.Println("Closing resources...")

	// コンテキストをキャンセルし、新しいgoroutineの起動を停止
	// シグナル受信などで既に呼ばれている可能性もあるが、冪等なので問題ない
	sm.Cancel()
	log.Println("Context cancelled.")

	// WaitGroupの完了を待つのは、通常main関数で行います。
	// ここでWaitすると、CloseResourcesがブロックされてしまい、
	// main関数がWaitする前にリソース解放が完了しない可能性があります。
	// そのため、Waitはmain関数に任せるのが一般的です。

	// 他のリソース解放処理
	// if sm.DB != nil {
	//      sm.DB.Db.Close() // gorpのDB接続をクローズ
	//      log.Println("Database connection closed.")
	// }
	// 他のリソース解放処理
	log.Println("Resources closed.")
}

// -------------------------------------------

func main() {

	logfilename := "GetPointsCont01" + "_" + version + "_" + ShowroomDBlib.Version + "_" + time.Now().Format("20060102") + ".txt"
	logfile, err := os.OpenFile(logfilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		panic("cannnot open logfile: " + logfilename + err.Error())
	}
	defer logfile.Close()
	log.SetOutput(logfile)
	//	log.SetOutput(io.MultiWriter(logfile, os.Stdout))

	log.Printf("\n")
	log.Printf("\n")
	log.Printf("************************ GetPointsCont01 Ver.%s *********************\n", version+"_"+ShowroomDBlib.Version)

	/*
		if len(os.Args) > 1 {
			fmt.Println("Usage: ", os.Args[0])
			log.Println("Usage: ", os.Args[0])
			log.Printf("%v\n", os.Args)
			return
		}
	*/

	dbconfig, err := ShowroomDBlib.LoadConfig("ServerConfig.yml")
	if err != nil {
		log.Printf("ShowroomDBlib.LoadConfig() Error: %s\n", err.Error())
		return
	}
	// log.Printf("dbconfig: %+v\n", dbconfig)

	var environment Environment

	exerr := exsrapi.LoadConfig("Environment.yml", &environment)
	if exerr != nil {
		log.Printf("exsrapi/LoadConfig() Error: %s\n", exerr.Error())
		log.Printf("Set IntervalMin to 99999.\n")
		environment.IntervalHour = 99999
	}
	log.Printf(" environment=%+v\n", environment)

	status := ShowroomDBlib.OpenDb(dbconfig)
	if status != 0 {
		log.Printf("ShowroomDBlib.OpenDB returned status = %d\n", status)
		return
	}
	defer ShowroomDBlib.Db.Close()

	dial := gorp.MySQLDialect{Engine: "InnoDB", Encoding: "utf8mb4"}
	srdblib.Dbmap = &gorp.DbMap{Db: ShowroomDBlib.Db, Dialect: dial, ExpandSliceArgs: true}
	srdblib.Dbmap.AddTableWithName(srdblib.Timetable{}, "timetable").SetKeys(false, "Eventid", "Userid", "Sampletm1")

	// -------------------------------------

	// 1. シグナル通知用のチャネルを作成
	// バッファリングされたチャネルにすることで、シグナル受信と処理の間に少し余裕を持たせます。
	sigCh := make(chan os.Signal, 1)
	// SIGINT (Ctrl+C) と SIGTERM を補足するように設定
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// 2. 新しいgoroutineの起動を制御するためのコンテキスト
	// context.WithCancel() でキャンセル可能なコンテキストを作成します。
	ctx, cancel := context.WithCancel(context.Background())
	// main関数が終了する際に確実にcancel()が呼ばれるようにdeferで設定
	// (シグナル受信時にもcancel()を呼びますが、二重呼び出しは問題ありません)
	// defer cancel() // (sm *AppShutdownManager) CloseResources()で呼び出されるので、ここでは不要)

	// 3. 実行中のgoroutineを追跡するための WaitGroup
	var wg sync.WaitGroup

	// ShutdownManager インスタンスを作成
	// DB接続などの初期化もここで行う
	sm := &AppShutdownManager{
		Ctx:    ctx,
		Cancel: cancel,
		Wg:     &wg,
		// DB: initDB(), // 例
	}
	// main関数が終了する際に、リソース解放処理を確実に実行
	// これにより、シグナル受信、エラー終了、正常終了のいずれの場合でも呼ばれる
	defer sm.CloseResources()

	// -------------------------------------

	sm.Wg.Add(1)
	go ExtractTask(sm, &environment)

	// シグナル受信を待つ
	<-sigCh
	log.Println("\nシグナルを受信しました。")

	// シグナル受信をトリガーとして、ShutdownManager経由でキャンセルを呼び出す
	// これにより、Contextを監視しているgoroutineが終了を開始する
	// deferされたCloseResourcesでもCancelは呼ばれるが、シグナル受信時に
	// 即座にキャンセルをトリガーしたい場合はここで明示的に呼ぶ
	// (CloseResources内でCancelを呼ぶ設計の場合は、ここでの明示的な呼び出しは不要)
	// 今回はCloseResources内でCancelを呼ぶ設計なので、ここはコメントアウト
	// sm.Cancel()
	log.Println("シャットダウン処理を開始します。")

	// ShutdownManager経由でWaitを呼び、実行中のすべてのgoroutineが終了するのを待つ
	// Contextがキャンセルされた後、すべてのワーカーgoroutineがDone()を呼ぶのを待つ
	log.Println("実行中のすべてのgoroutineが終了するのを待っています...")
	sm.Wg.Wait()

	// Wait()から戻ったら、すべてのgoroutineが終了したことになります。
	// main関数が終了するため、defer sm.CloseResources() が呼ばれ、
	// リソース解放処理が行われます。
	log.Println("すべてのgoroutineが終了しました。")
	// main関数が終了すると、deferが実行され、プログラムが終了します。

}
