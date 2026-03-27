package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var failureLogMutex sync.Mutex // 用於確保並發寫入日誌檔案時的安全

type StockData struct {
	StockNo   string
	Documents []interface{}
}

func main() {
	url := launcher.New().
		Headless(false).
		MustLaunch()

	browser := rod.New().ControlURL(url).MustConnect()
	defer browser.MustClose()

	// 設定 MongoDB 連線
	// 請確保你的 MongoDB 正在 localhost:27017 執行，或修改為你的連線字串
	// ctx := context.TODO()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017")
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		panic(fmt.Errorf("MongoDB 連線失敗: %v", err))
	}
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()
	// 指定資料庫 twse 和集合 stock_prices
	collection := client.Database("twse").Collection("stock_prices")

	// 從外部檔案讀取股票代號列表
	stockList, err := readCSV("stocks.csv")
	if err != nil {
		panic(fmt.Errorf("讀取股票列表失敗: %v", err))
	}

	// 建立 MongoDB 寫入工作的 Channel (Buffer 設定為 100，作為 Producer 與 Consumer 之間的緩衝)
	// 這樣爬蟲 (Producer) 不用等待資料庫寫入完成，可以立刻處理下一頁
	dbChan := make(chan StockData, 100)
	var wgDB sync.WaitGroup

	// 啟動 Worker Pool (Consumer)
	// 這裡啟動 2 個 Goroutine 專門負責寫入資料庫，控制 DB 連線數
	for i := 0; i < 2; i++ {
		wgDB.Add(1)
		go dbWorker(i, collection, dbChan, &wgDB)
	}

	var wg sync.WaitGroup
	// 建立一個容量為 3 的 buffered channel，用來限制同時執行的數量 (Semaphore)
	sem := make(chan struct{}, 3)

	for _, stockNo := range stockList {
		wg.Add(1)
		sem <- struct{}{} // 試圖佔用一個名額，如果目前已經有 3 個在跑，這裡會阻塞等待
		go func(id string) {
			defer wg.Done()
			defer func() { <-sem }() // 任務結束時（無論成功或失敗），釋放名額
			// 加入錯誤恢復機制 (Recover)，避免單一任務失敗導致整個程式崩潰
			defer func() {
				if r := recover(); r != nil {
					fmt.Printf("股票代號 %s 發生錯誤 (跳過): %v\n", id, r)
				}
			}()
			// 爬蟲 (Producer) 只負責抓取並將資料丟入 Channel
			scrapeStock(browser, dbChan, id)
		}(stockNo)
	}

	// 優雅關閉 (Graceful Shutdown) 流程：
	// 1. 等待所有爬蟲 (Producers) 完成任務
	wg.Wait()
	// 2. 關閉 Channel，這會向所有 Worker 發送「沒有更多資料」的訊號
	//    Worker 內部的 range 迴圈會因此結束
	close(dbChan)

	// 3. 等待所有 Worker (Consumers) 把 Channel 剩餘資料寫完並結束
	wgDB.Wait()
}

func scrapeStock(browser *rod.Browser, dbChan chan<- StockData, stockNo string) {
	fmt.Printf("正在處理股票代號: %s\n", stockNo)
	page := browser.MustPage("https://www.twse.com.tw/zh/trading/historical/stock-day-avg.html")
	defer page.MustClose() // 確保每個分頁處理完後關閉

	page.MustWaitLoad()
	page.MustWaitElementsMoreThan("form", 0)
	page.MustWaitStable() // 等待頁面穩定，確保 Loading 遮罩已消失

	// 找 input（Element UI）
	input := page.
		Timeout(40 * time.Second).            // 延長超時時間，避免網路慢或並發時發生 context deadline exceeded
		MustElement("input[name='stockNo']"). // 使用更精確的 name 屬性定位
		MustWaitVisible()
	// fmt.Println("input:", input)

	// 方法1: 用 Type
	// input.MustClick()
	// input.MustSelectAllText()
	// input.MustType('2', '3', '3', '0')
	// 方法2: 這行可以取代 MustClick, MustSelectAllText, MustType 三行
	input.MustInput(stockNo)

	time.Sleep(300 * time.Millisecond)

	// 點查詢
	page.MustElementR("button", "查詢").MustClick()

	// 等資料表
	page.MustWaitElementsMoreThan("tbody tr", 0)
	time.Sleep(time.Second)

	rows := page.MustElements("tbody tr")
	fmt.Println("rows:", len(rows))
	fmt.Printf("找到 %d 筆資料\n", len(rows))

	// 建立 CSV 檔案
	fileName := fmt.Sprintf("stock_data_%s.csv", stockNo)
	f, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// 寫入 UTF-8 BOM (避免 Excel 開啟時中文亂碼)
	f.WriteString("\xEF\xBB\xBF")

	w := csv.NewWriter(f)
	defer w.Flush()

	// 用於儲存要寫入 MongoDB 的資料
	// var documents []interface{}
	var documents []any
	for _, row := range rows {
		var cols []string
		for _, td := range row.MustElements("td") {
			cols = append(cols, strings.TrimSpace(td.MustText()))
		}
		fmt.Println(cols)
		if err := w.Write(cols); err != nil {
			panic(err)
		}

		// 準備 MongoDB 資料 (過濾掉 "月平均收盤價" 這一行)
		if len(cols) >= 2 && !strings.Contains(cols[0], "月平均") {
			doc := bson.M{
				"stock_id":   stockNo,
				"date":       cols[0],
				"price":      cols[1],
				"scraped_at": time.Now(),
			}
			documents = append(documents, doc)
		}
	}
	fmt.Printf("已儲存至 %s\n\n", fileName)

	// Producer: 將資料傳送至 Channel
	// 這裡取代了原本直接寫入 DB 的邏輯，實現了解耦
	if len(documents) > 0 {
		dbChan <- StockData{
			StockNo:   stockNo,
			Documents: documents,
		}
	}
}

// dbWorker 是消費者 (Consumer)，負責從 Channel 接收資料並寫入 MongoDB
func dbWorker(id int, collection *mongo.Collection, jobs <-chan StockData, wg *sync.WaitGroup) {
	defer wg.Done()
	// 當 jobs Channel 被關閉且緩衝區清空後，這個迴圈會自動結束
	for job := range jobs {
		// 使用無序寫入 (Unordered Writes) 來提升效能並允許部分失敗
		// 即使有幾筆資料因重複或其他原因寫入失敗，其他資料仍會繼續寫入
		opts := options.InsertMany().SetOrdered(false)
		insertResult, err := collection.InsertMany(context.TODO(), job.Documents, opts)
		if err != nil {
			fmt.Printf("[Worker %d] 股票代號 %s 寫入 MongoDB 時發生錯誤\n", id, job.StockNo)

			// 進行類型斷言，判斷錯誤是否為 BulkWriteException
			if bulkWriteException, ok := err.(mongo.BulkWriteException); ok {
				// 即使有錯誤，部分資料可能也寫入成功了
				if insertResult != nil && len(insertResult.InsertedIDs) > 0 {
					fmt.Printf("  => 成功寫入 %d 筆資料。\n", len(insertResult.InsertedIDs))
				}

				fmt.Printf("  => 但有 %d 筆資料寫入失敗:\n", len(bulkWriteException.WriteErrors))
				// 遍歷所有寫入錯誤
				for _, e := range bulkWriteException.WriteErrors {
					// e.Index 是失敗文件在原始 documents 切片中的索引
					failedDoc := job.Documents[e.Index] // 這就是失敗的那筆文件
					fmt.Printf("    - 原因: %s (錯誤碼: %d), 失敗文件的索引: %d\n", e.Message, e.Code, e.Index)
					// 在實際應用中，你可以將 failedDoc 存入一個 "dead-letter queue" 或日誌文件中，以便後續處理
					// 記錄失敗的文件
					logFailedDocument(failedDoc, e.Code, e.Message)
				}
			} else {
				// 如果不是 BulkWriteException，可能是連線中斷等更嚴重的問題
				fmt.Printf("  => 發生非預期的寫入錯誤: %v\n", err)
			}
		} else {
			fmt.Printf("[Worker %d] 成功寫入 %d 筆資料至 MongoDB (股票代號: %s)\n", id, len(insertResult.InsertedIDs), job.StockNo)
		}
	}
}

// readCSV 讀取 CSV 檔案並回傳第一欄的內容
func readCSV(filename string) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(f)
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}

	var list []string
	for _, record := range records {
		// 確保該行有資料，並取第一欄作為股票代號
		if len(record) > 0 {
			val := strings.TrimSpace(record[0])
			if val != "" {
				list = append(list, val)
			}
		}
	}
	return list, nil
}

// logFailedDocument 將失敗的寫入操作記錄到 JSON 檔案中 (Dead Letter Queue)
func logFailedDocument(doc interface{}, code int, msg string) {
	failureLogMutex.Lock()
	defer failureLogMutex.Unlock()

	// 定義日誌結構，包含錯誤資訊與原始資料
	type LogEntry struct {
		Timestamp string      `json:"timestamp"`
		ErrorCode int         `json:"error_code"`
		ErrorMsg  string      `json:"error_msg"`
		Document  interface{} `json:"document"`
	}

	entry := LogEntry{
		Timestamp: time.Now().Format(time.RFC3339),
		ErrorCode: code,
		ErrorMsg:  msg,
		Document:  doc,
	}

	// 開啟檔案 (Append 模式)，如果不存在則建立
	f, err := os.OpenFile("failed_inserts.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("無法開啟失敗日誌檔案: %v\n", err)
		return
	}
	defer f.Close()

	// 寫入 JSON
	encoder := json.NewEncoder(f)
	if err := encoder.Encode(entry); err != nil {
		fmt.Printf("寫入失敗日誌時發生錯誤: %v\n", err)
	}
}

/*
Q: 遇到了網站限制開啟過多分頁問題，如何修改限制同時執行的 goroutine 數量（例如一次最多 3 個）？

A: 要限制同時執行的 goroutine 數量（例如限制為 3 個），最簡單且道地的 Go 寫法是使用 Buffered Channel（緩衝通道） 來當作「信號量」（Semaphore）。
原理就像是停車場只有 3 個車位：
1. 進場 (sem <- struct{}{})：每次啟動 goroutine 前，先試著把車停進去。如果車位滿了（Channel 滿了），主程式就會卡在這裡等待，直到有車出來。
2. 出場 (<-sem)：當 goroutine 執行結束時，把車開出來，這樣主程式就能放下一台車進去。

修改重點解釋
1. sem := make(chan struct{}, 3)：建立一個容量只有 3 的通道。
2. sem <- struct{}{}：在 go func 之前執行。這行程式碼的意思是「往通道塞一個東西」。因為容量只有 3，當塞第 4 個的時候，程式會停在這裡（Block），直到有人把東西拿走。
3. defer func() { <-sem }()：在 goroutine 內部使用 defer。這行程式碼的意思是「當這個分身結束時，從通道拿走一個東西」。這樣通道就會空出一個位置，讓外面等待的迴圈可以繼續跑下一圈。

這樣就能確保任何時間點，最多只有 3 個分頁（goroutine）在同時運作，避免觸發網站限制或電腦當機。
*/

/*
問題一：為什麼 chan 定義為 struct{}？

答案：為了省記憶體，並明確表達「我不關心傳了什麼值，只在乎訊號」。
1. 零記憶體佔用 (Zero Memory Overhead)： 在 Go 語言中，空結構體 struct{} 是不佔用任何記憶體空間的（0 bytes）。
如果用 chan int，傳送一個訊號需要佔用 4 或 8 bytes。
如果用 chan bool，傳送一個訊號需要佔用 1 byte。
用 chan struct{}，佔用 0 bytes。 雖然在這個小程式中差異微乎其微，但在高並發或大量 Channel 的場景下，這是最佳實踐。
2. 語意明確 (Clear Semantics)： 當其他工程師看到 chan struct{} 時，他們會立刻明白：「這個 Channel 只是用來做通知或控制流程（如 Semaphore）的，裡面傳遞的資料內容本身沒有意義」。

問題二：sem <- struct{}{}，其中 struct{}{} 是什麼意思？

這行程式碼可以拆解成兩部分來看：
1. struct{}{} 是什麼？
這是 Go 語言中「建立一個空結構體實例」的寫法。 它由兩個部分組成：
- struct{} (前半部)：定義型別。這是一個沒有任何欄位的匿名結構體型別。
- {} (後半部)：初始化。這表示建立這個型別的一個實體（Instance）。
這就像你寫 int(0) 或 string("") 一樣，只是因為這個型別叫 struct{}，所以寫起來變成 struct{}{}，看起來有點像俄羅斯娃娃。
2. sem <- ... 是什麼？
這是 傳送（Send） 操作。

sem <- struct{}{} 的意思就是：「把一個空結構體丟進 sem 這個通道裡」。
在程式碼中的作用： 因為 sem 是一個容量為 3 的 Buffered Channel（緩衝通道）：
sem <- struct{}{} (進場)： 程式試圖把一個空結構體塞進通道。
如果通道裡已經有 3 個東西了（滿了），這行程式碼就會卡住（Block），直到有人拿走一個東西。
這就達到了「限制同時只能有 3 個 Goroutine 在跑」的效果。

<-sem (出場)： 從通道裡拿出一個東西（並丟棄它，因為我們不關心那個空結構體）。這會讓通道空出一個位置，讓外面卡住的程式可以擠進來。
*/

/*
Q: 跑了幾個股票代號後出現 panic: context deadline exceeded

A: 這個錯誤 panic: context deadline exceeded 表示在規定的時間內（原本設定 15 秒）網頁元素沒有出現，導致程式報錯。因為你現在是並發執行（同時開 3 個分頁），電腦或網路的負載變大，網頁載入速度可能會變慢，導致原本的 15 秒不夠用。
此外，因為使用了 goroutine，如果其中一個發生 panic，預設會導致整個程式崩潰。
解決方法有兩點：
1. 加入 recover 機制：在 go func 裡捕捉錯誤，這樣就算某個股票失敗，也不會影響其他股票繼續執行。
2. 延長超時時間：將原本的 15 秒延長到 40 秒或更長。
*/
