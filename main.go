package main

import (
	"context"
	"encoding/csv"
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
			scrapeStock(browser, collection, id)
		}(stockNo)
	}
	wg.Wait()
}

func scrapeStock(browser *rod.Browser, collection *mongo.Collection, stockNo string) {
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

	// 批次寫入 MongoDB
	if len(documents) > 0 {
		_, err := collection.InsertMany(context.TODO(), documents)
		if err != nil {
			panic(fmt.Errorf("寫入 MongoDB 失敗: %v", err))
		}
		fmt.Printf("成功寫入 %d 筆資料至 MongoDB\n", len(documents))
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
