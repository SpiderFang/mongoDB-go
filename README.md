# MongoDB 資料寫入指南

要將資料存入 MongoDB，我們需要引入 MongoDB 的官方 Go Driver，並在程式中建立連線，然後將抓取到的資料轉換為 BSON 格式後寫入資料庫。

## 修改步驟

### 1. 安裝 MongoDB Driver

在終端機執行以下指令來安裝必要的套件：

```bash
go get go.mongodb.org/mongo-driver/mongo
```

### 2. 修改 main.go

主要修改包含：

*   **引入套件**：加入 MongoDB 相關的 import。
*   **建立連線**：在 `main` 函式開頭建立 MongoDB 連線。
*   **傳遞 Collection**：將 MongoDB 的 Collection 物件傳遞給 `scrapeStock` 函式。
*   **寫入資料**：在 `scrapeStock` 解析完資料後，執行 `InsertMany` 寫入資料庫。

## 修改重點說明

1.  **MongoDB 連線設定**: 在 `main` 函式中，我們使用 `mongo.Connect` 建立連線。這裡預設連線到本機的 `mongodb://localhost:27017`，並使用 `twse` 資料庫中的 `stock_prices` 集合 (Collection)。
2.  **資料結構 (BSON)**: 使用 `bson.M` (Map) 來建構資料物件。我加入了 `stock_id` (股票代號)、`date` (日期)、`price` (收盤價) 以及 `scraped_at` (抓取時間)。
3.  **過濾資料**: 證交所的表格最後一行通常是「月平均收盤價」，這行資料格式不同，通常不適合直接存入以「日」為單位的資料庫結構中，所以我加了 `!strings.Contains(cols[0], "月平均")` 來過濾掉它。
4.  **批次寫入 (InsertMany)**: 為了效能考量，我們先將所有解析好的資料存入 `documents` slice，最後再一次性呼叫 `InsertMany` 寫入資料庫，這樣比一筆一筆寫入快得多。


## 如何優化 MongoDB 的 InsertMany 寫入效能？

優化 MongoDB InsertMany 的寫入效能，通常可以從程式碼設定、資料庫配置以及架構設計這三個層面著手。

在 Go 語言的 mongo-driver 環境下，以下是幾個最有效的優化策略：

1.  **使用 Unordered Writes (推薦)**:
    預設情況下，InsertMany 是 Ordered (有序) 的。這意味著 MongoDB 會依照順序寫入，如果中間有一筆失敗，後續的寫入就會停止。

    將其設為 Unordered (無序) 可以讓 MongoDB Server 平行處理這些寫入請求（特別是在分片叢集中），並且即使單筆失敗，其餘資料仍會繼續寫入，通常能顯著提升效能。
    預設寫入是有序的，若設為無序 (`SetOrdered(false)`)，MongoDB 可以平行處理寫入請求，且單筆失敗不會中斷後續寫入。
    ```go
    import (
        "go.mongodb.org/mongo-driver/mongo/options"
    )

    // ...

    // 設定 Ordered 為 false
    opts := options.InsertMany().SetOrdered(false)

    // 執行寫入
    _, err := collection.InsertMany(ctx, documents, opts)
    if err != nil {
        // 注意：這裡的錯誤可能包含部分寫入成功的資訊
        log.Printf("部分寫入可能失敗: %v", err)
    }
    ```

2.  **調整 Batch Size (批次大小)**: 
    雖然 InsertMany 本身就是批次寫入，但如果一次塞入過多資料（例如 10 萬筆），可能會導致記憶體飆升或觸發 MongoDB 的 BSON 文件大小限制（雖然 Driver 會自動拆分，但會增加客戶端負擔）。

    建議將大數據拆分成適當的區塊（Chunk），例如每 1,000 到 5,000 筆執行一次 InsertMany。
    ```go
    batchSize := 2000
    var batch []interface{}

    for i, doc := range allDocuments {
        batch = append(batch, doc)
        // 當累積到 batchSize 或最後一筆時寫入
        if len(batch) == batchSize || i == len(allDocuments)-1 {
            _, err := collection.InsertMany(ctx, batch, options.InsertMany().SetOrdered(false))
            if err != nil {
                log.Println(err)
            }
            // 清空 slice，保留容量以提升效能
            batch = batch[:0]
        }
    }
    ```

3.  **使用 Goroutines 平行寫入 (Concurrency)**: 
    如果你的資料量非常大，單一執行緒的 InsertMany 可能會受限於網路延遲 (RTT)。你可以啟動多個 Goroutine 並行處理不同的 Batches。

    注意：不要開過多 Goroutine，這會導致 MongoDB Server 的連線數耗盡或 CPU 滿載。通常控制在 CPU 核心數的 2-4 倍或使用 Worker Pool 模式較佳。

4.  **調整 Write Concern (寫入關注)**: 
    若允許極少量資料遺失以換取極致效能，可將 Write Concern 設為 `w:0` (不等待確認)。

    這是效能與資料安全性的權衡。
        預設 (w:1)：Primary 節點確認寫入記憶體即返回。
        w:0 (Fire and Forget)：不等待資料庫確認，速度最快，但風險最高（不知道是否寫入成功）。
        j:true (Journaling)：等待寫入硬碟日誌才返回，最安全但最慢。
        
    若你是做大量數據遷移且允許極少量遺失（或可重跑），可以考慮暫時降低 Write Concern。
    ```go
    import "go.mongodb.org/mongo-driver/mongo/writeconcern"

    // 建立連線時設定，或在 Collection 層級設定
    wc := writeconcern.New(writeconcern.W(0)) // 極速模式，不等待確認
    coll := client.Database("twse").Collection("stock_prices", options.Collection().SetWriteConcern(wc))
    ```

5.  **索引 (Indexes) 的影響**: 
    索引是寫入效能的殺手。每次寫入資料，MongoDB 都必須更新所有的索引樹。

    策略：如果是初次匯入大量資料，建議先刪除所有索引（除了 _id），資料寫入完成後，再使用 CreateIndexes 重建索引。這通常比帶著索引寫入快得多。

6. **減少 BSON 大小**: 
   - 欄位名稱：BSON 會儲存完整的欄位名稱。如果資料量極大，縮短欄位名稱（例如 stock_price 改為 p）可以節省磁碟空間與網路頻寬，進而提升寫入速度。
   - 資料類型：確保使用正確的型別（例如不要把數字存成字串）。

總結建議
1. 務必開啟 SetOrdered(false)。
2. 控制每次 InsertMany 的數量在 1000-5000 筆之間。
3. 如果是初始化資料庫，先寫資料，後建索引。
4. 根據需求調整 Write Concern。


## 關於 InsertMany 操作中若有單筆資料寫入失敗該如何處理？

在使用 `InsertMany` 操作時，如果有單筆資料寫入失敗，MongoDB Driver 會回傳一個錯誤，這個錯誤中包含了部分成功與失敗的資訊。
這個問題的處理方式，取決於您對 InsertMany 的設定。基本上有兩種模式：
1. 有序寫入 (Ordered Inserts - 預設行為)：這是 InsertMany 的預設模式。MongoDB 會依照您提供的資料順序逐筆寫入。如果中間有一筆資料寫入失敗（例如 _id 重複），整個寫入操作就會立刻中斷，後續的資料將不會被寫入。
2. 無序寫入 (Unordered Inserts)：您可以手動設定此模式。在無序模式下，MongoDB 會嘗試寫入所有您提供的資料，即使中間有幾筆失敗，它也會跳過錯誤的資料，繼續寫入剩下的部分。MongoDB Server 甚至可能為了提升效能而平行處理這些寫入。

建議採用「無序寫入」，這麼做有兩個主要好處：
1. 容錯性：單筆資料的失敗（例如網路波動、資料格式錯誤、唯一鍵衝突）不會導致整個批次的任務中斷。
2. 高效能：允許 MongoDB 平行處理寫入請求，通常能顯著提升大量資料寫入時的吞吐量。

程式碼修改：
要啟用無序寫入，您需要在呼叫 InsertMany 時傳入一個 options 物件，並將 Ordered 設為 false。同時，錯誤處理也應該更加細緻，而不是直接 panic。

修改重點說明：
1. 引入 options：go.mongodb.org/mongo-driver/mongo/options 套件讓您可以對資料庫操作進行細部設定。雖然您的程式碼中已經引入了 options，但在 scrapeStock 函式中我們將明確使用它。
2. 設定 SetOrdered(false)：我們建立一個 options.InsertMany() 的實例，並呼叫 SetOrdered(false) 來告訴 MongoDB 執行無序寫入。
3. 傳入 opts：將設定好的 opts 物件作為 collection.InsertMany 的第三個參數傳入。
4. 改善錯誤處理：當使用無序寫入時，回傳的 err 可能是 mongo.BulkWriteException 型別。這個錯誤物件內部其實包含了哪些資料寫入成功、哪些失敗的詳細資訊。因此，我們不再使用 panic 來粗暴地終止程式，而是改用 fmt.Printf 印出錯誤，讓主程式可以繼續處理下一個股票代號。
透過這樣的修改，您的爬蟲程式將會更加健壯且高效。


## 當使用無序寫入（Unordered Writes）時，知道如何處理那些失敗的資料，是確保資料完整性和系統健壯性的重要一步。

當 InsertMany 在無序模式下發生錯誤時，MongoDB Driver 會回傳一個 mongo.BulkWriteException 型別的錯誤。這個錯誤物件並不像一般錯誤那樣只是一條訊息，它內部詳細記錄了每一筆失敗的資訊。

* 處理失敗寫入的策略

處理這些失敗資料的常見策略有以下幾種：

1. 詳細記錄 (Logging)：最基本也最重要的步驟。將失敗的文件、失敗原因（例如：重複的 _id）、錯誤碼都記錄到你的日誌系統中，方便後續追蹤和分析。
2. 重試機制 (Retry)：如果失敗的原因是暫時性的（例如：網路抖動、暫時的資料庫超載），你可以設計一個重試邏輯。但要注意，像「主鍵衝突 (Duplicate Key)」這種錯誤是永久性的，重試也沒用，應該直接跳過或記錄。
3. 存入「死信佇列」(Dead-Letter Queue)：這是最推薦的健壯作法。將所有處理失敗的文件存到另一個地方，例如一個專門的 MongoDB Collection、一個日誌檔案，或是像 RabbitMQ、Kafka 這樣的訊息佇列。這樣做的好處是：
    - 主程式可以繼續正常運作，不受影響。
    - 你可以事後再寫一個獨立的程式或手動去分析和處理這些「死信」，看是要修正資料後重新匯入，還是直接捨棄。

* 程式碼實作
接下來，我將修改 main.go 中的 scrapeStock 函式，向你展示如何解析 mongo.BulkWriteException 並取得失敗的詳細資訊。

我們將修改原本的錯誤處理區塊，進行類型斷言（Type Assertion），並遍歷其中的 WriteErrors。

* 修改重點說明

1. 接收 insertResult：我們不再用 _ 忽略 InsertMany 的第一個回傳值，而是用 insertResult 接收它。insertResult.InsertedIDs 包含了一個由所有成功寫入文件的 _id 組成的列表，我們可以透過 len(insertResult.InsertedIDs) 精確地知道有多少筆資料成功了。
2. 類型斷言：我們使用 if bulkWriteException, ok := err.(mongo.BulkWriteException); ok 來檢查回傳的 err 是否是我們預期的 BulkWriteException。
3. 遍歷 WriteErrors：如果斷言成功，我們就可以遍歷 bulkWriteException.WriteErrors。這是一個切片（Slice），裡面存放了所有失敗的詳細資訊。
4. 取得失敗詳情：在迴圈中，e.Message 和 e.Code 提供了失敗的原因和錯誤碼（例如，11000 代表 E11000 duplicate key error）。e.Index 則告訴你這筆失敗的資料在原始 documents 切片中的索引位置。
5. 取得失敗文件：透過 e.Index，你可以用 documents[e.Index] 輕鬆地取回完整的失敗文件，然後決定要如何處理它（例如寫入日誌或死信佇列）。
6. 更精確的成功計數：無論成功還是部分失敗，我們都改用 len(insertResult.InsertedIDs) 來顯示實際成功寫入的筆數，這比之前用 len(documents) 更加準確。

透過這種方式，你的程式不僅能容忍部分失敗，還能精確地追蹤到每一筆失敗的資料，讓後續的維護和資料補救工作變得有據可依。


## 在實際應用中，可以將 failedDoc 存入一個 "dead-letter queue" 或日誌文件中，以便後續處理

這是一個非常實用的需求。在本地執行的爬蟲或小型工具中，最簡單且有效的「死信佇列 (Dead-Letter Queue)」實作方式，就是將失敗的資料寫入一個本地的 JSON 檔案。

這樣做的好處是格式通用（JSON），方便你之後用其他程式讀取、分析，甚至直接修正後重新匯入 MongoDB。

由於程式是並發執行（多個 Goroutine 同時跑），寫入同一個檔案時必須注意執行緒安全 (Thread Safety)。我們需要使用 sync.Mutex 來確保不會有兩個人同時寫入檔案導致內容錯亂。

以下是具體的實作步驟與程式碼修改：
修改計畫說明：
1. 引入套件：加入 encoding/json 用於序列化資料。
2. 加入全域鎖 (Mutex)：宣告一個 failureLogMutex 來保護日誌檔案的寫入操作。
3. 實作 logFailedDocument 函式：這個函式會接收失敗的文件與錯誤訊息，將其包裝成 JSON 格式並「附加 (Append)」到 failed_inserts.json 檔案中。
4. 呼叫函式：在 scrapeStock 的錯誤處理迴圈中呼叫此函式。

實作細節說明:
1. failureLogMutex：
   - 因為您的程式使用了 Goroutine 並發執行，多個股票可能同時發生寫入錯誤。如果不加鎖，同時寫入同一個檔案會導致內容交錯亂碼。
   - Lock() 和 Unlock() 確保同一時間只有一個 Goroutine 能寫入檔案。

2. logFailedDocument 函式：
    - 結構化記錄：我們定義了一個 LogEntry 結構，不僅儲存原始資料 (Document)，還加上了 Timestamp (發生時間)、ErrorCode (錯誤碼) 和 ErrorMsg (錯誤訊息)。這對後續排查問題非常有幫助。
   - 檔案模式：使用 os.O_APPEND|os.O_CREATE|os.O_WRONLY 開啟檔案。這表示如果檔案不存在就建立；如果存在，則將新資料寫在檔案的最後面 (Append)，而不是覆蓋舊資料。
   - JSON Encoder：json.NewEncoder(f).Encode(entry) 會自動將結構轉為 JSON 字串並寫入檔案，且會自動換行。這樣產生的檔案格式稱為 JSON Lines，每一行都是一個獨立且完整的 JSON 物件。

後續如何使用這個檔案？
當程式跑完後，如果 failed_inserts.json 產生了，您可以：
1. 檢視錯誤：直接用文字編輯器打開，查看 error_msg 欄位了解失敗原因。
2. 重試/補救：您可以寫另一個簡單的 Go 程式或 Python 腳本，逐行讀取這個檔案，修正資料（例如修改重複的 ID），然後重新 Insert 到 MongoDB 中。


# 重構程式碼, 將「爬蟲 (Producer)」與「資料庫寫入 (Consumer)」解耦

將「爬蟲 (Producer)」與「資料庫寫入 (Consumer)」解耦，可以讓爬蟲專注於抓取資料（IO Bound，但受限於網速與網站限制），而資料庫寫入則由專門的 Worker 處理（IO Bound，但速度通常較快且穩定）。

這樣的 Worker Pool 模式有幾個好處：
1. 職責分離：爬蟲不再需要等待資料庫寫入完成才能處理下一頁。
2. 錯誤隔離：資料庫寫入的暫時性錯誤不會直接卡住爬蟲流程。
3. 流量控制：你可以固定資料庫的連線數（例如 2 個 Worker），避免因為爬蟲開太多併發而瞬間灌爆資料庫連線。

以下是修改後的程式碼，主要變動如下：

1. 新增 StockData 結構，用於在 Channel 中傳遞資料。這是一個簡單的結構體，用來封裝從爬蟲抓取到的資料。它包含了 StockID、Date 和 Price 三個欄位，這些都是我們最終要寫入 MongoDB 的資料。
2. 在 main 中建立 dbChan (Channel) 與啟動 dbWorker (Consumer)。
3. 修改 scrapeStock，將原本的寫入邏輯改為「發送資料到 Channel」。
4. 新增 dbWorker 函式，專門負責從 Channel 接收資料並寫入 MongoDB。

詳細架構說明：

這個模式將程式拆解為三個主要部分，就像工廠的流水線一樣：

A. 輸送帶 (Channel)
```go
dbChan := make(chan StockData, 100)
```
* 角色：這是連接爬蟲與資料庫的「緩衝區」。
* 為什麼要 Buffer (100)？：
    * 如果沒有 Buffer (容量為 0)，爬蟲抓完資料要丟進去時，必須等待 Worker 剛好有空來拿，否則爬蟲會卡住 (Block)。
    * 有了 Buffer，爬蟲抓完資料丟進 Channel 就可以立刻去抓下一頁，不用管資料庫寫入是否來得及。這就是解耦 (Decoupling)。

B. 生產者 (Producer) - scrapeStock
```go
// 舊做法：直接寫入 DB (會卡住等待 DB 回應)
// collection.InsertMany(...)

// 新做法：丟進 Channel 就跑
dbChan <- StockData{StockNo: stockNo, Documents: documents}
```
* 職責：只負責「抓資料」和「整理資料」。
* 優點：爬蟲是 IO Bound (網路)，通常很慢。我們不希望它因為資料庫連線慢或鎖定而變更慢。現在它只要把資料丟上輸送帶，任務就結束了。

C. 消費者 (Consumer) - dbWorker
```go
func dbWorker(id int, collection *mongo.Collection, jobs <-chan StockData, wg *sync.WaitGroup) {
    for job := range jobs {
        // 專心處理寫入資料庫
        collection.InsertMany(...)
    }
}
```
* 職責：只負責「寫入資料庫」。
* 併發控制：你在 main 中啟動了 2 個 Worker。這意味著無論你有 10 個還是 100 個爬蟲在跑，同時對 MongoDB 的連線數永遠只有 2 個。
* 保護資料庫：這能避免因為爬蟲開太多，瞬間產生大量連線把 MongoDB 打掛 (Connection Storm)。

## 優雅關閉 (Graceful Shutdown) 的流程
如何安全地結束程式，確保資料不遺失：
1. 爬蟲工作 (wg)：主程式等待所有爬蟲完成 (wg.Wait())。
2. 關閉通道 (close(dbChan))：
    * 當所有爬蟲都結束了，表示不會再有新資料進來了。
    * 這時我們關閉 Channel。這就像在輸送帶入口掛上「停止進貨」的牌子。
3. Worker 收尾：
    * Worker 中的 for job := range jobs 迴圈會偵測到 Channel 被關閉且資料已讀完，然後自動跳出迴圈。
4. 等待寫入完成 (wgDB)：
    * 主程式最後等待 Worker 把輸送帶上剩餘的貨物都處理完 (wgDB.Wait())，才真正結束程式。

## 總結優點

1. 速度更快：爬蟲不用等寫入，寫入不用等爬蟲。
2. 穩定性高：限制了 DB 連線數，不會因為爬蟲暴衝而拖垮資料庫。
3. 容錯性：如果 DB 暫時變慢，資料會堆積在 Channel (Buffer) 裡，不會直接影響爬蟲的網路請求。
