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