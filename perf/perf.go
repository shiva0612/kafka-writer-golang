package main

import (
	"bytes"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var (
	res_200                 uint64
	res_500                 uint64
	res_400                 uint64
	request_creation_failed uint64
	api_request_failed      uint64
	unknown_failed          uint64
)

func main() {
	concurrency := 10

	totalRequests := 3000

	apiURL := `http://localhost:8080/push?topic=JRCS_SMT&key=some_key` // Replace with your API URL
	body := []byte("send this to kafka")
	var wg sync.WaitGroup

	startTime := time.Now()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < totalRequests/concurrency; j++ {
				req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(body))
				if err != nil {
					fmt.Printf("Error creating request: %v\n", err)
					atomic.AddUint64(&request_creation_failed, 1)
					return
				}

				client := &http.Client{}
				resp, err := client.Do(req)
				if err != nil {
					fmt.Printf("Error sending request: %v\n", err)
					atomic.AddUint64(&api_request_failed, 1)
					return
				}
				defer resp.Body.Close()

				switch resp.StatusCode {
				case http.StatusOK:
					atomic.AddUint64(&res_200, 1)
				case http.StatusInternalServerError:
					atomic.AddUint64(&res_500, 1)
				case http.StatusBadRequest:
					atomic.AddUint64(&res_400, 1)
				default:
					atomic.AddUint64(&unknown_failed, 1)

				}
			}
		}()
	}

	wg.Wait()

	elapsedTime := time.Since(startTime)

	tps := float64(totalRequests) / elapsedTime.Seconds()

	fmt.Printf("Total Requests: %d\n", totalRequests)
	fmt.Printf("Concurrency Level: %d\n", concurrency)
	fmt.Printf("Time Taken: %.2f seconds\n", elapsedTime.Seconds())
	fmt.Printf("Transactions Per Second (TPS): %.2f\n", tps)

	fmt.Printf("no of req creation failed: %d\n", atomic.LoadUint64(&request_creation_failed))
	fmt.Printf("no of api req failed: %d\n", atomic.LoadUint64(&api_request_failed))
	fmt.Printf("200 Responses: %d\n", atomic.LoadUint64(&res_200))
	fmt.Printf("400 Responses: %d\n", atomic.LoadUint64(&res_400))
	fmt.Printf("500 Responses: %d\n", atomic.LoadUint64(&res_500))
	fmt.Printf("no of unknown errors: %d\n", atomic.LoadUint64(&unknown_failed))
}
