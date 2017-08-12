package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"sample/article"

	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial("localhost:8080", grpc.WithInsecure())

	if err != nil {
		fmt.Println("Unexpected Error", err)
	}

	defer conn.Close()
	c := article.NewArticleHandlerClient(conn)

	req := &article.SingleRequest{
		Id: 1,
	}
	r, err := c.GetArticle(context.Background(), req)
	if err != nil {
		fmt.Println("Unexpected Error", err)
	}
	fmt.Println("Article : ", r.Title)
	callStream(c)

	// fmt.Println("DOING BATCH INSERT")

	// batchInsert(c)

	fmt.Println("Doing Batch Update")
	batchUpdate(c)
}
func batchInsert(c article.ArticleHandlerClient) {
	raw, err := ioutil.ReadFile("insert.json")
	if err != nil {
		fmt.Println("Something error when trying to read file", err)
	}

	var data []*article.Article

	err = json.Unmarshal(raw, &data)
	if err != nil {
		fmt.Println("Something happen when trying to unmarshall", err)
	}

	stream, err := c.BatchInsert(context.Background())
	if err != nil {
		fmt.Println("Unexpected Error", err)
	}
	for _, v := range data {
		if err := stream.Send(v); err != nil {
			fmt.Println("Cant Sent data ", v, " Error ", err)
		}
	}

	reply, err := stream.CloseAndRecv()
	if err != nil {
		fmt.Println("Unexpected Error", err)
	}
	fmt.Println("TOTAL SUCCESS ", reply.GetTotalSuccess())
	fmt.Println("Total Error ", len(reply.GetErrors()))

}

func batchUpdate(c article.ArticleHandlerClient) {
	raw, err := ioutil.ReadFile("update.json")
	if err != nil {
		fmt.Println("Something error when trying to read file")
	}

	var data []*article.Article

	err = json.Unmarshal(raw, &data)
	if err != nil {
		fmt.Println("Something happen when trying to unmarshall")
	}

	stream, err := c.BatchUpdate(context.Background())
	if err != nil {
		fmt.Println("Unexpected Error", err)
	}
	//using buffered chanel
	ch := make(chan article.Article)
	go func(chan article.Article) {
		for {
			ar, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("DO YOU IN ? ")
				close(ch)

				return
			}
			if err != nil {
				fmt.Println("ERROR HAPPEN", err)
				close(ch)
				return
			}
			fmt.Println("BRAPA KALI MASUK")
			ch <- *ar
		}

	}(ch)

	for _, v := range data {
		if err := stream.Send(v); err != nil {
			fmt.Println("ERROR : Deal with it ", err)
		}
	}

	stream.CloseSend()

	for c := range ch {
		fmt.Println("Success to update ", c.Title)
	}

}
func callStream(c article.ArticleHandlerClient) {
	f := &article.FetchRequest{
		Num:    5,
		Cursor: "",
	}
	stream, err := c.FetchArticle(context.Background(), f)
	if err != nil {
		fmt.Println("Unexpected Error", err)
	}

	for {
		a, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("Unexpected Error", err)
			break
		}
		fmt.Println("Data From Stream :  ", a.Title)
	}
}
