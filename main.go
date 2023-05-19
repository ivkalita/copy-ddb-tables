package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"golang.org/x/sync/errgroup"
)

func main() {
	err := run(context.Background())
	if err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	copyConfig, err := parseConfig(ctx)
	if err != nil {
		return fmt.Errorf("parse config: %w", err)
	}

	sdkConfig, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load aws sdk config: %w", err)
	}

	err = copyTables(ctx, sdkConfig, copyConfig)
	if err != nil {
		return fmt.Errorf("copy tables: %w", err)
	}

	return nil
}

func parseConfig(_ context.Context) (copyConfig, error) {
	args := os.Args[1:]
	if len(args) != 2 {
		return copyConfig{}, fmt.Errorf("want 2 arguments (src, dest), got %d", len(args))
	}

	src, dest := args[0], args[1]
	return copyConfig{tablePairs: []tablePair{{src, dest}}}, nil
}

type tablePair struct {
	src  string
	dest string
}

type copyConfig struct {
	tablePairs []tablePair
}

func copyTables(ctx context.Context, sdkConfig aws.Config, copyConfig copyConfig) error {
	ddbClient := dynamodb.NewFromConfig(sdkConfig)
	eg, ctx := errgroup.WithContext(ctx)

	for _, pair := range copyConfig.tablePairs {
		copier := tableCopier{
			src:       pair.src,
			dest:      pair.dest,
			ddbClient: ddbClient,
		}
		eg.Go(func() error {
			err := copier.run(ctx)
			if err != nil {
				return fmt.Errorf("process (%s->%s): %w", copier.src, copier.dest, err)
			}
			return nil
		})
	}

	err := eg.Wait()
	if err != nil {
		return fmt.Errorf("wait: %w", err)
	}
	return nil
}

type tableCopier struct {
	src       string
	dest      string
	ddbClient *dynamodb.Client
}

func (t tableCopier) log(msg string) {
	log.Printf(
		"(%s -> %s) %s\n",
		t.firstNChars(t.src, 30),
		t.firstNChars(t.dest, 30),
		msg,
	)
}

func (t tableCopier) firstNChars(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}

	return s
}

func (t tableCopier) run(ctx context.Context) error {
	t.log("start copy")
	var totalItemsCopied uint64 = 0
	lastTime := time.Now()
	var exclusiveStartKey map[string]types.AttributeValue
	const maxBatchSize = 25

	for {
		scanOutput, err := t.ddbClient.Scan(ctx, &dynamodb.ScanInput{
			TableName:         &t.src,
			ExclusiveStartKey: exclusiveStartKey,
		})
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		for i := 0; i < len(scanOutput.Items); i += maxBatchSize {
			end := i + maxBatchSize
			if end > len(scanOutput.Items) {
				end = len(scanOutput.Items)
			}

			err := t.writeBatch(ctx, scanOutput.Items[i:end])
			if err != nil {
				return fmt.Errorf("write batch: %w", err)
			}

			totalItemsCopied += uint64(end - i)
			if time.Since(lastTime).Seconds() > 5 {
				t.log(fmt.Sprintf("%d items copied", totalItemsCopied))
				lastTime = time.Now()
			}
		}

		if scanOutput.LastEvaluatedKey == nil {
			break
		}
		exclusiveStartKey = scanOutput.LastEvaluatedKey
	}

	t.log(fmt.Sprintf("%d items copied", totalItemsCopied))

	return nil
}

func (t tableCopier) writeBatch(ctx context.Context, items []map[string]types.AttributeValue) error {
	writeRequests := make([]types.WriteRequest, 0, len(items))
	for _, item := range items {
		writeRequests = append(writeRequests, types.WriteRequest{
			PutRequest: &types.PutRequest{Item: item},
		})
	}

	batchWriteInput := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{t.dest: writeRequests},
	}

	batchWriteOutput, err := t.ddbClient.BatchWriteItem(ctx, batchWriteInput)
	if err != nil {
		return err
	}

	// Check if there are any unprocessed items
	if len(batchWriteOutput.UnprocessedItems) > 0 {
		return errors.New(fmt.Sprintf("unprocessed items found: %v", batchWriteOutput.UnprocessedItems))
	}

	return nil
}
