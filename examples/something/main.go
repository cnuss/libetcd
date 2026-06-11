package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/cnuss/libetcd"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // cancelling the context gracefully stops the nodes

	var wg sync.WaitGroup

	fmt.Println("starting leader...")
	leader := libetcd.New().WithContext(ctx)
	if err := leader.Start(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("leader started with peer URLs: %v\n", leader.Peers())

	for i := range 3 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			log.Printf("joining peer %d to cluster...", i)
			peer := libetcd.From(leader.Peers()...).WithContext(ctx)
			if err := peer.Join(); err != nil {
				log.Printf("join error: %v", err)
			}
			// if _, err := peer.Self().Put(ctx, time.Nanosecond.String(), fmt.Sprintf("hello from %v", peer.Self().Endpoints())); err != nil {
			// 	log.Printf("put error: %v", err)
			// }
		}(i)
	}
	wg.Wait()

	fmt.Printf("voters: %v\n", leader.Voters().Endpoints())
	fmt.Printf("peers: %v\n", leader.Peers())
	// cli := leader.Voters()
	// allData, err := cli.Get(ctx, "", clientv3.WithPrefix())
	// if err != nil {
	// 	log.Printf("get error: %v", err)
	// }
	// for _, kv := range allData.Kvs {
	// 	fmt.Printf("%s: %s\n", kv.Key, kv.Value)
	// }
}
