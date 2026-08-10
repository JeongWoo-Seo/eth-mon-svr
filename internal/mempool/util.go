package mempool

import "github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"

func getFeeBucket(tip uint64) uint32 {
	return uint32(tip / blockstore.FeeBucketSize)
}
