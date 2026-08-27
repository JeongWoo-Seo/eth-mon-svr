package processor

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/blockstore"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/network/grpcClient"
	rpcmanager "github.com/JeongWoo-Seo/eth-mon-svr/internal/network/rpcManager"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor/mocks"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"go.uber.org/mock/gomock"
)

// --- test helpers ---------------------------------------------------------

func hashAt(i int) common.Hash {
	return common.HexToHash(fmt.Sprintf("0x%02x", i))
}
func newTestTx(nonce uint64) *types.Transaction {
	return types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		GasPrice: big.NewInt(1_000_000_000),
		Gas:      21000,
		To:       &common.Address{},
	})
}

// fetchPlan describes how the mock FetchBatch should populate each batch
// element: a hash in errs becomes an individual error, a hash in txs becomes a
// valid transaction, and a hash in neither stays a nil result.
type fetchPlan struct {
	txs  map[common.Hash]*types.Transaction
	errs map[common.Hash]error
}

func applyFetchPlan(elems []rpc.BatchElem, plan fetchPlan) {
	for j := range elems {
		hash := elems[j].Args[0].(common.Hash)
		if e, ok := plan.errs[hash]; ok {
			elems[j].Error = e
			continue
		}
		if tx, ok := plan.txs[hash]; ok {
			if ptr, ok := elems[j].Result.(**types.Transaction); ok {
				*ptr = tx
			}
		}
	}
}

func assertPushed(t *testing.T, pushed, want []*types.Transaction) {
	t.Helper()
	if len(pushed) != len(want) {
		t.Fatalf("pushed %d txs, want %d", len(pushed), len(want))
	}
	for i := range want {
		if pushed[i] != want[i] {
			t.Fatalf("pushed[%d] = %p, want %p", i, pushed[i], want[i])
		}
	}
}

// --- GetTxInfo -------------------------------------------------------------

// PushBatch이 호출되면 test 결과 fail 처리
func TestGetTxInfo_RPCCallError_NoPush(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockRPC := mocks.NewMockRPCManager(ctrl)
	mockGasAnalyzer := mocks.NewMockGasAnalyzer(ctrl)
	mockPendingPool := mocks.NewMockPendingPool(ctrl)

	hashes := []common.Hash{
		hashAt(0),
		hashAt(1),
	}

	mockRPC.EXPECT().
		FetchBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(errors.New("rpc down")).
		Times(1)

	mockGasAnalyzer.EXPECT().
		GetCurrentBlockNumAndTime().
		Times(0)

	mockPendingPool.EXPECT().
		PushBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)

	p := &Process{
		rpcManager:  mockRPC,
		gasanalyzer: mockGasAnalyzer,
		pendingPool: mockPendingPool,
	}

	p.GetTxInfo(context.Background(), hashes)
}

func TestGetTxInfo_IndividualBatchElemError_Filtered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRPC := mocks.NewMockRPCManager(ctrl)
	mockGA := mocks.NewMockGasAnalyzer(ctrl)
	mockPool := mocks.NewMockPendingPool(ctrl)

	validHash := hashAt(0)
	errHash := hashAt(1)
	validTx := newTestTx(10)

	hashes := []common.Hash{validHash, errHash}
	plan := fetchPlan{
		txs:  map[common.Hash]*types.Transaction{validHash: validTx},
		errs: map[common.Hash]error{errHash: errors.New("individual rpc error")},
	}

	mockRPC.EXPECT().
		FetchBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ rpcmanager.RPCCost, elems []rpc.BatchElem) error {
			applyFetchPlan(elems, plan)
			return nil
		}).
		Times(1)
	mockGA.EXPECT().GetCurrentBlockNumAndTime().Return(uint64(1), uint64(2)).AnyTimes()

	var pushed []*types.Transaction
	mockPool.EXPECT().
		PushBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(txs []*types.Transaction, _, _ uint64) { pushed = append(pushed, txs...) }).
		Times(1)

	p := &Process{rpcManager: mockRPC, gasanalyzer: mockGA, pendingPool: mockPool}
	p.GetTxInfo(context.Background(), hashes)

	assertPushed(t, pushed, []*types.Transaction{validTx})
}

func TestGetTxInfo_NilTransactionResult_Filtered(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRPC := mocks.NewMockRPCManager(ctrl)
	mockGA := mocks.NewMockGasAnalyzer(ctrl)
	mockPool := mocks.NewMockPendingPool(ctrl)

	validHash := hashAt(0)
	nilHash := hashAt(1)
	validTx := newTestTx(10)

	hashes := []common.Hash{validHash, nilHash}
	// nilHash is absent from txs -> its Result stays nil
	plan := fetchPlan{txs: map[common.Hash]*types.Transaction{validHash: validTx}}

	mockRPC.EXPECT().
		FetchBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ rpcmanager.RPCCost, elems []rpc.BatchElem) error {
			applyFetchPlan(elems, plan)
			return nil
		}).
		Times(1)
	mockGA.EXPECT().GetCurrentBlockNumAndTime().Return(uint64(1), uint64(2)).AnyTimes()

	var pushed []*types.Transaction
	mockPool.EXPECT().
		PushBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(txs []*types.Transaction, _, _ uint64) { pushed = append(pushed, txs...) }).
		Times(1)

	p := &Process{rpcManager: mockRPC, gasanalyzer: mockGA, pendingPool: mockPool}
	p.GetTxInfo(context.Background(), hashes)

	assertPushed(t, pushed, []*types.Transaction{validTx})
}

func TestGetTxInfo_ValidTransactions_Pushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRPC := mocks.NewMockRPCManager(ctrl)
	mockGA := mocks.NewMockGasAnalyzer(ctrl)
	mockPool := mocks.NewMockPendingPool(ctrl)

	hashes := []common.Hash{hashAt(0), hashAt(1), hashAt(2)}
	plan := fetchPlan{txs: make(map[common.Hash]*types.Transaction, len(hashes))}
	want := make([]*types.Transaction, 0, len(hashes))
	for i, h := range hashes {
		tx := newTestTx(uint64(i))
		plan.txs[h] = tx
		want = append(want, tx)
	}

	mockRPC.EXPECT().
		FetchBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ rpcmanager.RPCCost, elems []rpc.BatchElem) error {
			applyFetchPlan(elems, plan)
			return nil
		}).
		Times(1)
	mockGA.EXPECT().GetCurrentBlockNumAndTime().Return(uint64(100), uint64(200)).AnyTimes()

	var pushed []*types.Transaction
	var pushedBlockNum, pushedBlockTime uint64
	mockPool.EXPECT().
		PushBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(txs []*types.Transaction, blockNum, blockTime uint64) {
			pushed = append(pushed, txs...)
			pushedBlockNum = blockNum
			pushedBlockTime = blockTime
		}).
		Times(1)

	p := &Process{rpcManager: mockRPC, gasanalyzer: mockGA, pendingPool: mockPool}
	p.GetTxInfo(context.Background(), hashes)

	assertPushed(t, pushed, want)
	if pushedBlockNum != 100 || pushedBlockTime != 200 {
		t.Fatalf("PushBatch block num/time = (%d, %d), want (100, 200)", pushedBlockNum, pushedBlockTime)
	}
}

func TestGetTxInfo_PartialSuccess_OnlyValidPushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockRPC := mocks.NewMockRPCManager(ctrl)
	mockGA := mocks.NewMockGasAnalyzer(ctrl)
	mockPool := mocks.NewMockPendingPool(ctrl)

	// 8 hashes: mix of valid, individual error, and nil result.
	validIdx := []int{0, 2, 4, 7}
	errIdx := []int{1, 5}

	hashes := make([]common.Hash, 8)
	plan := fetchPlan{
		txs:  make(map[common.Hash]*types.Transaction),
		errs: make(map[common.Hash]error),
	}
	want := make([]*types.Transaction, 0, len(validIdx))

	for i := range hashes {
		hashes[i] = hashAt(i)
	}
	for _, i := range validIdx {
		tx := newTestTx(uint64(i))
		plan.txs[hashes[i]] = tx
		want = append(want, tx)
	}
	for _, i := range errIdx {
		plan.errs[hashes[i]] = errors.New("individual rpc error")
	}
	// hashes at indices 3 and 6 are absent from both maps -> nil results

	mockRPC.EXPECT().
		FetchBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ rpcmanager.RPCCost, elems []rpc.BatchElem) error {
			applyFetchPlan(elems, plan)
			return nil
		}).
		Times(1)
	mockGA.EXPECT().GetCurrentBlockNumAndTime().Return(uint64(1), uint64(2)).AnyTimes()

	var pushed []*types.Transaction
	mockPool.EXPECT().
		PushBatch(gomock.Any(), gomock.Any(), gomock.Any()).
		Do(func(txs []*types.Transaction, _, _ uint64) { pushed = append(pushed, txs...) }).
		Times(1)

	p := &Process{rpcManager: mockRPC, gasanalyzer: mockGA, pendingPool: mockPool}
	p.GetTxInfo(context.Background(), hashes)

	assertPushed(t, pushed, want)
}

// --- CalculateBlockTxTip ---------------------------------------------------

func TestCalculateBlockTxTip(t *testing.T) {
	header := &types.Header{Number: big.NewInt(100), BaseFee: big.NewInt(10), GasLimit: 30_000_000}
	validHash := common.HexToHash("0x01")
	lowHash := common.HexToHash("0x02")

	validReceipt := &types.Receipt{TxHash: validHash, EffectiveGasPrice: big.NewInt(30), GasUsed: 7_500_000}
	lowReceipt := &types.Receipt{TxHash: lowHash, EffectiveGasPrice: big.NewInt(5), GasUsed: 1000}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGA := mocks.NewMockGasAnalyzer(ctrl)

	// 유효한 receipt: tip 계산 + 가중치 계산 호출
	mockGA.EXPECT().
		EffectiveTipFromReceipt(validReceipt.EffectiveGasPrice, header.BaseFee).
		Return(uint64(20), true)
	mockGA.EXPECT().
		CalculateWeightForGasUsed(validReceipt.GasUsed, header.GasLimit).
		Return(0.5)
	// baseFee 이하인 receipt: tip 없음(ok=false)
	mockGA.EXPECT().
		EffectiveTipFromReceipt(lowReceipt.EffectiveGasPrice, header.BaseFee).
		Return(uint64(0), false)

	p := &Process{gasanalyzer: mockGA}
	got := p.CalculateBlockTxTip(header, []*types.Receipt{nil, validReceipt, lowReceipt})

	if got.Number != 100 || got.GasLimit != 30_000_000 {
		t.Fatalf("block meta = (%d, %d), want (100, 30000000)", got.Number, got.GasLimit)
	}
	if len(got.Txs) != 1 {
		t.Fatalf("txs len = %d, want 1", len(got.Txs))
	}
	want := blockstore.TxInfo{Hash: validHash.Hex(), Tip: 20, GasUsed: 7_500_000, GasWeight: 0.5}
	if got.Txs[0] != want {
		t.Fatalf("tx[0] = %+v, want %+v", got.Txs[0], want)
	}
}

// --- SendFeeBucketsToGrpc --------------------------------------------------

func TestSendFeeBucketsToGrpc(t *testing.T) {
	store := blockstore.NewBlockStore(10)

	store.AddBlock(blockstore.BlockData{
		FeeBuckets: []blockstore.FeeBucketStat{
			{
				Bucket:           2,
				TxCount:          10,
				TotalWaitBlocks:  20,
				TotalWaitSeconds: 100,
			},
			{
				Bucket:           1,
				TxCount:          5,
				TotalWaitBlocks:  5,
				TotalWaitSeconds: 20,
			},
		},
	})

	grpcClient := &grpcClient.GasPredictionClient{
		FeeBucketCh: make(chan *pb.FeeStatisticsRequest, 1),
	}

	p := &Process{
		blockstore: store,
		grpcClient: grpcClient,
	}

	p.SendFeeBucketsToGrpc()

	select {
	case req := <-grpcClient.FeeBucketCh:
		if req == nil {
			t.Fatal("expected request, got nil")
		}

		if len(req.Buckets) != 2 {
			t.Fatalf("expected 2 buckets, got %d", len(req.Buckets))
		}

		// Bucket 순으로 정렬되었는지 확인
		if req.Buckets[0].Bucket != 1 {
			t.Errorf("expected first bucket = 1, got %d",
				req.Buckets[0].Bucket)
		}

		if req.Buckets[1].Bucket != 2 {
			t.Errorf("expected second bucket = 2, got %d",
				req.Buckets[1].Bucket)
		}

	default:
		t.Fatal("expected FeeStatisticsRequest")
	}
}

// --- aggregateFeeBucket / convertFeeBucketToProto ---------------------------

func TestAggregateFeeBucket(t *testing.T) {
	tests := []struct {
		name      string
		blockData []blockstore.BlockData
		want      map[uint32]*blockstore.FeeBucketStat
	}{
		{
			name:      "empty input",
			blockData: nil,
			want:      map[uint32]*blockstore.FeeBucketStat{},
		},
		{
			name:      "block without fee buckets skipped",
			blockData: []blockstore.BlockData{{Number: 1}},
			want:      map[uint32]*blockstore.FeeBucketStat{},
		},
		{
			name: "aggregates across blocks",
			blockData: []blockstore.BlockData{
				{Number: 1, FeeBuckets: []blockstore.FeeBucketStat{{Bucket: 3, TxCount: 5, TotalWaitBlocks: 10, TotalWaitSeconds: 20}}},
				{Number: 2, FeeBuckets: []blockstore.FeeBucketStat{{Bucket: 3, TxCount: 2, TotalWaitBlocks: 4, TotalWaitSeconds: 6}}},
			},
			want: map[uint32]*blockstore.FeeBucketStat{
				3: {Bucket: 3, TxCount: 7, TotalWaitBlocks: 14, TotalWaitSeconds: 26},
			},
		},
		{
			name: "wait block count aggregated",
			blockData: []blockstore.BlockData{
				{FeeBuckets: []blockstore.FeeBucketStat{{Bucket: 1, WaitBlockCount: [16]uint32{1, 2, 3}}}},
				{FeeBuckets: []blockstore.FeeBucketStat{{Bucket: 1, WaitBlockCount: [16]uint32{4, 5, 6}}}},
			},
			want: map[uint32]*blockstore.FeeBucketStat{
				1: {Bucket: 1, WaitBlockCount: [16]uint32{5, 7, 9}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := aggregateFeeBucket(tt.blockData)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for bucket, wantStat := range tt.want {
				gotStat, ok := got[bucket]
				if !ok {
					t.Fatalf("bucket %d not found", bucket)
				}
				if gotStat.Bucket != wantStat.Bucket ||
					gotStat.TxCount != wantStat.TxCount ||
					gotStat.TotalWaitBlocks != wantStat.TotalWaitBlocks ||
					gotStat.TotalWaitSeconds != wantStat.TotalWaitSeconds ||
					gotStat.WaitBlockCount != wantStat.WaitBlockCount {
					t.Fatalf("bucket %d = %+v, want %+v", bucket, gotStat, wantStat)
				}
			}
		})
	}
}

func TestConvertFeeBucketToProto(t *testing.T) {
	tests := []struct {
		name  string
		stats map[uint32]*blockstore.FeeBucketStat
		want  []*pb.FeeBucket
	}{
		{
			name:  "empty stats",
			stats: map[uint32]*blockstore.FeeBucketStat{},
			want:  []*pb.FeeBucket{},
		},
		{
			name:  "zero tx count skipped",
			stats: map[uint32]*blockstore.FeeBucketStat{1: {Bucket: 1, TxCount: 0}},
			want:  []*pb.FeeBucket{},
		},
		{
			name: "single bucket converted",
			stats: map[uint32]*blockstore.FeeBucketStat{
				2: {Bucket: 2, TxCount: 4, TotalWaitBlocks: 8, TotalWaitSeconds: 16, WaitBlockCount: [16]uint32{0, 4}},
			},
			want: []*pb.FeeBucket{
				{
					Bucket:         2,
					MinPriority:    1.0,
					MaxPriority:    1.5,
					TotalTxCount:   4,
					AvrWaitBlocks:  2.0,
					AvrWaitSeconds: 4.0,
					WaitBlockCount: []*pb.WaitBlockCount{
						{WaitBlock: 1, TxCount: 4},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := convertFeeBucketToProto(tt.stats)
			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i, wantBucket := range tt.want {
				gotBucket := got[i]
				if gotBucket.Bucket != wantBucket.Bucket ||
					gotBucket.MinPriority != wantBucket.MinPriority ||
					gotBucket.MaxPriority != wantBucket.MaxPriority ||
					gotBucket.TotalTxCount != wantBucket.TotalTxCount ||
					gotBucket.AvrWaitBlocks != wantBucket.AvrWaitBlocks ||
					gotBucket.AvrWaitSeconds != wantBucket.AvrWaitSeconds {
					t.Fatalf("bucket[%d] = %+v, want %+v", i, gotBucket, wantBucket)
				}
				if len(gotBucket.WaitBlockCount) != len(wantBucket.WaitBlockCount) {
					t.Fatalf("bucket[%d] wait block count len = %d, want %d", i, len(gotBucket.WaitBlockCount), len(wantBucket.WaitBlockCount))
				}
				for j, wantWb := range wantBucket.WaitBlockCount {
					gotWb := gotBucket.WaitBlockCount[j]
					if gotWb.WaitBlock != wantWb.WaitBlock || gotWb.TxCount != wantWb.TxCount {
						t.Fatalf("bucket[%d].WaitBlockCount[%d] = %+v, want %+v", i, j, gotWb, wantWb)
					}
				}
			}
		})
	}
}

// --- ClearMempoolToReceipts ------------------------------------------------

func TestClearMempoolToReceipts_EmptyReceipts(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPool := mocks.NewMockPendingPool(ctrl) // no expectation -> RemoveByReceipts must not be called

	p := &Process{pendingPool: mockPool}
	got := p.ClearMempoolToReceipts(context.Background(), &types.Header{Number: big.NewInt(100)}, nil)

	if len(got) != 0 {
		t.Fatalf("fee buckets len = %d, want 0", len(got))
	}
}

func TestClearMempoolToReceipts_ReturnsFeeBuckets(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPool := mocks.NewMockPendingPool(ctrl)

	header := &types.Header{Number: big.NewInt(100)}
	receipts := []*types.Receipt{{TxHash: common.HexToHash("0x01")}}
	want := []blockstore.FeeBucketStat{{Bucket: 1, TxCount: 3}}

	mockPool.EXPECT().
		RemoveByReceipts(header, receipts).
		Return(want, 3).
		Times(1)

	p := &Process{pendingPool: mockPool}
	got := p.ClearMempoolToReceipts(context.Background(), header, receipts)

	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("fee buckets = %+v, want %+v", got, want)
	}
}

// --- ProcessBlock ----------------------------------------------------------

func TestProcessBlock_NilHeader(t *testing.T) {
	p := &Process{} // early return before any dependency call

	err := p.ProcessBlock(context.Background(), nil)
	if !errors.Is(err, ErrLatestBlockHeaderNil) {
		t.Fatalf("error = %v, want ErrLatestBlockHeaderNil", err)
	}
}
