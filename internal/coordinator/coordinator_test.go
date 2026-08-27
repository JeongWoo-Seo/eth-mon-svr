package coordinator

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/coordinator/mocks"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"go.uber.org/mock/gomock"
)

func hashAt(i int) common.Hash {
	return common.HexToHash(fmt.Sprintf("0x%02x", i))
}

func headerAt(num uint64) *types.Header {
	return &types.Header{Number: new(big.Int).SetUint64(num)}
}

// --- blockPipeline: checkStale --------------------------------------------

func TestBlockPipeline_CheckStale(t *testing.T) {
	header := headerAt(5)
	other := &types.Header{Number: big.NewInt(5), GasLimit: 999} // 다른 hash

	tests := []struct {
		name      string
		latest    *types.Header
		err       error
		wantStale bool
		wantErr   bool
	}{
		{name: "same hash not stale", latest: header, wantStale: false},
		{name: "different hash reorg", latest: other, wantStale: true},
		{name: "rpc error", latest: nil, err: errors.New("rpc down"), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockProc := mocks.NewMockBlockProcessor(ctrl)
			mockProc.EXPECT().
				HeaderByNumber(gomock.Any(), uint64(5)).
				Return(tt.latest, tt.err).
				Times(1)

			b := &blockPipeline{proc: mockProc}
			latest, stale, err := b.checkStale(context.Background(), header)

			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if latest != nil {
					t.Fatalf("latest = %v, want nil", latest)
				}
				return
			}
			if stale != tt.wantStale {
				t.Fatalf("stale = %v, want %v", stale, tt.wantStale)
			}
			if latest != tt.latest {
				t.Fatalf("latest = %v, want %v", latest, tt.latest)
			}
		})
	}
}

// --- blockPipeline: resync -------------------------------------------------

func TestBlockPipeline_Resync(t *testing.T) {
	tests := []struct {
		name           string
		latest         *types.Header
		err            error
		wantNext       uint64
		wantPendingLen int
	}{
		{name: "success resets state", latest: headerAt(100), wantNext: 101, wantPendingLen: 0},
		{name: "error keeps state", latest: nil, err: errors.New("resync failed"), wantNext: 10, wantPendingLen: 1},
		{name: "nil latest keeps state", latest: nil, wantNext: 10, wantPendingLen: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockProc := mocks.NewMockBlockProcessor(ctrl)
			mockProc.EXPECT().Resync(gomock.Any()).Return(tt.latest, tt.err).Times(1)

			b := &blockPipeline{
				proc:         mockProc,
				nextBlockNum: 10,
				pendingBlock: map[uint64]*types.Header{5: headerAt(5)},
			}

			b.resync(context.Background())

			if b.nextBlockNum != tt.wantNext {
				t.Fatalf("nextBlockNum = %d, want %d", b.nextBlockNum, tt.wantNext)
			}
			if len(b.pendingBlock) != tt.wantPendingLen {
				t.Fatalf("pendingBlock len = %d, want %d", len(b.pendingBlock), tt.wantPendingLen)
			}
		})
	}
}

// --- blockPipeline: handleHeader -------------------------------------------

func TestBlockPipeline_HandleHeader(t *testing.T) {
	existingHeader := headerAt(5)

	tests := []struct {
		name         string
		header       *types.Header
		nextBlockNum uint64
		pendingBlock map[uint64]*types.Header

		wantNextBlockNum uint64
		wantPendingLen   int
		wantExisting     *types.Header
		wantProcess      bool
	}{
		{
			name:             "nil header",
			header:           nil,
			nextBlockNum:     5,
			pendingBlock:     map[uint64]*types.Header{},
			wantNextBlockNum: 5,
			wantPendingLen:   0,
			wantProcess:      false,
		},
		{
			name:             "nil number",
			header:           &types.Header{Number: nil},
			nextBlockNum:     5,
			pendingBlock:     map[uint64]*types.Header{},
			wantNextBlockNum: 5,
			wantPendingLen:   0,
			wantProcess:      false,
		},
		{
			name:             "stale block",
			header:           headerAt(3),
			nextBlockNum:     5,
			pendingBlock:     map[uint64]*types.Header{},
			wantNextBlockNum: 5,
			wantPendingLen:   0,
			wantProcess:      false,
		},
		{
			name:         "duplicate block",
			header:       headerAt(5),
			nextBlockNum: 5,
			pendingBlock: map[uint64]*types.Header{
				5: existingHeader,
			},
			wantNextBlockNum: 5,
			wantPendingLen:   1,
			wantExisting:     existingHeader,
			wantProcess:      false,
		},
		{
			name:             "new block",
			header:           headerAt(5),
			nextBlockNum:     5,
			pendingBlock:     map[uint64]*types.Header{},
			wantNextBlockNum: 6,
			wantPendingLen:   0,
			wantProcess:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var proc *mocks.MockBlockProcessor

			if tt.wantProcess {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()

				proc = mocks.NewMockBlockProcessor(ctrl)

				proc.EXPECT().
					ProcessBlock(gomock.Any(), tt.header).
					Return(nil).
					Times(1)
			}

			b := &blockPipeline{
				proc:         proc,
				nextBlockNum: tt.nextBlockNum,
				pendingBlock: tt.pendingBlock,
			}

			b.handleHeader(context.Background(), tt.header)

			if b.nextBlockNum != tt.wantNextBlockNum {
				t.Fatalf(
					"nextBlockNum = %d, want %d",
					b.nextBlockNum,
					tt.wantNextBlockNum,
				)
			}

			if len(b.pendingBlock) != tt.wantPendingLen {
				t.Fatalf(
					"pendingBlock len = %d, want %d",
					len(b.pendingBlock),
					tt.wantPendingLen,
				)
			}

			if tt.wantExisting != nil {
				got, exists := b.pendingBlock[5]

				if !exists {
					t.Fatal("expected block 5 to exist")
				}

				if got != tt.wantExisting {
					t.Fatal("existing block was replaced")
				}
			}
		})
	}
}

// --- blockPipeline: checkBlockGap ------------------------------------------

func TestBlockPipeline_CheckBlockGap_NoGap(t *testing.T) {
	b := &blockPipeline{
		nextBlockNum: 5,
		pendingBlock: map[uint64]*types.Header{},
		// proc nil; must not be called
	}

	b.checkBlockGap(context.Background()) // 정상종료 되는 경우 return 되어 panic이 발생하지 않음
}

func TestBlockPipeline_CheckBlockGap_LargeGapResync(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockProc := mocks.NewMockBlockProcessor(ctrl)
	mockProc.EXPECT().Resync(gomock.Any()).Return(headerAt(100), nil).Times(1)

	b := &blockPipeline{
		proc:         mockProc,
		nextBlockNum: 5,
		pendingBlock: map[uint64]*types.Header{20: headerAt(20)},
		maxBlockLag:  10,
	}

	b.checkBlockGap(context.Background())

	if b.nextBlockNum != 101 {
		t.Fatalf("nextBlockNum = %d, want 101", b.nextBlockNum)
	}
}

func TestBlockPipeline_CheckBlockGap_Backfill(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockProc := mocks.NewMockBlockProcessor(ctrl)

	mockProc.EXPECT().HeaderByNumber(gomock.Any(), uint64(5)).Return(headerAt(5), nil).Times(1)
	mockProc.EXPECT().HeaderByNumber(gomock.Any(), uint64(6)).Return(headerAt(6), nil).Times(1)

	b := &blockPipeline{
		proc:         mockProc,
		nextBlockNum: 5,
		pendingBlock: map[uint64]*types.Header{7: headerAt(7)},
		maxBlockLag:  10,
		wakeupChan:   make(chan struct{}, 1),
	}

	b.checkBlockGap(context.Background())

	if len(b.pendingBlock) != 3 {
		t.Fatalf("pendingBlock len = %d, want 3", len(b.pendingBlock))
	}
}

// --- pendingPipeline: worker ------------------------------------------------

func TestPendingPipeline_Worker_FlushesFullBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockProc := mocks.NewMockPendingProcessor(ctrl)

	p := &pendingPipeline{
		workers: 1,
		proc:    mockProc,
		jobs:    make(chan common.Hash, maxBatchSize),
	}

	flushed := make(chan int, 1)
	mockProc.EXPECT().
		GetTxInfo(gomock.Any(), gomock.Any()).
		Do(func(_ context.Context, hashes []common.Hash) { flushed <- len(hashes) }).
		Times(1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.wg.Add(1)
	go p.worker(ctx)

	for i := 0; i < maxBatchSize; i++ {
		p.jobs <- hashAt(i)
	}

	select {
	case n := <-flushed:
		if n != maxBatchSize {
			t.Fatalf("batch len = %d, want %d", n, maxBatchSize)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for flush")
	}

	cancel()
	p.wg.Wait()
}
