package coordinator

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/logger"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/processor"
	"github.com/ethereum/go-ethereum/core/types"
)

const (
	headBufferSize        = 100
	failedBlockBufferSize = 10
	retryCount            = 3
	retryDelay            = 300 * time.Millisecond
	wakeupBufferSize      = 1
)

//go:generate mockgen -destination=mocks/mock_block_processor.go -package=mocks . BlockProcessor
type BlockProcessor interface {
	ProcessBlock(ctx context.Context, header *types.Header) error
	Initialize(ctx context.Context) (*types.Header, error)
	HeaderByNumber(ctx context.Context, number uint64) (*types.Header, error)
	CleanupFailedBlock(ctx context.Context, header *types.Header) error
	Resync(ctx context.Context) (*types.Header, error)
	CompareFeeHistory(ctx context.Context)
}

type blockPipeline struct {
	proc            BlockProcessor
	headerChan      chan *types.Header
	failedBlockChan chan *types.Header
	wakeupChan      chan struct{}
	maxBlockLag     uint64

	nextBlockNum uint64
	pendingBlock map[uint64]*types.Header

	wg sync.WaitGroup
	mu sync.Mutex
}

func newBlockPipeline(proc *processor.Process, maxBlockCount int) *blockPipeline {
	return &blockPipeline{
		headerChan:      make(chan *types.Header, headBufferSize),
		failedBlockChan: make(chan *types.Header, failedBlockBufferSize),
		wakeupChan:      make(chan struct{}, wakeupBufferSize),
		pendingBlock:    make(map[uint64]*types.Header),
		proc:            proc,
		maxBlockLag:     uint64(maxBlockCount),
	}
}

func (b *blockPipeline) stop() {
	b.wg.Wait()
}

func (b *blockPipeline) push(header *types.Header) {
	select {
	case b.headerChan <- header:
	default:
		logger.Warn(context.Background(), "headerChan is full, and drop header",
			slog.String("system", "blockpipeline"),
		)
	}
}

func (b *blockPipeline) start(ctx context.Context) {
	header, err := b.proc.Initialize(ctx)
	if err != nil {
		logger.Error(ctx, "failed to initialize block info",
			err,
			slog.String("system", "blockpipeline"))
		panic(err)
	}

	b.nextBlockNum = header.Number.Uint64() + 1
	b.wg.Add(2)

	go func() {
		defer b.wg.Done()
		b.blockProcLoop(ctx)
	}()

	go func() {
		defer b.wg.Done()
		b.failBlockLoop(ctx)
	}()
}

func (b *blockPipeline) blockProcLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case <-b.wakeupChan:
			b.procAvailable(ctx)

		case header, ok := <-b.headerChan:
			if !ok {
				return
			}

			//feehistory 분석
			b.proc.CompareFeeHistory(ctx)

			b.handleHeader(ctx, header)
		}
	}
}

func (b *blockPipeline) handleHeader(ctx context.Context, header *types.Header) {
	if header == nil || header.Number == nil {
		return
	}
	blockNum := header.Number.Uint64()

	b.mu.Lock()
	if b.nextBlockNum > blockNum {
		b.mu.Unlock()
		return
	}

	if _, exists := b.pendingBlock[blockNum]; exists {
		b.mu.Unlock()
		return
	}

	b.pendingBlock[blockNum] = header

	b.mu.Unlock()

	b.procAvailable(ctx)
}

func (b *blockPipeline) procAvailable(ctx context.Context) {
	for {
		b.mu.Lock()
		blockNum := b.nextBlockNum
		header, ok := b.pendingBlock[blockNum]
		if !ok {
			b.mu.Unlock()

			//처리할 block이 더이상 없는지, 오류로 뻐진 것인지 확인
			b.checkBlockGap(ctx)
			return
		}

		b.mu.Unlock()

		err := b.procWithRetry(ctx, header)
		if err != nil {
			//Reorg 확인
			latest, stale, e := b.checkStale(ctx, header)
			if e == nil && stale {
				b.mu.Lock()
				b.pendingBlock[blockNum] = latest
				b.mu.Unlock()

				logger.Info(ctx, "reorg detected",
					slog.Uint64("block", blockNum),
					slog.String("old_hash", header.Hash().Hex()),
					slog.String("new_hash", latest.Hash().Hex()),
				)
				//재처리
				continue
			}

			b.failedBlock(header)
			logger.Error(ctx, "block processing failed after retries",
				err,
				slog.String("system", "blockpipeline"),
				slog.Uint64("block", header.Number.Uint64()),
			)
		}

		b.mu.Lock()
		delete(b.pendingBlock, blockNum)
		b.nextBlockNum++
		b.mu.Unlock()
	}
}

func (b *blockPipeline) procWithRetry(ctx context.Context, header *types.Header) error {
	var err error
	for i := 0; i < retryCount; i++ {
		err = b.proc.ProcessBlock(ctx, header)
		if err == nil {
			return nil
		}

		if i < retryCount-1 {
			timer := time.NewTimer(retryDelay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
	}

	return err
}

func (b *blockPipeline) checkBlockGap(ctx context.Context) {
	b.mu.Lock()
	nextBlock := b.nextBlockNum

	var highest uint64

	for num := range b.pendingBlock {
		if num > highest {
			highest = num
		}
	}

	b.mu.Unlock()

	// 실행할 블록이 없음
	if highest <= nextBlock {
		return
	}

	gap := highest - nextBlock

	//블록이 너무 많이 밀렸을 때 block과 pending tx 정보를 정리
	if gap >= b.maxBlockLag {
		logger.Warn(ctx, "block lag exceeded maximum threshold, starting resync",
			slog.String("system", "blockpipeline"),
			slog.Uint64("gap", gap),
			slog.Uint64("next_block", nextBlock),
			slog.Uint64("latest_block", highest),
		)
		b.resync(ctx)
		return
	}

	for num := nextBlock; num < highest; num++ {
		b.mu.Lock()
		_, exists := b.pendingBlock[num]
		b.mu.Unlock()

		if exists {
			continue
		}

		//블록 헤더 정보 가져오기
		header, err := b.proc.HeaderByNumber(ctx, num)
		if err != nil {
			logger.Error(ctx, "failed to backfill block header",
				err,
				slog.String("system", "blockpipeline"),
				slog.Uint64("block_number", num),
			)
			// 오류 발생시 다음 header가 입력 될 때 다시 수행,
			// continue로 하면 handleBlockGap과 procAvailable가 무한 반복할 수 있음
			return
		}
		if header == nil || header.Number == nil {
			logger.Error(ctx, "backfilled block header is nil",
				errHeaderNotFound,
				slog.String("system", "blockpipeline"),
				slog.Uint64("block_number", num),
			)
			return
		}

		b.mu.Lock()
		b.pendingBlock[num] = header
		b.mu.Unlock()
	}

	//procAvailable을 다시 실행할 수 있도록 blockProcLoop에 신호를 보냄
	b.wakeup()
}

func (b *blockPipeline) wakeup() {
	select {
	case b.wakeupChan <- struct{}{}:
	default:
		logger.Warn(context.Background(), "wakeupChan is full",
			slog.String("system", "blockpipeline"),
		)
	}
}

func (b *blockPipeline) failedBlock(header *types.Header) {
	select {
	case b.failedBlockChan <- header:
	default:
		logger.Warn(context.Background(), "failedBlockChan is full",
			slog.String("system", "blockpipeline"),
			slog.Uint64("block_number", header.Number.Uint64()),
		)
	}
}

func (b *blockPipeline) failBlockLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return

		case header, ok := <-b.failedBlockChan:
			if !ok {
				return
			}
			b.handleFailedBlock(ctx, header)
		}
	}
}

func (b *blockPipeline) handleFailedBlock(ctx context.Context, header *types.Header) {
	if header == nil || header.Number == nil {
		return
	}

	latest, stale, err := b.checkStale(ctx, header)
	if err != nil {
		logger.Error(ctx, "failed to check stale block",
			err,
			slog.String("system", "blockpipeline"),
			slog.String("block_hash", header.Hash().Hex()),
		)
		return
	}

	if stale {
		logger.Info(ctx, "reorg detected",
			slog.Uint64("block", latest.Number.Uint64()),
			slog.String("old_hash", header.Hash().Hex()),
			slog.String("new_hash", latest.Hash().Hex()),
		)

		header = latest
	}

	blockNum := header.Number.Uint64()

	//pending tx 정리
	if err := b.proc.CleanupFailedBlock(ctx, header); err != nil {
		logger.Error(ctx, "failed to cleanup failed block",
			err,
			slog.String("system", "blockpipeline"),
			slog.Uint64("block_number", blockNum),
		)

		return
	}

	logger.Info(ctx, "failed block skipped after cleanup",
		slog.String("system", "blockpipeline"),
		slog.Uint64("block_number", blockNum),
	)
}

func (b *blockPipeline) checkStale(ctx context.Context, header *types.Header) (*types.Header, bool, error) {
	if header == nil || header.Number == nil {
		return nil, false, errHeaderNotFound
	}

	latest, err := b.proc.HeaderByNumber(ctx, header.Number.Uint64())
	if err != nil {
		return nil, false, err
	}
	if header == nil || header.Number == nil {
		return nil, false, errHeaderNotFound
	}

	if latest.Hash() != header.Hash() {
		return latest, true, nil // Reorg 발생
	}

	return latest, false, nil // 동일한 블록
}

func (b *blockPipeline) resync(ctx context.Context) {
	latest, err := b.proc.Resync(ctx)
	if err != nil {
		logger.Error(ctx, "failed to resync processor",
			err,
			slog.String("system", "blockpipeline"))
		return
	}

	if latest == nil || latest.Number == nil {
		return
	}

	b.mu.Lock()
	oldBlockNum := b.nextBlockNum
	b.pendingBlock = make(map[uint64]*types.Header)
	b.nextBlockNum = latest.Number.Uint64() + 1
	b.mu.Unlock()

	logger.Info(ctx, "block pipeline resynchronized",
		slog.String("system", "blockpipeline"),
		slog.Uint64("old_next_block", oldBlockNum),
		slog.Uint64("latest_block", latest.Number.Uint64()),
		slog.Uint64("new_next_block", latest.Number.Uint64()+1),
	)
}
