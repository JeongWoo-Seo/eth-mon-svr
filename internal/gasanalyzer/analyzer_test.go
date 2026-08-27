package gasanalyzer

import (
	"testing"
	"time"

	"github.com/JeongWoo-Seo/eth-mon-svr/internal/gasanalyzer/mocks"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/mempool"
	"github.com/JeongWoo-Seo/eth-mon-svr/internal/pb"
	"go.uber.org/mock/gomock"
)

// --- collectPendingTx ------------------------------------------------------

func TestCollectPendingTx(t *testing.T) {
	tests := []struct {
		name          string
		txs           []mempool.PendingTx
		nextBaseFee   uint64
		blockGasLimit uint64
		want          []WeightedTip
	}{
		{
			name:        "feeCap below nextBaseFee skipped",
			txs:         []mempool.PendingTx{{FeeCap: 90, TipCap: 10, GasLimit: 250}},
			nextBaseFee: 100, blockGasLimit: 1000,
			want: []WeightedTip{},
		},
		{
			name:        "feeCap equal nextBaseFee skipped",
			txs:         []mempool.PendingTx{{FeeCap: 100, TipCap: 10, GasLimit: 250}},
			nextBaseFee: 100, blockGasLimit: 1000,
			want: []WeightedTip{},
		},
		{
			name:        "valid tx included",
			txs:         []mempool.PendingTx{{FeeCap: 150, TipCap: 20, GasLimit: 250}},
			nextBaseFee: 100, blockGasLimit: 1000,
			want: []WeightedTip{{Tip: 20, Weight: 0.5}},
		},
		{
			name:        "tip capped by diff",
			txs:         []mempool.PendingTx{{FeeCap: 130, TipCap: 50, GasLimit: 250}},
			nextBaseFee: 100, blockGasLimit: 1000,
			want: []WeightedTip{{Tip: 30, Weight: 0.5}},
		},
		{
			name:        "nonce gap halves weight",
			txs:         []mempool.PendingTx{{FeeCap: 150, TipCap: 20, GasLimit: 250, NonceGap: true}},
			nextBaseFee: 100, blockGasLimit: 1000,
			want: []WeightedTip{{Tip: 20, Weight: 0.25}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			mockPool := mocks.NewMockPendingPool(ctrl)
			mockPool.EXPECT().Snapshot().Return(tt.txs).Times(1)

			a := &Analyzer{pendingPool: mockPool}
			got := a.collectPendingTx(0, tt.nextBaseFee, tt.blockGasLimit, 0)

			if len(got) != len(tt.want) {
				t.Fatalf("len = %d, want %d", len(got), len(tt.want))
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("pool[%d] = %+v, want %+v", i, got[i], tt.want[i])
				}
			}
		})
	}
}

// --- sendResultToGRPC ------------------------------------------------------

func TestSendResultToGRPC(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockGC := mocks.NewMockGasPredictionClient(ctrl)

	var got *pb.GasPredictionStream
	mockGC.EXPECT().
		GasPredictResultSend(gomock.Any()).
		Do(func(req *pb.GasPredictionStream) { got = req }).
		Times(1)

	result := &GasPrediction{
		NextBlockNumber: 100,
		NextBaseFee:     50,
		PredictResult: map[string]GasLevel{
			"market": {PriorityFee: 10, MaxFee: 60},
		},
		UpdatedAt: time.Now(),
	}

	a := &Analyzer{grpcClient: mockGC}
	a.sendResultToGRPC(result)

	pred := got.GetPrediction()
	if pred == nil {
		t.Fatal("expected prediction event")
	}
	if pred.NextBlockNumber != 100 || pred.NextBaseFee != 50 {
		t.Fatalf("prediction = (%d, %d), want (100, 50)", pred.NextBlockNumber, pred.NextBaseFee)
	}
	if got := pred.PredictResult["market"]; got == nil || got.PriorityFee != 10 || got.MaxFee != 60 {
		t.Fatalf("market gas level = %+v, want PriorityFee=10 MaxFee=60", got)
	}
}

// --- AnalyzeGasPrice -------------------------------------------------------
func TestAnalyzeGasPrice_Ready_SendsPrediction(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockPool := mocks.NewMockPendingPool(ctrl)
	mockGC := mocks.NewMockGasPredictionClient(ctrl)

	mockPool.EXPECT().
		Snapshot().
		Return([]mempool.PendingTx{{FeeCap: 150, TipCap: 20, GasLimit: 250}}).
		Times(1)

	var got *pb.GasPredictionStream
	mockGC.EXPECT().
		GasPredictResultSend(gomock.Any()).
		Do(func(req *pb.GasPredictionStream) { got = req }).
		Times(1)

	a := &Analyzer{
		ready: true,
		latestBlockData: BlockAnalysisData{
			BlockNumber: 99,
			BaseFee:     40,
			NextBaseFee: 50,
			GasLimit:    1000,
		},
		pendingPool: mockPool,
		grpcClient:  mockGC,
	}

	a.AnalyzeGasPrice()

	pred := got.GetPrediction()
	if pred == nil {
		t.Fatal("expected prediction event")
	}
	if pred.NextBlockNumber != 100 || pred.NextBaseFee != 50 {
		t.Fatalf("prediction = (%d, %d), want (100, 50)", pred.NextBlockNumber, pred.NextBaseFee)
	}
}

// --- UpdateResult ----------------------------------------------------------

func TestUpdateResult_Blend(t *testing.T) {
	a := &Analyzer{
		latestResult: GasPrediction{
			AnalyzerBlock: map[string]GasLevel{
				"market": {PriorityFee: 10},
			},
			AnalyzerPending: map[string]GasLevel{
				"market": {PriorityFee: 20},
			},
		},
	}

	result := a.UpdateResult(100, 40, 50)

	if result.NextBlockNumber != 100 || result.NextBaseFee != 50 {
		t.Fatalf("prediction = (%d, %d), want (100, 50)", result.NextBlockNumber, result.NextBaseFee)
	}
	market := result.PredictResult["market"]
	// blend = 0.2*10 + 0.8*20 = 18, MaxFee = NextBaseFee + PriorityFee = 68
	if market.PriorityFee != 18 || market.MaxFee != 68 {
		t.Fatalf("market = %+v, want PriorityFee=18 MaxFee=68", market)
	}
}

// --- percentiles (empty -> default) ----------------------------------------

func TestPendingPercentiles_Empty(t *testing.T) {
	a := &Analyzer{}
	result := a.PendingPercentiles(nil)

	if result["market"] != 750_000_000 {
		t.Fatalf("market = %d, want 750000000", result["market"])
	}
}

func TestBlockPercentiles_Empty(t *testing.T) {
	a := &Analyzer{}
	result, cutoff := a.BlockPercentiles(nil)

	if result["market"] != 750_000_000 {
		t.Fatalf("market = %d, want 750000000", result["market"])
	}
	if cutoff != 1_000_000 {
		t.Fatalf("cutoff = %d, want 1000000", cutoff)
	}
}
