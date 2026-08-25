//go:build !integration

package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	svcmocks "github.com/Vladislav747/golang-project-order-system/internal/service/mocks"
	"github.com/Vladislav747/golang-project-order-system/internal/worker/mocks"
)

func TestOutboxCleaner_Clean_OK(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	repoOutbox, txManager, mockTx := createMocks(t)

	txManager.EXPECT().Begin(mock.Anything).Return(mockTx, nil)
	mockTx.EXPECT().Rollback(mock.Anything).Return(nil)
	mockTx.EXPECT().Commit(mock.Anything).Return(nil)

	repoOutbox.EXPECT().
		CleanOutboxMessages(mock.Anything, mockTx, 24*time.Hour, 3).
		Return(nil)

	cleaner := NewOutboxCleaner(
		repoOutbox,
		zap.NewNop(),
		time.Hour,    // interval — для clean не важен
		24*time.Hour, // должен совпасть с EXPECT
		3,            // maxAttempts — тоже
		txManager,
	)
	cleaner.clean(ctx)
}

func createMocks(t *testing.T) (*mocks.MockOutboxCleanerRepository, *mocks.MockTxManager, *svcmocks.MockTx) {

	repoOutbox := mocks.NewMockOutboxCleanerRepository(t)
	txManager := mocks.NewMockTxManager(t)
	mockTx := svcmocks.NewMockTx(t)
	return repoOutbox, txManager, mockTx
}
