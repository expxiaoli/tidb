// Copyright 2024 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl/mock"
	"github.com/pingcap/tidb/pkg/ddl/session"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"go.uber.org/mock/gomock"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func reduceIntervals(t testing.TB) {
	loopRetryIntBak := schedulerLoopRetryInterval
	schedulerLoopRetryInterval = 10 * time.Millisecond
	t.Cleanup(func() {
		schedulerLoopRetryInterval = loopRetryIntBak
	})
}

func TestMustReloadSchemas(t *testing.T) {
	reduceIntervals(t)
	assertReady := func(t *testing.T, ch <-chan struct{}, expected bool) {
		t.Helper()
		select {
		case <-ch:
			require.True(t, expected)
		default:
			require.False(t, expected)
		}
	}
	newScheduler := func(t *testing.T) (*jobScheduler, *mock.MockSchemaLoader, context.CancelFunc) {
		ctrl := gomock.NewController(t)
		loader := mock.NewMockSchemaLoader(ctrl)
		ctx, cancel := context.WithCancel(context.Background())
		return &jobScheduler{
			schCtx:                        ctx,
			schemaLoader:                  loader,
			storageClassTransitionReadyCh: make(chan struct{}),
		}, loader, cancel
	}

	t.Run("direct success", func(t *testing.T) {
		sch, loader, cancel := newScheduler(t)
		defer cancel()
		loader.EXPECT().Reload().Return(nil)
		sch.mustReloadSchemas()
		assertReady(t, sch.storageClassTransitionReadyCh, true)
	})

	t.Run("success after retry", func(t *testing.T) {
		sch, loader, cancel := newScheduler(t)
		defer cancel()
		loader.EXPECT().Reload().Return(errors.New("mock err"))
		loader.EXPECT().Reload().Return(nil)
		sch.mustReloadSchemas()
		assertReady(t, sch.storageClassTransitionReadyCh, true)
	})

	t.Run("cancelled reload does not make poller ready", func(t *testing.T) {
		sch, loader, cancel := newScheduler(t)
		loader.EXPECT().Reload().DoAndReturn(func() error {
			cancel()
			return errors.New("mock err")
		})
		sch.mustReloadSchemas()
		assertReady(t, sch.storageClassTransitionReadyCh, false)
	})
}

func TestUnSyncedJobTracker(t *testing.T) {
	jt := newUnSyncedJobTracker()
	jt.addUnSynced(1)
	require.True(t, jt.isUnSynced(1))
	jt.removeUnSynced(1)
	require.False(t, jt.isUnSynced(1))
}

func TestDDLJobRuntimeLog(t *testing.T) {
	now := time.Unix(2000, 0)
	for _, tc := range []struct {
		name    string
		state   model.JobState
		age     time.Duration
		noStart bool
		otherTp bool
		wantLog bool
	}{
		{name: "running", state: model.JobStateRunning, age: 601 * time.Second, wantLog: true},
		{name: "queueing", state: model.JobStateQueueing, age: 20 * time.Minute, wantLog: true},
		{name: "threshold", state: model.JobStateRunning, age: 10 * time.Minute},
		{name: "below threshold", state: model.JobStateQueueing, age: 599 * time.Second},
		{name: "future start", state: model.JobStateRunning, age: -time.Minute},
		{name: "missing start", state: model.JobStateRunning, age: 20 * time.Minute, noStart: true},
		{name: "other type", state: model.JobStateRunning, age: 20 * time.Minute, otherTp: true},
		{name: "paused", state: model.JobStatePaused, age: 20 * time.Minute},
		{name: "pausing", state: model.JobStatePausing, age: 20 * time.Minute},
		{name: "cancelling", state: model.JobStateCancelling, age: 20 * time.Minute},
		{name: "rolling back", state: model.JobStateRollingback, age: 20 * time.Minute},
		{name: "done", state: model.JobStateDone, age: 20 * time.Minute},
		{name: "synced", state: model.JobStateSynced, age: 20 * time.Minute},
		{name: "cancelled", state: model.JobStateCancelled, age: 20 * time.Minute},
		{name: "rollback done", state: model.JobStateRollbackDone, age: 20 * time.Minute},
		{name: "none", state: model.JobStateNone, age: 20 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := &model.Job{ID: 123, Type: model.ActionAddIndex, State: tc.state,
				StartTS: oracle.GoTimeToTS(now.Add(-tc.age)), RealStartTS: oracle.GoTimeToTS(now),
				Query: "SQL containing sensitive data must not be logged"}
			if tc.noStart {
				job.StartTS = 0
			}
			if tc.otherTp {
				job.Type = model.ActionCreateTable
			}
			var buf bytes.Buffer
			logger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
				zapcore.AddSync(&buf), zap.InfoLevel))
			logLongRunningAddIndexJob(logger, job, now)
			if !tc.wantLog {
				require.Empty(t, buf.String())
				return
			}
			var entry map[string]any
			require.NoError(t, json.Unmarshal(buf.Bytes(), &entry))
			require.Equal(t, "info", entry["level"])
			require.Equal(t, "DDL job runtime", entry["msg"])
			require.Equal(t, "ddl_job_runtime", entry["event"])
			require.Equal(t, float64(123), entry["job_id"])
			require.Equal(t, "add index", entry["job_type"])
			require.Equal(t, tc.state.String(), entry["job_state"])
			require.Equal(t, tc.age.Seconds(), entry["duration_seconds"])
			require.NotContains(t, buf.String(), job.Query)
		})
	}
}

func TestDDLJobRuntimeLogLoopCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A retired Owner must not borrow a session or keep a timer loop alive.
	(&jobScheduler{schCtx: ctx}).logDDLJobRuntimeLoop()
}

// NewDDLJobRuntimeLoggerForTest exposes the metadata scan to mock-storage tests.
func NewDDLJobRuntimeLoggerForTest(pool *session.Pool) func(context.Context) error {
	return (&jobScheduler{sessPool: pool}).logDDLJobRuntime
}
