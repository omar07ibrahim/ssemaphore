package admission

import (
	"context"
	"testing"
)

func TestSchedulerReportsValidatedIntegrationLimits(t *testing.T) {
	t.Parallel()
	config := baseConfig()
	wantGlobal := config.GlobalQueue
	wantTenant := config.Tenants[0].Queue
	scheduler, err := New(config)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := scheduler.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	config.MaxBodyBytes = 1
	config.MaxRequestUnits = 1
	config.GlobalQueue = QueueLimits{}
	config.Tenants[0].ID = 99
	config.Tenants[0].Queue = QueueLimits{}

	if got := scheduler.MaxBodyBytes(); got != 100 {
		t.Fatalf("MaxBodyBytes() = %d, want 100", got)
	}
	if got := scheduler.MaxRequestUnits(); got != 100 {
		t.Fatalf("MaxRequestUnits() = %d, want 100", got)
	}
	if got := scheduler.GlobalQueueLimits(); got != wantGlobal {
		t.Fatalf("GlobalQueueLimits() = %+v, want %+v", got, wantGlobal)
	}
	if got, ok := scheduler.TenantQueueLimits(1); !ok || got != wantTenant {
		t.Fatalf("TenantQueueLimits(1) = (%+v, %t), want (%+v, true)", got, ok, wantTenant)
	}
	if got, ok := scheduler.TenantQueueLimits(99); ok || got != (QueueLimits{}) {
		t.Fatalf("TenantQueueLimits(99) = (%+v, %t), want (zero, false)", got, ok)
	}
	if !scheduler.HasTenant(1) {
		t.Fatal("HasTenant(1) = false, want true")
	}
	if scheduler.HasTenant(0) || scheduler.HasTenant(99) {
		t.Fatal("HasTenant() accepted an unconfigured tenant")
	}

	copyOfGlobal := scheduler.GlobalQueueLimits()
	copyOfGlobal.Count = 0
	if got := scheduler.GlobalQueueLimits(); got != wantGlobal {
		t.Fatalf("GlobalQueueLimits() retained caller mutation: %+v", got)
	}
	copyOfTenant, _ := scheduler.TenantQueueLimits(1)
	copyOfTenant.Count = 0
	if got, _ := scheduler.TenantQueueLimits(1); got != wantTenant {
		t.Fatalf("TenantQueueLimits() retained caller mutation: %+v", got)
	}
}
