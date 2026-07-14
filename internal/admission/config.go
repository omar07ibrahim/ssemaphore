package admission

import (
	"errors"
	"fmt"
	"math/bits"
	"time"
)

const (
	absoluteMaxTenants          = 1024
	absoluteMaxQueuedRequests   = 65_536
	absoluteMaxInflightRequests = 4_096
	maxFundingVisits            = 65_536

	// MaximumQueueTimeout is the longest queue residence accepted by a Scheduler.
	MaximumQueueTimeout = time.Hour
)

type validatedConfig struct {
	maxBodyBytes    uint64
	maxRequestUnits uint64
	deficitCap      uint64
	globalQueue     QueueLimits
	globalInflight  InflightLimits
	tenants         []validatedTenant
	tenantByID      map[TenantID]int
}

type validatedTenant struct {
	id       TenantID
	quantum  uint64
	queue    QueueLimits
	inflight InflightLimits
}

func validateConfig(config Config) (validatedConfig, error) {
	if config.MaxBodyBytes == 0 {
		return validatedConfig{}, errors.New("maximum body bytes must be positive")
	}
	if config.MaxRequestUnits == 0 {
		return validatedConfig{}, errors.New("maximum request units must be positive")
	}
	if config.BaseQuantum == 0 {
		return validatedConfig{}, errors.New("base quantum must be positive")
	}
	if len(config.Tenants) == 0 || len(config.Tenants) > absoluteMaxTenants {
		return validatedConfig{}, errors.New("tenant count is outside the hard safety bounds")
	}
	if err := validateQueueLimits("global", config.GlobalQueue, config.MaxBodyBytes, config.MaxRequestUnits); err != nil {
		return validatedConfig{}, err
	}
	if config.GlobalQueue.Count > absoluteMaxQueuedRequests {
		return validatedConfig{}, errors.New("global queued count exceeds the hard safety bound")
	}
	if err := validateInflightLimits("global", config.GlobalInflight, config.MaxRequestUnits); err != nil {
		return validatedConfig{}, err
	}
	if config.GlobalInflight.Count > absoluteMaxInflightRequests {
		return validatedConfig{}, errors.New("global in-flight count exceeds the hard safety bound")
	}

	validated := validatedConfig{
		maxBodyBytes:    config.MaxBodyBytes,
		maxRequestUnits: config.MaxRequestUnits,
		deficitCap:      config.DeficitCap,
		globalQueue:     config.GlobalQueue,
		globalInflight:  config.GlobalInflight,
		tenants:         make([]validatedTenant, 0, len(config.Tenants)),
		tenantByID:      make(map[TenantID]int, len(config.Tenants)),
	}

	var maximumQuantum uint64
	minimumQuantum := ^uint64(0)
	for index, tenant := range config.Tenants {
		if tenant.ID == 0 {
			return validatedConfig{}, fmt.Errorf("tenant %d has a zero ID", index)
		}
		if _, duplicate := validated.tenantByID[tenant.ID]; duplicate {
			return validatedConfig{}, fmt.Errorf("tenant %d repeats an ID", index)
		}
		if tenant.Weight == 0 {
			return validatedConfig{}, fmt.Errorf("tenant %d has a zero weight", index)
		}
		hi, quantum := bits.Mul64(config.BaseQuantum, tenant.Weight)
		if hi != 0 || quantum == 0 {
			return validatedConfig{}, fmt.Errorf("tenant %d quantum overflows", index)
		}
		if err := validateQueueLimits(fmt.Sprintf("tenant %d", index), tenant.Queue, config.MaxBodyBytes, config.MaxRequestUnits); err != nil {
			return validatedConfig{}, err
		}
		if tenant.Queue.Count > config.GlobalQueue.Count || tenant.Queue.Bytes > config.GlobalQueue.Bytes || tenant.Queue.Work > config.GlobalQueue.Work {
			return validatedConfig{}, fmt.Errorf("tenant %d queue limits exceed global limits", index)
		}
		if err := validateInflightLimits(fmt.Sprintf("tenant %d", index), tenant.Inflight, config.MaxRequestUnits); err != nil {
			return validatedConfig{}, err
		}
		if tenant.Inflight.Count > config.GlobalInflight.Count || tenant.Inflight.Work > config.GlobalInflight.Work {
			return validatedConfig{}, fmt.Errorf("tenant %d in-flight limits exceed global limits", index)
		}

		validated.tenantByID[tenant.ID] = index
		validated.tenants = append(validated.tenants, validatedTenant{
			id:       tenant.ID,
			quantum:  quantum,
			queue:    tenant.Queue,
			inflight: tenant.Inflight,
		})
		maximumQuantum = max(maximumQuantum, quantum)
		minimumQuantum = min(minimumQuantum, quantum)
	}

	requiredCap, carry := bits.Add64(config.MaxRequestUnits-1, maximumQuantum, 0)
	if carry != 0 {
		return validatedConfig{}, errors.New("maximum request units and quantum overflow the deficit bound")
	}
	if config.DeficitCap < requiredCap {
		return validatedConfig{}, errors.New("deficit cap is too small for bounded DRR credit")
	}

	rounds := divideRoundUp(config.MaxRequestUnits, minimumQuantum)
	visits, overflow := checkedMultiply(rounds, uint64(len(config.Tenants)))
	if !overflow {
		visits, overflow = checkedMultiply(visits, config.GlobalInflight.Count)
	}
	if overflow || visits > maxFundingVisits {
		return validatedConfig{}, errors.New("scheduler funding work exceeds the hard CPU bound")
	}

	return validated, nil
}

func validateQueueLimits(scope string, limits QueueLimits, maxBody, maxWork uint64) error {
	if limits.Count == 0 || limits.Bytes < maxBody || limits.Work < maxWork {
		return fmt.Errorf("%s queue cannot hold one maximum request", scope)
	}
	return nil
}

func validateInflightLimits(scope string, limits InflightLimits, maxWork uint64) error {
	if limits.Count == 0 || limits.Work < maxWork {
		return fmt.Errorf("%s in-flight limits cannot hold one maximum request", scope)
	}
	return nil
}

func divideRoundUp(value, divisor uint64) uint64 {
	return 1 + (value-1)/divisor
}

func checkedMultiply(left, right uint64) (uint64, bool) {
	hi, low := bits.Mul64(left, right)
	return low, hi != 0
}

func validateAdmission(config validatedConfig, admission Admission) Decision {
	if admission.Tenant == 0 {
		return Decision{Kind: DecisionInvalid, Resource: ResourceNone}
	}
	if admission.BodyBytes == 0 || admission.BodyBytes > config.maxBodyBytes {
		return Decision{Kind: DecisionInvalid, Resource: ResourceBytes}
	}
	if admission.WorkUnits == 0 || admission.WorkUnits > config.maxRequestUnits {
		return Decision{Kind: DecisionInvalid, Resource: ResourceWork}
	}
	if admission.QueueTimeout <= 0 || admission.QueueTimeout > MaximumQueueTimeout {
		return Decision{Kind: DecisionInvalid, Resource: ResourceNone}
	}
	if _, exists := config.tenantByID[admission.Tenant]; !exists {
		return Decision{Kind: DecisionInvalid, Resource: ResourceNone}
	}
	return Decision{}
}
