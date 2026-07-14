package app

import (
	"crypto/sha256"
	"errors"
	"os"
	"reflect"

	"github.com/omar07ibrahim/ssemaphore/internal/httpapi"
)

const maximumSecretBytes = 4096

var errSecretResolutionFailed = errors.New("secret resolution failed")

type secretSource interface {
	LookupEnv(string) (string, bool)
	Unsetenv(string) error
}

type systemSecretSource struct{}

func (systemSecretSource) LookupEnv(name string) (string, bool) {
	return os.LookupEnv(name)
}

func (systemSecretSource) Unsetenv(name string) error {
	return os.Unsetenv(name)
}

type resolvedSecrets struct {
	credentials []httpapi.Credential
	upstream    string
}

func (resolvedSecrets) String() string   { return "app.resolvedSecrets{redacted}" }
func (resolvedSecrets) GoString() string { return "app.resolvedSecrets{redacted}" }

// clear drops every live reference held by resolvedSecrets. Go strings are
// immutable, so this is a best-effort overwrite of their containing fields,
// not a guarantee that the runtime has erased every backing byte.
func (secrets *resolvedSecrets) clear() {
	if secrets == nil {
		return
	}
	for index := range secrets.credentials {
		secrets.credentials[index] = httpapi.Credential{}
	}
	secrets.credentials = nil
	secrets.upstream = ""
}

func resolveSecrets(policy *validatedPolicy, source secretSource) (resolvedSecrets, error) {
	if policy == nil || nilSecretSource(source) {
		return resolvedSecrets{}, errSecretResolutionFailed
	}

	resolved := resolvedSecrets{
		credentials: make([]httpapi.Credential, len(policy.credentials)),
	}
	seen := make(map[[sha256.Size]byte]struct{}, len(policy.credentials)+1)
	failed := false

	consume := func(name string) string {
		value, present := source.LookupEnv(name)
		if err := source.Unsetenv(name); err != nil {
			failed = true
		}
		if !present || len(value) == 0 || len(value) > maximumSecretBytes {
			failed = true
			return ""
		}

		digest := sha256.Sum256([]byte(value))
		if _, duplicate := seen[digest]; duplicate {
			failed = true
		}
		seen[digest] = struct{}{}
		return value
	}

	for index, reference := range policy.credentials {
		resolved.credentials[index] = httpapi.Credential{
			Tenant: reference.tenant,
			Token:  consume(reference.env),
		}
	}
	resolved.upstream = consume(policy.upstreamTokenEnv)

	if failed {
		resolved.clear()
		return resolvedSecrets{}, errSecretResolutionFailed
	}
	return resolved, nil
}

func nilSecretSource(source secretSource) bool {
	if source == nil {
		return true
	}
	value := reflect.ValueOf(source)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}
