package codec

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
)

// --- Test helpers ---

// testPayload is a simple struct for basic marshal/unmarshal tests.
type testPayload struct {
	Name  string `json:"name"`
	Value int    `json:"value"`
}

// customMarshaler implements both Marshaler and Unmarshaler interfaces
// to verify the Registry dispatches to custom methods.
type customMarshaler struct {
	Data string
}

func (c *customMarshaler) Marshal() ([]byte, error) {
	return []byte("custom:" + c.Data), nil
}

func (c *customMarshaler) Unmarshal(b []byte) error {
	if len(b) < 7 {
		return errors.New("invalid custom format")
	}
	c.Data = string(b[7:]) // strip "custom:" prefix
	return nil
}

// brokenMarshaler always returns an error on Marshal/Unmarshal.
type brokenMarshaler struct{}

func (b *brokenMarshaler) Marshal() ([]byte, error) {
	return nil, errors.New("marshal exploded")
}

func (b *brokenMarshaler) Unmarshal([]byte) error {
	return errors.New("unmarshal exploded")
}

// --- New / Options ---

func TestNew_DefaultsToJSON(t *testing.T) {
	// A fresh Registry must use json.Marshal/json.Unmarshal by default.
	r := New()

	p := testPayload{Name: "hello", Value: 42}
	b, err := r.Marshal(p)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var got testPayload
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("result is not valid JSON: %v", err)
	}
	if got != p {
		t.Fatalf("expected %+v, got %+v", p, got)
	}
}

func TestNew_WithCustomDefaults(t *testing.T) {
	// Custom default marshal/unmarshal must be wired correctly.
	called := false
	customMarshal := func(v any) ([]byte, error) {
		called = true
		return json.Marshal(v)
	}
	customUnmarshal := func(b []byte, v any) error {
		return json.Unmarshal(b, v)
	}

	r := New(Default(customMarshal, customUnmarshal))
	r.Register("tp", func() any { return &testPayload{} })

	if _, err := r.Marshal(testPayload{Name: "x"}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("expected custom marshal function to be called")
	}
}

func TestDefault_PanicsOnNilMarshal(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when marshal is nil")
		}
	}()
	Default(nil, func([]byte, any) error { return nil })
}

func TestDefault_PanicsOnNilUnmarshal(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when unmarshal is nil")
		}
	}()
	Default(func(any) ([]byte, error) { return nil, nil }, nil)
}

func TestDebugOption(t *testing.T) {
	// Debug option must not break anything; just toggle the flag.
	r := New(Debug(true))
	r.Register("tp", func() any { return &testPayload{} })

	// Exercise debug paths: Register, New, Marshal, Unmarshal.
	if _, err := r.New("tp"); err != nil {
		t.Fatal(err)
	}
	b, err := r.Marshal(testPayload{Name: "dbg"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Unmarshal(b, "tp"); err != nil {
		t.Fatal(err)
	}

	// Also exercise the custom Marshaler/Unmarshaler debug branch.
	r.Register("cm", func() any { return &customMarshaler{} })
	cb, err := r.Marshal(&customMarshaler{Data: "dbg"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Unmarshal(cb, "cm"); err != nil {
		t.Fatal(err)
	}
}

// --- Register / New ---

func TestRegister_And_New(t *testing.T) {
	r := New()
	r.Register("payload", func() any { return &testPayload{} })

	v, err := r.New("payload")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := v.(*testPayload); !ok {
		t.Fatalf("expected *testPayload, got %T", v)
	}
}

func TestNew_UnknownName(t *testing.T) {
	r := New()

	_, err := r.New("ghost")
	if err == nil {
		t.Fatal("expected error for unregistered name")
	}
}

func TestRegister_Overwrite(t *testing.T) {
	// Registering the same name twice must overwrite the factory.
	r := New()
	r.Register("x", func() any { return &testPayload{} })
	r.Register("x", func() any { return &customMarshaler{} })

	v, err := r.New("x")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := v.(*customMarshaler); !ok {
		t.Fatalf("expected *customMarshaler after overwrite, got %T", v)
	}
}

// --- Generic Register / Make ---

func TestGenericRegister_And_Make(t *testing.T) {
	r := New()
	Register[testPayload](r, "tp")

	got, err := Make[testPayload](r, "tp")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != (testPayload{}) {
		t.Fatalf("expected zero value, got %+v", got)
	}
}

func TestMake_UnknownName(t *testing.T) {
	r := New()

	_, err := Make[testPayload](r, "nope")
	if err == nil {
		t.Fatal("expected error for unregistered name")
	}
}

func TestMake_TypeMismatch(t *testing.T) {
	// Register one type, request another — must fail.
	r := New()
	Register[testPayload](r, "tp")

	_, err := Make[customMarshaler](r, "tp")
	if err == nil {
		t.Fatal("expected type mismatch error")
	}
}

// --- Marshal ---

func TestMarshal_DefaultJSON(t *testing.T) {
	r := New()
	p := testPayload{Name: "test", Value: 7}

	b, err := r.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}

	var got testPayload
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got != p {
		t.Fatalf("expected %+v, got %+v", p, got)
	}
}

func TestMarshal_CustomMarshaler(t *testing.T) {
	// Types implementing Marshaler must bypass the default path.
	r := New()
	cm := &customMarshaler{Data: "hello"}

	b, err := r.Marshal(cm)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "custom:hello" {
		t.Fatalf("expected 'custom:hello', got %q", string(b))
	}
}

func TestMarshal_CustomMarshalerError(t *testing.T) {
	r := New()

	_, err := r.Marshal(&brokenMarshaler{})
	if err == nil {
		t.Fatal("expected error from broken custom marshaler")
	}
}

func TestMarshal_DefaultError(t *testing.T) {
	// Channels are not JSON-serializable — must return an error.
	r := New()

	_, err := r.Marshal(make(chan int))
	if err == nil {
		t.Fatal("expected error when marshaling unsupported type")
	}
}

// --- Unmarshal ---

func TestUnmarshal_DefaultJSON(t *testing.T) {
	r := New()
	Register[testPayload](r, "tp")

	original := testPayload{Name: "abc", Value: 99}
	b, _ := json.Marshal(original)

	raw, err := r.Unmarshal(b, "tp")
	if err != nil {
		t.Fatal(err)
	}

	got, ok := raw.(testPayload)
	if !ok {
		t.Fatalf("expected testPayload, got %T", raw)
	}
	if got != original {
		t.Fatalf("expected %+v, got %+v", original, got)
	}
}

func TestUnmarshal_CustomUnmarshaler(t *testing.T) {
	r := New()
	r.Register("cm", func() any { return &customMarshaler{} })

	raw, err := r.Unmarshal([]byte("custom:world"), "cm")
	if err != nil {
		t.Fatal(err)
	}

	// resolve dereferences the pointer, so we get customMarshaler (not *customMarshaler).
	got, ok := raw.(customMarshaler)
	if !ok {
		t.Fatalf("expected customMarshaler, got %T", raw)
	}
	if got.Data != "world" {
		t.Fatalf("expected Data='world', got %q", got.Data)
	}
}

func TestUnmarshal_CustomUnmarshalerError(t *testing.T) {
	r := New()
	r.Register("broken", func() any { return &brokenMarshaler{} })

	_, err := r.Unmarshal([]byte("anything"), "broken")
	if err == nil {
		t.Fatal("expected error from broken custom unmarshaler")
	}
}

func TestUnmarshal_UnknownName(t *testing.T) {
	r := New()

	_, err := r.Unmarshal([]byte(`{}`), "unknown")
	if err == nil {
		t.Fatal("expected error for unregistered name")
	}
}

func TestUnmarshal_DefaultError(t *testing.T) {
	// Invalid JSON into a struct → default unmarshaler must return error.
	r := New()
	Register[testPayload](r, "tp")

	result, err := r.Unmarshal([]byte(`{invalid`), "tp")
	if err == nil {
		t.Fatal("expected unmarshal error on bad JSON")
	}
	// The function still returns resolve(ptr) even on error.
	if result == nil {
		t.Fatal("expected non-nil result even on error (matches source behavior)")
	}
}

// --- Marshal + Unmarshal round-trip ---

func TestRoundTrip_Default(t *testing.T) {
	r := New()
	Register[testPayload](r, "tp")

	original := testPayload{Name: "round", Value: 123}

	b, err := r.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := r.Unmarshal(b, "tp")
	if err != nil {
		t.Fatal(err)
	}

	got := raw.(testPayload)
	if got != original {
		t.Fatalf("round-trip mismatch: expected %+v, got %+v", original, got)
	}
}

func TestRoundTrip_CustomMarshalerUnmarshaler(t *testing.T) {
	r := New()
	r.Register("cm", func() any { return &customMarshaler{} })

	original := &customMarshaler{Data: "trip"}

	b, err := r.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := r.Unmarshal(b, "cm")
	if err != nil {
		t.Fatal(err)
	}

	got := raw.(customMarshaler)
	if got.Data != original.Data {
		t.Fatalf("round-trip mismatch: expected %q, got %q", original.Data, got.Data)
	}
}

// --- Map ---

func TestMap_ReturnsAllRegistered(t *testing.T) {
	r := New()
	r.Register("a", func() any { return &testPayload{} })
	r.Register("b", func() any { return &customMarshaler{} })

	m := r.Map()
	if len(m) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(m))
	}
	for _, name := range []string{"a", "b"} {
		if _, ok := m[name]; !ok {
			t.Fatalf("expected key %q in map", name)
		}
	}
}

func TestMap_ReturnsCopy(t *testing.T) {
	// Mutating the returned map must not affect the Registry internals.
	r := New()
	r.Register("x", func() any { return &testPayload{} })

	m := r.Map()
	m["injected"] = func() any { return nil }

	if _, err := r.New("injected"); err == nil {
		t.Fatal("mutating Map() result must not affect the Registry")
	}
}

func TestMap_Empty(t *testing.T) {
	r := New()
	m := r.Map()
	if len(m) != 0 {
		t.Fatalf("expected empty map, got %d entries", len(m))
	}
}

// --- resolve ---

func TestResolve_DereferencesPointer(t *testing.T) {
	p := &testPayload{Name: "ptr", Value: 1}
	got := resolve(p)

	v, ok := got.(testPayload)
	if !ok {
		t.Fatalf("expected testPayload, got %T", got)
	}
	if v != *p {
		t.Fatalf("expected %+v, got %+v", *p, v)
	}
}

func TestResolve_DoublePointer(t *testing.T) {
	inner := &testPayload{Name: "deep", Value: 2}
	pp := &inner

	got := resolve(pp)
	v, ok := got.(testPayload)
	if !ok {
		t.Fatalf("expected testPayload after double deref, got %T", got)
	}
	if v != *inner {
		t.Fatalf("expected %+v, got %+v", *inner, v)
	}
}

func TestResolve_NonPointer(t *testing.T) {
	// Non-pointer value must pass through unchanged.
	got := resolve(42)
	if got != 42 {
		t.Fatalf("expected 42, got %v", got)
	}
}

// --- Concurrency ---

func TestConcurrent_RegisterAndNew(t *testing.T) {
	// Concurrent Register + New must not race (run with -race).
	r := New()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("type_%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Register(name, func() any { return &testPayload{} })
		}()
	}
	// Reads in parallel with writes.
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("type_%d", i)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Might or might not be registered yet — we only care about no race.
			r.New(name)
		}()
	}
	wg.Wait()
}

func TestConcurrent_MarshalUnmarshal(t *testing.T) {
	r := New()
	Register[testPayload](r, "tp")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			p := testPayload{Name: "concurrent", Value: n}
			b, err := r.Marshal(p)
			if err != nil {
				t.Errorf("marshal error: %v", err)
				return
			}
			raw, err := r.Unmarshal(b, "tp")
			if err != nil {
				t.Errorf("unmarshal error: %v", err)
				return
			}
			got := raw.(testPayload)
			if got != p {
				t.Errorf("mismatch: expected %+v, got %+v", p, got)
			}
		}(i)
	}
	wg.Wait()
}

func TestConcurrent_Map(t *testing.T) {
	// Map must be safe to call concurrently with Register.
	r := New()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		name := fmt.Sprintf("t_%d", i)
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.Register(name, func() any { return &testPayload{} })
		}()
		go func() {
			defer wg.Done()
			_ = r.Map()
		}()
	}
	wg.Wait()
}
