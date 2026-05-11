package server

import (
	"slices"
	"testing"
)

func TestBuildAndParseReconfigTxRoundTrip(t *testing.T) {
	tx, err := buildReconfigTx([]uint32{3, 1, 3, 2})
	if err != nil {
		t.Fatalf("buildReconfigTx returned error: %v", err)
	}
	if tx != "reconfig:members=1,2,3" {
		t.Fatalf("unexpected tx format: %q", tx)
	}

	members, ok, err := parseReconfigTx(tx)
	if err != nil {
		t.Fatalf("parseReconfigTx returned error: %v", err)
	}
	if !ok {
		t.Fatalf("expected reconfiguration tx")
	}
	expected := []uint32{1, 2, 3}
	if !slices.Equal(members, expected) {
		t.Fatalf("unexpected members: got %v want %v", members, expected)
	}
}

func TestParseReconfigTxRejectsInvalidID(t *testing.T) {
	_, ok, err := parseReconfigTx("reconfig:members=1,foo,3")
	if !ok {
		t.Fatalf("expected reconfiguration tx")
	}
	if err == nil {
		t.Fatalf("expected parse error for invalid member ID")
	}
}

func TestLeaderForHeightUsesMembershipIDs(t *testing.T) {
	members := []uint32{2, 4, 7}
	seen := map[uint32]bool{}
	for h := uint64(1); h <= 64; h++ {
		leader := leaderForHeight(h, members)
		if !slices.Contains(members, leader) {
			t.Fatalf("leader %d not in member set %v", leader, members)
		}
		seen[leader] = true
	}
	if len(seen) < 2 {
		t.Fatalf("leader schedule did not vary over member set: %v", seen)
	}
}
