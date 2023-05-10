package repository

import "testing"

func TestSum(t *testing.T) {
	opt := MatchOptions{}
	opt.EQ("hello", "world")
	opt.GTE("hello", "world")
	sum := opt.Sum()
	t.Log(sum)
	if sum != "0ca96518fe5e365852b88890962a1a6a" {
		t.Fatal("sum error")
	}
}
