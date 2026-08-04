package main

import "testing"

func TestIsRecycleSyncArtifactItem(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		item recycleBackfillItem
		want bool
	}{
		{
			name: "apps lock file from path",
			item: recycleBackfillItem{
				Path: "apps/localhost-3020/backup.__sync_mutex_v1.__sync_lock_v1/lock.json",
			},
			want: true,
		},
		{
			name: "apps txn file from directory and name",
			item: recycleBackfillItem{
				Directory: "apps/localhost-3020",
				Name:      "backup.__sync_txn_data_v1.123-abc.json",
			},
			want: true,
		},
		{
			name: "personal same name is user data",
			item: recycleBackfillItem{
				Path: "personal/backup.__sync_txn_data_v1.123-abc.json",
			},
			want: false,
		},
		{
			name: "normal app file",
			item: recycleBackfillItem{
				Path: "apps/localhost-3020/backup.json",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isRecycleSyncArtifactItem(tt.item); got != tt.want {
				t.Fatalf("isRecycleSyncArtifactItem() = %v, want %v", got, tt.want)
			}
		})
	}
}
