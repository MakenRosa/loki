package postings

// RangeStreamIDs invokes fn for each stream ID set in an LSB-ordered stream-ID
// bitmap: for byte index b and bit position p (0-7), a set bit means stream ID
// b*8+p is present. This bit layout is part of the postings section's on-disk
// encoding.
func RangeStreamIDs(bitmap []byte, fn func(streamID int64)) {
	for byteIdx, b := range bitmap {
		if b == 0 {
			continue
		}
		for bitPos := 0; bitPos < 8; bitPos++ {
			if (b>>uint(bitPos))&1 == 0 {
				continue
			}
			fn(int64(byteIdx*8 + bitPos))
		}
	}
}
