package murmur

import (
	"fmt"

	"testing"
)

func BenchmarkStreamSegments(b *testing.B) {
	for _, size := range []int{1024, 1 << 20} {
		for _, spans := range []int{1, 16, 1024} {
			b.Run(fmt.Sprintf("bytes=%d/spans=%d", size, spans), func(b *testing.B) {
				data := make([]byte, size)
				width := size / spans
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					var s Stream
					for j := 0; j < len(data); j += width {
						s.Write(data[j:min(j+width, len(data))])
					}
					_ = s.Sum64()
				}
			})
		}
	}
}
