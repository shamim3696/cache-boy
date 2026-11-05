// Package cluster implements Redis Cluster style key routing with a static
// slot map: the keyspace is split into 16384 hash slots, each node serves a
// set of slot ranges, and a node answers MOVED for keys it doesn't own so
// cluster-aware clients learn where to send them.
//
// There is no gossip, failover or resharding: the slot map comes from the
// configuration (cluster-node lines) and is the same on every node.
package cluster

import "strings"

// NumSlots is the number of hash slots, as in Redis Cluster.
const NumSlots = 16384

// crc16tab is the CRC16-XMODEM (polynomial 0x1021, init 0) lookup table.
var crc16tab = func() [256]uint16 {
	var t [256]uint16
	for i := range t {
		crc := uint16(i) << 8
		for range 8 {
			if crc&0x8000 != 0 {
				crc = crc<<1 ^ 0x1021
			} else {
				crc <<= 1
			}
		}
		t[i] = crc
	}
	return t
}()

// CRC16 computes CRC16-XMODEM, the checksum Redis Cluster uses for slots.
func CRC16(s string) uint16 {
	var crc uint16
	for i := 0; i < len(s); i++ {
		crc = crc<<8 ^ crc16tab[byte(crc>>8)^s[i]]
	}
	return crc
}

// HashTag returns the part of key used for hashing: the text between the
// first '{' and the next '}' if that is not empty, otherwise the whole key.
// "{user1}.a" and "{user1}.b" therefore land in the same slot.
func HashTag(key string) string {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		if j := strings.IndexByte(key[i+1:], '}'); j > 0 {
			return key[i+1 : i+1+j]
		}
	}
	return key
}

// Slot returns the hash slot of key.
func Slot(key string) int {
	return int(CRC16(HashTag(key)) % NumSlots)
}
