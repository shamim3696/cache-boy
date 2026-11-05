package resp

// RESP3 notes
//
// A client switches protocol with HELLO 3. After that the server may send:
//   %  maps      (HGETALL, CONFIG GET, HELLO reply)
//   ~  sets      (SMEMBERS)
//   ,  doubles   (ZSCORE, INCRBYFLOAT)
//   #  booleans
//   _  null
//   >  push      (pub/sub messages, client-side caching invalidations)
//
// Writer.Append down-converts these for RESP2 clients, so command handlers
// always return the richest type and never check the protocol version.

// Invalidate builds the push message for client-side caching. A nil keys slice
// means "flush everything" (sent after FLUSHALL).
func Invalidate(keys []string) Value {
	if keys == nil {
		return Push(Bulk("invalidate"), Null())
	}
	return Push(Bulk("invalidate"), Strings(keys))
}
