package v2

type requestXHeaderSource struct {
	req Request
}

type responseXHeaderSource struct {
	resp Response
	req  requestXHeaderSource
}

func (s requestXHeaderSource) headerValue(key string) (string, bool) {
	if s.req != nil {
		for _, x := range s.req.GetMetaHeader().GetXHeaders() {
			if x.GetKey() == key {
				return x.GetValue(), true
			}
		}
	}
	return "", false
}
