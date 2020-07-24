package fix

import (
	"fmt"
	"reflect"

	"go.uber.org/zap"
)

func (a *app) Catch(err error) {
	if err == nil {
		return
	}

	if a.log == nil {
		panic(err)
	}

	a.log.Fatal("Can't run app",
		zap.Error(err))
}

// CatchTrace catch errors for debugging
// use that function just for debug your application.
func (a *app) CatchTrace(err error) {
	if err == nil {
		return
	}

	// digging into the root of the problem
	for {
		var (
			ok bool
			v  = reflect.ValueOf(err)
			fn reflect.Value
		)

		if v.Type().Kind() != reflect.Struct {
			break
		}

		if !v.FieldByName("Reason").IsValid() {
			break
		}

		if v.FieldByName("Func").IsValid() {
			fn = v.FieldByName("Func")
		}

		fmt.Printf("Place: %#v\nReason: %s\n\n", fn, err)

		if err, ok = v.FieldByName("Reason").Interface().(error); !ok {
			err = v.Interface().(error)
			break
		}
	}

	panic(err)
}
